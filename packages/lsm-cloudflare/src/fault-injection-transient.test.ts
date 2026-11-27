import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Ref, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";
import {
  ObjectStore,
  ObjectStoreUnavailableError,
  objectStoreTestLayer,
  R2_READ_RETRIES,
  retryTransientRead,
} from "./object-store.ts";

const DB = DatabaseId.make("orders");

/** The object store's idempotent reads. */
type ReadKind = "get" | "getRange" | "getVersioned" | "head" | "list" | "listPrefixes";

/**
 * Whether the R2 call under a read fails transiently: `attempt` counts this read's attempts from 1.
 * `true` fails the attempt with R2's own transient error.
 */
type TransientRule = (read: ReadKind, key: string, attempt: number) => boolean;

class Transients extends Context.Service<Transients, Ref.Ref<TransientRule>>()("test/Transients") {}

const OPERATION_OF: Record<ReadKind, ObjectStoreUnavailableError["operation"]> = {
  get: "get",
  getRange: "getRange",
  getVersioned: "get",
  head: "head",
  list: "list",
  listPrefixes: "list",
};

const TRANSIENT = "get: We encountered an internal error. Please try again. (10001)";

/**
 * An in-memory bucket whose reads fail at the R2 call when `Transients` says so, under the object
 * store's own retry (`retryTransientRead`, D134), mapped to `ObjectStoreUnavailableError` after the
 * retries as `makeObjectStore` does. Writes never fail here.
 */
const transientObjectStoreLayer = Layer.effect(
  ObjectStore,
  Effect.gen(function* () {
    const rule = yield* Transients;
    const inner = Context.get(yield* Layer.build(objectStoreTestLayer), ObjectStore);
    const read = <A>(
      kind: ReadKind,
      key: string,
      effect: Effect.Effect<A, ObjectStoreUnavailableError, RuntimeContext>,
    ) =>
      Effect.suspend(() => {
        let attempt = 0;
        return retryTransientRead(
          Effect.gen(function* () {
            attempt++;
            if ((yield* Ref.get(rule))(kind, key, attempt)) {
              return yield* new Cloudflare.R2.R2Error({
                message: TRANSIENT,
                cause: new Error(TRANSIENT),
              });
            }
            return yield* Effect.orDie(effect);
          }),
        ).pipe(
          Effect.mapError(
            (error) =>
              new ObjectStoreUnavailableError({
                operation: OPERATION_OF[kind],
                key,
                message: `Object storage ${kind} of ${key} failed after retries: ${error.message}.`,
              }),
          ),
        );
      });
    return ObjectStore.of({
      ...inner,
      get: (key) => read("get", key, inner.get(key)),
      getRange: (key, range) => read("getRange", key, inner.getRange(key, range)),
      getVersioned: (key) => read("getVersioned", key, inner.getVersioned(key)),
      head: (key) => read("head", key, inner.head(key)),
      list: (prefix, cursor) => read("list", prefix, inner.list(prefix, cursor)),
      listPrefixes: (prefix, after, limit) =>
        read("listPrefixes", prefix, inner.listPrefixes(prefix, after, limit)),
    });
  }),
);

const transientsLayer = Layer.effect(
  Transients,
  Ref.make<TransientRule>(() => false),
);

const testLayer = dbClientTestLayer(
  { blockSize: 64 },
  transientObjectStoreLayer.pipe(Layer.provide(transientsLayer)),
).pipe(Layer.provideMerge(transientsLayer));

type Services = Layer.Success<typeof testLayer> | DbReaderServices;

const provide = <A, E>(program: Effect.Effect<A, E, Services>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

const setRule = (rule: TransientRule) =>
  Effect.gen(function* () {
    yield* Ref.set(yield* Transients, rule);
  });

/** A database with keys 0..9 flushed to an L0 SST, so reads go to R2 for the manifest and blocks. */
const flushedDatabase = Effect.gen(function* () {
  yield* (yield* ManifestStore).initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  );
  const client = yield* DbClient;
  for (let key = 0; key < 10; key++) {
    yield* client.put(DB, Uint8Array.of(key), Uint8Array.of(100 + key));
  }
  yield* client.flush(DB);
  return client;
});

const readAll = (client: DbClient["Service"], consistency: "Latest" | "Flushed") =>
  Effect.gen(function* () {
    const values: Array<number | undefined> = [];
    for (let key = 0; key < 10; key++) {
      const row = yield* client.get(DB, Uint8Array.of(key), {
        consistency: { _tag: consistency },
        snapshot: Option.none(),
        refuseSealed: false,
      });
      values.push(Option.getOrUndefined(Option.map(row, (bytes) => bytes[0])));
    }
    const scanned = yield* Stream.runCollect(
      client.scan(DB, FULL_KEY_RANGE, {
        consistency: { _tag: consistency },
        snapshot: Option.none(),
        refuseSealed: false,
        limit: Option.none(),
      }),
    );
    return { values, scanned: scanned.length };
  });

const EXPECTED = {
  values: Array.from({ length: 10 }, (_, key) => 100 + key),
  scanned: 10,
};

describe("fault injection: R2's transient read failures (D134)", () => {
  // The read path's kinds; `head`, `list`, and `listPrefixes` are GC's and the lifecycle's, and
  // `object-store.test.ts` checks their retry.
  for (const kind of ["get", "getRange", "getVersioned"] as const) {
    it(`a transient failure on the first attempt of every ${kind} is invisible to readers`, async () => {
      const outcome = await provide(
        Effect.gen(function* () {
          const client = yield* flushedDatabase;
          let failed = 0;
          yield* setRule((read, _key, attempt) => {
            const fails = read === kind && attempt === 1;
            if (fails) failed++;
            return fails;
          });
          const latest = yield* readAll(client, "Latest");
          const flushed = yield* readAll(client, "Flushed");
          return { latest, flushed, failed };
        }),
      );
      expect(outcome.latest).toEqual(EXPECTED);
      expect(outcome.flushed).toEqual(EXPECTED);
      expect(outcome.failed).toBeGreaterThan(0);
    });
  }

  it("a flush whose every read fails transiently once commits, and nothing is lost", async () => {
    const outcome = await provide(
      Effect.gen(function* () {
        const client = yield* flushedDatabase;
        let failed = 0;
        yield* setRule((_read, _key, attempt) => {
          const fails = attempt === 1;
          if (fails) failed++;
          return fails;
        });
        for (let key = 10; key < 20; key++) {
          yield* client.put(DB, Uint8Array.of(key), Uint8Array.of(100 + key));
        }
        yield* client.flush(DB);
        yield* setRule(() => false);
        const after = yield* readAll(client, "Flushed");
        return { failed, values: after.values };
      }),
    );
    expect(outcome.failed).toBeGreaterThan(0);
    expect(outcome.values).toEqual(EXPECTED.values);
  });

  it("a Flushed get whose manifest refresh meets one transient failure still answers", async () => {
    const outcome = await provide(
      Effect.gen(function* () {
        const client = yield* flushedDatabase;
        let failed = 0;
        yield* setRule((_read, key, attempt) => {
          const fails = key.includes("/manifest") && attempt === 1;
          if (fails) failed++;
          return fails;
        });
        const row = yield* client.get(DB, Uint8Array.of(3), {
          consistency: { _tag: "Flushed" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        return { value: Option.getOrUndefined(Option.map(row, (bytes) => bytes[0])), failed };
      }),
    );
    expect(outcome.value).toBe(103);
    expect(outcome.failed).toBeGreaterThan(0);
  });

  it("a transient failure on every attempt reaches the caller as the retryable unavailable error", async () => {
    const outcome = await provide(
      Effect.gen(function* () {
        const client = yield* flushedDatabase;
        const attempts: Array<number> = [];
        yield* setRule((read, _key, attempt) => {
          if (read !== "getRange") return false;
          attempts.push(attempt);
          return true;
        });
        const result = yield* Effect.result(
          client.get(DB, Uint8Array.of(3), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          }),
        );
        return {
          failure: result._tag === "Failure" ? result.failure._tag : "answered",
          maxAttempt: Math.max(...attempts),
        };
      }),
    );
    expect(outcome).toEqual({
      failure: "ReaderUnavailableError",
      maxAttempt: R2_READ_RETRIES + 1,
    });
  });
});
