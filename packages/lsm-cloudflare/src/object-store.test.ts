import { Arbitrary, Bytes, Effect, Option, Schema, Stream } from "effect";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { describe, expect, it } from "vite-plus/test";

import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ObjectStore, objectStoreTestLayer, toUniformParts } from "./object-store.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const run = <A, E>(effect: Effect.Effect<A, E, ObjectStore | RuntimeContext>) =>
  effect.pipe(
    Effect.provide(objectStoreTestLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

describe("toUniformParts", () => {
  it("re-chunks any byte stream into full parts and one shorter last part, losing nothing", () => {
    const chunks = Arbitrary.array(
      Arbitrary.array(smallInt(0, 255), { maxLength: 40 }).pipe(
        Arbitrary.map((bytes) => Uint8Array.from(bytes)),
      ),
      { maxLength: 30 },
    );
    const result = Effect.runSync(
      Arbitrary.checkEffect(
        Arbitrary.all({ chunks, partSize: smallInt(1, 64) }),
        ({ chunks: input, partSize }) => {
          const parts = Effect.runSync(
            Stream.runCollect(toUniformParts(Stream.fromIterable(input), partSize)),
          );
          const sizes = parts.map((part) => part.length);
          const allButLastFull = sizes.slice(0, -1).every((size) => size === partSize);
          const lastFits =
            sizes.length === 0 || ((sizes.at(-1) ?? 0) > 0 && (sizes.at(-1) ?? 0) <= partSize);
          return (
            allButLastFull &&
            lastFits &&
            Bytes.Equivalence(Bytes.concat(parts), Bytes.concat(input))
          );
        },
        { runs: 300 },
      ),
    );
    expect(Arbitrary.formatCheckFailure(result)).toBeUndefined();
  });
});

describe("ObjectStore (in-memory)", () => {
  it("replaces an object only at the version it read, like R2 If-Match", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        yield* store.put("gc/boundary", Uint8Array.of(1));
        const first = yield* store.getVersioned("gc/boundary");
        const version = Option.getOrThrow(first).version;
        const written = yield* store.putIfVersion("gc/boundary", Uint8Array.of(2), version);
        // The version moved with the write, so a second writer holding the old one loses.
        const stale = yield* store.putIfVersion("gc/boundary", Uint8Array.of(3), version);
        const missing = yield* store.putIfVersion("gc/other", Uint8Array.of(4), version);
        const final = yield* store.get("gc/boundary");
        return { written, stale, missing, final };
      }),
    );
    expect(outcome).toMatchObject({
      written: "Written",
      stale: "VersionChanged",
      missing: "VersionChanged",
    });
    expect(outcome.final).toEqual(Option.some(Uint8Array.of(2)));
  });

  it("lists next-level prefixes in order", async () => {
    const prefixes = await run(
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        for (const key of [
          "db/b/manifest/1",
          "db/a/manifest/1",
          "db/a/compacted/x.sst",
          "db/c",
          "other/x",
        ]) {
          yield* store.put(key, Uint8Array.of(0));
        }
        return yield* store.listPrefixes("db/", Option.none());
      }),
    );
    expect(prefixes).toEqual({ prefixes: ["db/a/", "db/b/"], cursor: Option.none() });
  });
});
