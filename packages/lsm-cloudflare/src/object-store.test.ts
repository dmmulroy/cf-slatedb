import { DatabaseId, ManifestId, md5Hex, SstId } from "@shaledb/lsm-core";
import { Arbitrary, Bytes, Effect, Option, Schema, Stream, Tracer } from "effect";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { describe, expect, it } from "vite-plus/test";

import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import {
  latestManifestKey,
  manifestGcBoundaryKey,
  manifestKey,
  objectKindOfKey,
  sstKey,
} from "./object-keys.ts";
import * as Cloudflare from "@alchemy.run/cloudflare";

import { LsmBucketClient } from "./lsm-bucket.ts";
import {
  ObjectStore,
  objectStoreLayerWithoutDependencies,
  objectStoreTestLayer,
  objectStoreWithTransitCorruptionTestLayer,
  isTransientR2Error,
  PART_INTEGRITY_ATTEMPTS,
  R2_READ_RETRIES,
  toUniformParts,
  uploadPartVerified,
} from "./object-store.ts";

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
        return yield* store.listPrefixes("db/", Option.none(), 1000);
      }),
    );
    expect(prefixes).toEqual({ prefixes: ["db/a/", "db/b/"], cursor: Option.none() });
  });
});

describe("multipart part integrity (D62)", () => {
  const PART = 5 * 1024 * 1024;
  const body = Uint8Array.from({ length: 2 * PART + 1234 }, (_, index) => (index * 131) & 0xff);
  const upload = (corrupt: (key: string, partNumber: number, attempt: number) => boolean) =>
    Effect.gen(function* () {
      const store = yield* ObjectStore;
      const written = yield* Effect.result(
        store.putMultipart("db/a/sst/x.sst", Stream.make(body), PART),
      );
      return { written, stored: yield* store.get("db/a/sst/x.sst") };
    }).pipe(
      Effect.provide(objectStoreWithTransitCorruptionTestLayer(corrupt)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );

  it("uploads a part that arrived changed again, storing exactly what was sent", async () => {
    const { written, stored } = await upload(
      (_, partNumber, attempt) => partNumber === 2 && attempt === 1,
    );
    expect(written._tag).toBe("Success");
    // Compared by digest: a deep comparison of 10 MiB is slow.
    expect(Option.map(stored, md5Hex)).toEqual(Option.some(md5Hex(body)));
  });

  it("fails the upload, storing nothing, when a part arrives changed on every attempt", async () => {
    const { written, stored } = await upload((_, partNumber) => partNumber === 3);
    expect(written._tag).toBe("Failure");
    if (written._tag === "Failure") {
      expect(written.failure._tag).toBe("ObjectStoreUnavailableError");
      expect(written.failure.message).toContain(`on ${PART_INTEGRITY_ATTEMPTS} attempts`);
    }
    expect(stored).toEqual(Option.none());
  });

  it("checks the ETag R2 returns, quoted or not, against the MD5 of the part", async () => {
    const part = Uint8Array.of(1, 2, 3);
    const attempts: Array<number> = [];
    const result = await Effect.runPromise(
      uploadPartVerified("k", 1, part, (partNumber) =>
        Effect.sync(() => {
          attempts.push(partNumber);
          // R2's ETag for the bytes 1,2,3, quoted and upper-cased on the second attempt.
          return {
            partNumber,
            etag:
              attempts.length === 1
                ? '"00000000000000000000000000000000"'
                : '"5289DF737DF57326FCDD22597AFB1FAC"',
          };
        }),
      ),
    );
    expect(result).toEqual({
      uploaded: { partNumber: 1, etag: '"5289DF737DF57326FCDD22597AFB1FAC"' },
      verified: true,
    });
    expect(attempts).toEqual([1, 1]);
  });

  it("marks the upload's span when a part had to be sent again, for the health report (D82)", async () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const md5Of123 = '"5289df737df57326fcdd22597afb1fac"';
    const sendWith = (etags: Array<string>) =>
      Effect.runPromise(
        uploadPartVerified("k", 1, Uint8Array.of(1, 2, 3), (partNumber) =>
          Effect.sync(() => ({ partNumber, etag: etags.shift() ?? md5Of123 })),
        ).pipe(Effect.withSpan("upload"), Effect.withTracer(tracer)),
      );
    await sendWith([md5Of123]);
    await sendWith(['"00000000000000000000000000000000"', md5Of123]);
    expect(spans.map((span) => span.attributes.get("object.part_retried"))).toEqual([
      undefined,
      true,
    ]);
  });

  it("accepts a part as unverified when the ETag is not an MD5 (local R2's are opaque)", async () => {
    const result = await Effect.runPromise(
      uploadPartVerified("k", 1, Uint8Array.of(1, 2, 3), (partNumber) =>
        Effect.succeed({ partNumber, etag: "77qvic6fgjffwykb1nrhrj3nbtji-opaque" }),
      ),
    );
    expect(result.verified).toBe(false);
  });
});

describe("objectKindOfKey", () => {
  it("names what each key in the layout holds, for the object.kind span attribute", () => {
    const db = DatabaseId.make("orders");
    expect(objectKindOfKey(manifestKey(db, ManifestId.make(7)))).toBe("Manifest");
    expect(objectKindOfKey(latestManifestKey(db))).toBe("LatestHint");
    expect(objectKindOfKey(manifestGcBoundaryKey(db))).toBe("ManifestBoundary");
    expect(objectKindOfKey(`db/${db}/gc/sst.boundary`)).toBe("Gc");
    expect(objectKindOfKey(sstKey(db, SstId.make("01JD0000000000000000000001")))).toBe("Sst");
    expect(objectKindOfKey(`db/${db}/changelog/00000000000000000009-x.sst`)).toBe("Changelog");
    expect(objectKindOfKey("probe/r2/run/1")).toBe("Other");
  });
});

describe("ObjectStore.delete against R2's limits (Workflow audit)", () => {
  /**
   * The R2 store over a bucket that holds `keys`, refuses a delete of more than 1,000 keys (R2's
   * limit), and fails every delete while `failing` is set after it has deleted `deletesBeforeFailing`
   * calls' keys. Every other bucket operation is out of scope here and dies.
   */
  const deleteAgainstR2Limits = (keys: ReadonlyArray<string>, deletesBeforeFailing: number) => {
    const stored = new Set(keys);
    const calls: Array<number> = [];
    let failing = true;
    const outOfScope = () => Effect.die(new Error("not used by this test"));
    const bucket = LsmBucketClient.of({
      raw: outOfScope(),
      head: outOfScope,
      get: outOfScope,
      list: outOfScope,
      put: outOfScope,
      createMultipartUpload: outOfScope,
      resumeMultipartUpload: outOfScope,
      delete: (input) =>
        Effect.suspend(() => {
          const batch = typeof input === "string" ? [input] : input;
          calls.push(batch.length);
          if (batch.length > 1000) {
            return Effect.fail(
              new Cloudflare.R2.R2Error({
                message: "too many keys",
                cause: new Error("delete accepts at most 1000 keys"),
              }),
            );
          }
          if (failing && calls.length > deletesBeforeFailing) {
            return Effect.fail(
              new Cloudflare.R2.R2Error({ message: "unavailable", cause: new Error("503") }),
            );
          }
          for (const key of batch) stored.delete(key);
          return Effect.void;
        }),
    });
    const withStore = <A, E>(
      use: (store: ObjectStore["Service"]) => Effect.Effect<A, E, RuntimeContext>,
    ) =>
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        return yield* use(store);
      }).pipe(
        Effect.provide(objectStoreLayerWithoutDependencies),
        Effect.provideService(LsmBucketClient, bucket),
        Effect.provide(lsmTestRuntimeContextLayer),
      );
    return {
      stored,
      calls,
      heal: () => {
        failing = false;
      },
      withStore,
    };
  };

  const keys = Array.from({ length: 2_500 }, (_, index) => `db/orders/compacted/${index}.sst`);

  it("deletes every key in calls of at most 1,000", async () => {
    const bucket = deleteAgainstR2Limits(keys, Number.POSITIVE_INFINITY);
    await Effect.runPromise(bucket.withStore((store) => store.delete(keys)));
    expect(bucket.stored.size).toBe(0);
    expect(bucket.calls).toEqual([1000, 1000, 500]);
  });

  it("fails typed when a call fails partway, and a later delete of the same keys finishes", async () => {
    const bucket = deleteAgainstR2Limits(keys, 1);
    const failed = await Effect.runPromise(
      bucket.withStore((store) => Effect.flip(store.delete(keys))),
    );
    expect(failed._tag).toBe("ObjectStoreUnavailableError");
    // The first call's keys are gone; the rest are still stored.
    expect(bucket.stored.size).toBe(1_500);
    bucket.heal();
    await Effect.runPromise(bucket.withStore((store) => store.delete(keys)));
    expect(bucket.stored.size).toBe(0);
  });
});

describe("transient R2 read failures are retried inside the read (D134)", () => {
  const TRANSIENT = "get: We encountered an internal error. Please try again. (10001)";
  /**
   * A bucket whose read operations fail with `message` on their first `failures` calls (per
   * operation), then succeed. Writes and deletes are out of scope and die.
   */
  const flakyBucket = (failures: number, message: string) => {
    const calls = { get: 0, head: 0, list: 0 };
    const fail = (operation: keyof typeof calls) =>
      calls[operation]++ < failures
        ? Effect.fail(new Cloudflare.R2.R2Error({ message, cause: new Error(message) }))
        : undefined;
    const object: Cloudflare.R2.R2Object = {
      key: "k",
      version: "v1",
      size: 3,
      etag: "v1",
      httpEtag: '"v1"',
      checksums: { toJSON: () => ({}) },
      uploaded: new Date(0),
      storageClass: "Standard",
      writeHttpMetadata: () => Effect.void,
    };
    const body: Cloudflare.R2.ObjectBody = {
      ...object,
      body: Stream.make(Uint8Array.of(1, 2, 3)),
      bodyUsed: false,
      arrayBuffer: () => Effect.succeed(Uint8Array.of(1, 2, 3).buffer),
      bytes: () => Effect.succeed(Uint8Array.of(1, 2, 3)),
      text: () => Effect.succeed(""),
      json: () => Effect.die(new Error("not used by this test")),
      blob: () => Effect.die(new Error("not used by this test")),
    };
    const listing: Cloudflare.R2.Objects = { objects: [], delimitedPrefixes: [], truncated: false };
    const outOfScope = () => Effect.die(new Error("not used by this test"));
    const bucket = LsmBucketClient.of({
      raw: outOfScope(),
      get: () => Effect.suspend(() => fail("get") ?? Effect.succeed(body)),
      head: () => Effect.suspend(() => fail("head") ?? Effect.succeed(object)),
      list: () => Effect.suspend(() => fail("list") ?? Effect.succeed(listing)),
      put: outOfScope,
      delete: outOfScope,
      createMultipartUpload: outOfScope,
      resumeMultipartUpload: outOfScope,
    });
    return { bucket, calls };
  };
  const reads: ReadonlyArray<
    readonly [
      string,
      keyof ReturnType<typeof flakyBucket>["calls"],
      (store: ObjectStore["Service"]) => Effect.Effect<unknown, unknown, RuntimeContext>,
    ]
  > = [
    ["get", "get", (store) => store.get("k")],
    ["getRange", "get", (store) => store.getRange("k", { offset: 0, length: 3 })],
    ["getVersioned", "get", (store) => store.getVersioned("k")],
    ["head", "head", (store) => store.head("k")],
    ["list", "list", (store) => store.list("db/", Option.none())],
    ["listPrefixes", "list", (store) => store.listPrefixes("db/", Option.none(), 10)],
  ];
  const runRead = (
    failures: number,
    message: string,
    read: (store: ObjectStore["Service"]) => Effect.Effect<unknown, unknown, RuntimeContext>,
  ) => {
    const flaky = flakyBucket(failures, message);
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    return Effect.gen(function* () {
      const store = yield* ObjectStore;
      return yield* Effect.result(read(store));
    }).pipe(
      Effect.provide(objectStoreLayerWithoutDependencies),
      Effect.provideService(LsmBucketClient, flaky.bucket),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.withTracer(tracer),
      Effect.runPromise,
      (promise) =>
        promise.then((result) => {
          const span = spans.find((candidate) => candidate.name.startsWith("ObjectStore."));
          return {
            result: result._tag,
            calls: flaky.calls,
            retried: span?.attributes.get("object.read_retried"),
            spanOk: span?.status._tag === "Ended" && span.status.exit._tag === "Success",
          };
        }),
    );
  };

  for (const [name, operation, read] of reads) {
    it(`${name}: one transient failure is retried, and the span ends ok, marked`, async () => {
      const outcome = await runRead(1, TRANSIENT, read);
      expect(outcome).toMatchObject({ result: "Success", retried: 1, spanOk: true });
      expect(outcome.calls[operation]).toBe(2);
    });
    it(`${name}: a transient failure on every attempt fails after ${R2_READ_RETRIES} retries`, async () => {
      const outcome = await runRead(Number.POSITIVE_INFINITY, TRANSIENT, read);
      expect(outcome).toMatchObject({ result: "Failure", retried: R2_READ_RETRIES, spanOk: false });
      expect(outcome.calls[operation]).toBe(R2_READ_RETRIES + 1);
    });
  }

  it("a failure that is a fact about the request (a range past the end) is not retried", async () => {
    const outcome = await runRead(
      Number.POSITIVE_INFINITY,
      "get: The requested range is not satisfiable (10039)",
      (store) => store.getRange("k", { offset: 10, length: 3 }),
    );
    expect(outcome).toMatchObject({ result: "Failure", retried: undefined });
    expect(outcome.calls.get).toBe(1);
  });

  it("names the transient R2 failures: service codes and code-less binding failures", () => {
    expect(
      [
        TRANSIENT,
        "get: Service Unavailable (10043)",
        "put: Reduce your concurrent request rate for the same object. (10058)",
        "Network connection lost.",
        "get: The requested range is not satisfiable (10039)",
        "put: Precondition Failed (10031)",
        "get: Unauthorized (10002)",
      ].map((message) => isTransientR2Error({ message })),
    ).toEqual([true, true, true, true, false, false, false]);
  });
});
