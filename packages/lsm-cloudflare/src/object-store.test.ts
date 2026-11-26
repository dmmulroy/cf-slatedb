import { DatabaseId, ManifestId, md5Hex, SstId } from "@cf-slatedb/lsm-core";
import { Arbitrary, Bytes, Effect, Option, Schema, Stream } from "effect";
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
import {
  ObjectStore,
  objectStoreTestLayer,
  objectStoreWithTransitCorruptionTestLayer,
  PART_INTEGRITY_ATTEMPTS,
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
