import { Arbitrary, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  hasSstCollectionCandidates,
  planManifestCollection,
  planSstCollection,
  prepareGarbageCollection,
  type StoredSst,
} from "./garbage-collection-plan.ts";
import {
  CheckpointId,
  DatabaseId,
  ManifestId,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "./ids.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";
import {
  addCheckpoint,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type Manifest,
  replaceManifestFields,
  sstCreatedAt,
  type SstHandle,
} from "./manifest.ts";
import { encodeUlid } from "./ulid.ts";

const DB = DatabaseId.make("orders");
const SOURCE = DatabaseId.make("source");

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** SST `n`, created at time `n * 100`. */
const sstId = (n: number) =>
  SstId.make(
    encodeUlid(
      n * 100,
      Array.from({ length: 16 }, () => n % 32),
    ),
  );

const handle = (n: number, owner: Option.Option<DatabaseId> = Option.none()): SstHandle => ({
  id: sstId(n),
  firstKey: Uint8Array.of(0),
  lastKey: Uint8Array.of(9),
  sizeBytes: 100,
  rowCount: 1,
  metaOffset: 50,
  metaLength: 50,
  owner,
  view: Option.none(),
});

const empty = initialManifest(DB, {
  keyRange: Option.none(),
  databaseOptions: DEFAULT_DATABASE_OPTIONS,
});

const manifestWith = (l0: ReadonlyArray<number>, external: ReadonlyArray<number> = []): Manifest =>
  replaceManifestFields(empty, {
    l0: [...l0.map((n) => handle(n)), ...external.map((n) => handle(n, Option.some(SOURCE)))],
  });

const stored = (ids: ReadonlyArray<number>): ReadonlyArray<StoredSst> =>
  ids.map((n) => ({ key: `db/orders/compacted/${sstId(n)}.sst`, sstId: sstId(n) }));

describe("planSstCollection", () => {
  it("never deletes an SST a retained manifest references, nor one created at or after the boundary", () => {
    assertProperty(
      Arbitrary.all({
        manifests: Arbitrary.array(Arbitrary.array(smallInt(1, 30), { maxLength: 6 }), {
          minLength: 1,
          maxLength: 5,
        }),
        objects: Arbitrary.array(smallInt(1, 30), { maxLength: 30 }),
        boundary: smallInt(0, 3200),
      }),
      ({ manifests, objects, boundary }) => {
        const retained = manifests.map((l0) => manifestWith(l0));
        const deleted = planSstCollection({
          retained,
          stored: stored(objects),
          boundary: Timestamp.make(boundary),
        });
        const referenced = new Set(manifests.flat().map(sstId));
        const deletedIds = new Set(deleted.map((object) => object.sstId));
        const expected = objects.filter((n) => !manifests.flat().includes(n) && n * 100 < boundary);
        return (
          deleted.every(
            (object) => !referenced.has(object.sstId) && sstCreatedAt(object.sstId) < boundary,
          ) &&
          // ...and it deletes every such SST, so orphans do not accumulate.
          expected.every((n) => deletedIds.has(sstId(n)))
        );
      },
      { runs: 300 },
    );
  });

  it("ignores SSTs other databases own (a fork's source files) and keeps changelog SSTs", () => {
    const fork = replaceManifestFields(manifestWith([1], [2]), {
      // An explicit owner equal to the database itself is its own SST, as for readers.
      sortedRuns: [{ id: SortedRunId.make(0), ssts: [handle(5, Option.some(DB))] }],
      changelog: [
        {
          sst: handle(3),
          firstSeq: SeqNum.make(1),
          lastSeq: SeqNum.make(2),
          createdAt: Timestamp.make(0),
        },
      ],
    });
    const deleted = planSstCollection({
      retained: [fork],
      stored: stored([1, 2, 3, 4, 5]),
      boundary: Timestamp.make(10_000),
    });
    // SST 2 is the source's (stored under the source's prefix, never listed here); a stray object
    // with its id under this prefix is unreferenced by this database and is collected.
    expect(deleted.map((object) => object.sstId)).toEqual([sstId(2), sstId(4)]);
  });
});

describe("planManifestCollection", () => {
  it("keeps the latest, checkpoint-pinned, and recently superseded manifests", () => {
    const latest = Result.getOrThrow(
      addCheckpoint(empty, {
        id: CheckpointId.make("01JD00000000000000000000CP"),
        manifestId: ManifestId.make(2),
        createdAt: Timestamp.make(0),
        expireAt: Option.none(),
        holder: Option.some(DatabaseId.make("fork")),
        name: Option.none(),
      }),
    );
    const plan = planManifestCollection({
      latestId: ManifestId.make(5),
      latest,
      stored: [1, 2, 3, 4, 5].map((id) => ({
        id: ManifestId.make(id),
        uploadedAt: Timestamp.make(id * 1000),
      })),
      now: Timestamp.make(5000),
      minAge: 1500,
    });
    // 1 and 2 were superseded by 3000; 2 is pinned; 3 was superseded at 4000, within minAge of
    // now (5000), so readers may still hold it; 4 was superseded just now; 5 is the latest.
    expect(plan).toEqual({ deletable: [1], retained: [2, 3, 4, 5] });
  });

  it("measures a manifest's age from when it was superseded, not when it was written", () => {
    const plan = planManifestCollection({
      latestId: ManifestId.make(2),
      latest: empty,
      stored: [
        // The newest manifest for an hour, superseded a moment ago.
        { id: ManifestId.make(1), uploadedAt: Timestamp.make(0) },
        { id: ManifestId.make(2), uploadedAt: Timestamp.make(3_600_000) },
      ],
      now: Timestamp.make(3_600_500),
      minAge: 1_000,
    });
    expect(plan.deletable).toEqual([]);
  });
});

describe("prepareGarbageCollection", () => {
  it("advances the boundary only when asked and changes nothing on an idle database", () => {
    const options = { now: Timestamp.make(10_000), cutoff: Timestamp.make(4_000) };
    expect(prepareGarbageCollection(empty, { ...options, advanceBoundary: false })).toBe(empty);
    const advanced = prepareGarbageCollection(empty, { ...options, advanceBoundary: true });
    expect(advanced.gcSstBoundary).toBe(4_000);
    expect(prepareGarbageCollection(advanced, { ...options, advanceBoundary: true })).toBe(
      advanced,
    );
  });

  it("finds candidates only among old SSTs the latest manifest does not reference", () => {
    const latest = manifestWith([1, 2]);
    expect(hasSstCollectionCandidates(latest, stored([1, 2, 40]), Timestamp.make(3_000))).toBe(
      false,
    );
    // SST 3 was created at 300 ms: not before a 300 ms cutoff, but before 301 ms.
    expect(hasSstCollectionCandidates(latest, stored([1, 2, 3]), Timestamp.make(300))).toBe(false);
    expect(hasSstCollectionCandidates(latest, stored([1, 2, 3]), Timestamp.make(301))).toBe(true);
  });
});
