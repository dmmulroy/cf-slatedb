import { Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CheckpointId,
  DatabaseId,
  ManifestId,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "./ids.ts";
import {
  type Checkpoint,
  type ChangelogEntry,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type Manifest,
  ManifestJson,
  replaceManifestFields,
  type SstHandle,
} from "./manifest.ts";

/**
 * The manifest's size budget (D99): bytes per SST handle, checkpoint, and changelog entry, and
 * how long encoding and decoding a manifest takes at 1, 10, and 50 MiB. Every commit encodes the
 * whole manifest and every cold read decodes it. Times are Node's on the machine that runs the
 * tests, recorded in D99; the assertions only keep the measurement honest.
 */
const encode = Schema.encodeSync(ManifestJson);
const decode = Schema.decodeSync(ManifestJson);
const bytes = (text: string) => new TextEncoder().encode(text).length;

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = (index: number) => {
  let suffix = "";
  for (let rest = index; suffix.length < 22; rest = Math.floor(rest / 32)) {
    suffix = CROCKFORD[rest % 32] + suffix;
  }
  return `01JD${suffix}`;
};

const handle = (index: number): SstHandle => ({
  id: SstId.make(ulid(index)),
  firstKey: Uint8Array.from({ length: 16 }, (_, byte) => (index + byte) & 0xff),
  lastKey: Uint8Array.from({ length: 16 }, (_, byte) => (index + byte + 1) & 0xff),
  sizeBytes: 64 * 1024 * 1024,
  rowCount: 600_000,
  metaOffset: 63 * 1024 * 1024,
  metaLength: 512 * 1024,
  owner: Option.none(),
  view: Option.none(),
});

const checkpoint = (index: number): Checkpoint => ({
  id: CheckpointId.make(ulid(index)),
  manifestId: ManifestId.make(index + 1),
  createdAt: Timestamp.make(1_764_000_000_000 + index),
  expireAt: Option.some(Timestamp.make(1_766_000_000_000 + index)),
  holder: Option.none(),
  name: Option.none(),
});

const changelogEntry = (index: number): ChangelogEntry => ({
  sst: handle(index),
  firstSeq: SeqNum.make(index * 1_000 + 1),
  lastSeq: SeqNum.make(index * 1_000 + 1_000),
  createdAt: Timestamp.make(1_764_000_000_000 + index),
});

const empty = initialManifest(DatabaseId.make("orders"), {
  keyRange: Option.none(),
  databaseOptions: DEFAULT_DATABASE_OPTIONS,
});

const withSsts = (count: number): Manifest =>
  replaceManifestFields(empty, {
    sortedRuns: [
      { id: SortedRunId.make(0), ssts: Array.from({ length: count }, (_, i) => handle(i)) },
    ],
  });

/** Bytes one more item adds to the encoded manifest. */
const perItem = (build: (count: number) => Manifest) =>
  (bytes(encode(build(1_001))) - bytes(encode(build(1)))) / 1_000;

const timed = (run: () => unknown) => {
  const start = performance.now();
  run();
  return Math.round(performance.now() - start);
};

describe("the manifest's size budget (D99)", () => {
  it("measures bytes per SST handle, checkpoint, and changelog entry", () => {
    const sst = perItem(withSsts);
    const checkpoints = perItem((count) =>
      replaceManifestFields(empty, {
        checkpoints: Array.from({ length: count }, (_, i) => checkpoint(i)),
      }),
    );
    const changelog = perItem((count) =>
      replaceManifestFields(empty, {
        changelog: Array.from({ length: count }, (_, i) => changelogEntry(i)),
      }),
    );
    console.info(
      JSON.stringify({
        bytesPerSst: sst,
        bytesPerCheckpoint: checkpoints,
        bytesPerChangelogEntry: changelog,
      }),
    );
    // 16-byte keys: about 220 bytes per handle (D92).
    expect(sst).toBeGreaterThan(150);
    expect(sst).toBeLessThan(300);
    expect(checkpoints).toBeLessThan(sst);
    expect(changelog).toBeGreaterThan(sst);
  });

  it("times encoding and decoding a manifest of 1, 10, and 50 MiB", { timeout: 120_000 }, () => {
    const sst = perItem(withSsts);
    const results = [1, 10, 50].map((mebibytes) => {
      const manifest = withSsts(Math.round((mebibytes * 1024 * 1024) / sst));
      let text = "";
      const encodeMs = timed(() => {
        text = encode(manifest);
      });
      const heapBefore = process.memoryUsage().heapUsed;
      let decoded: Manifest | undefined;
      const decodeMs = timed(() => {
        decoded = decode(text);
      });
      // Heap the decoded manifest holds, beside its JSON text: a rough figure (no forced GC).
      const decodedHeapMiB = Math.round(
        (process.memoryUsage().heapUsed - heapBefore) / 1024 / 1024,
      );
      return {
        mebibytes,
        ssts: decoded?.sortedRuns[0]?.ssts.length,
        bytes: bytes(text),
        encodeMs,
        decodeMs,
        decodedHeapMiB,
      };
    });
    console.info(JSON.stringify(results));
    for (const result of results) {
      expect(result.bytes).toBeGreaterThan(result.mebibytes * 1024 * 1024 * 0.9);
    }
  });
});
