import { Option, Result } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CheckpointId, DatabaseId, ManifestId, SeqNum, SstId, Timestamp } from "./ids.ts";
import {
  addCallerCheckpoint,
  addCheckpoint,
  type ChangelogEntry,
  changelogAfterSeqOf,
  type Checkpoint,
  commitL0Flush,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type Manifest,
  MAX_CHANGELOG_ENTRIES,
  MAX_CHECKPOINTS_PER_DATABASE,
  replaceManifestFields,
  type SstHandle,
} from "./manifest.ts";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = (index: number) => {
  let suffix = "";
  for (let rest = index; suffix.length < 22; rest = Math.floor(rest / 32)) {
    suffix = CROCKFORD[rest % 32] + suffix;
  }
  return `01JD${suffix}`;
};

const checkpoint = (index: number, holder: Option.Option<DatabaseId>): Checkpoint => ({
  id: CheckpointId.make(ulid(index)),
  manifestId: ManifestId.make(1),
  createdAt: Timestamp.make(0),
  expireAt: Option.none(),
  holder,
  name: Option.none(),
});

const holding = (count: number): Manifest => {
  let manifest = initialManifest(DatabaseId.make("orders"), {
    keyRange: Option.none(),
    databaseOptions: DEFAULT_DATABASE_OPTIONS,
  });
  for (let index = 0; index < count; index++) {
    manifest = Result.getOrThrow(addCheckpoint(manifest, checkpoint(index, Option.none())));
  }
  return manifest;
};

describe("checkpoints a caller may hold (D99)", () => {
  it("refuses one more than the limit, typed, saying how many exist", () => {
    const full = holding(MAX_CHECKPOINTS_PER_DATABASE);
    const below = holding(MAX_CHECKPOINTS_PER_DATABASE - 1);
    expect(Result.isSuccess(addCallerCheckpoint(below, checkpoint(5_000, Option.none())))).toBe(
      true,
    );
    const refused = addCallerCheckpoint(full, checkpoint(5_000, Option.none()));
    expect(Result.isFailure(refused) && refused.failure).toMatchObject({
      _tag: "CheckpointLimitError",
      checkpoints: MAX_CHECKPOINTS_PER_DATABASE,
      limit: MAX_CHECKPOINTS_PER_DATABASE,
    });
  });

  it("never refuses a repeat, nor a fork's or successor's hold", () => {
    const full = holding(MAX_CHECKPOINTS_PER_DATABASE);
    expect(addCallerCheckpoint(full, checkpoint(0, Option.none()))).toEqual(Result.succeed(full));
    const held = addCallerCheckpoint(
      full,
      checkpoint(5_001, Option.some(DatabaseId.make("orders-a"))),
    );
    expect(Result.isSuccess(held) && held.success.checkpoints.length).toBe(
      MAX_CHECKPOINTS_PER_DATABASE + 1,
    );
    // Holds do not use up the callers' allowance.
    const withHolds = Result.getOrThrow(
      addCheckpoint(
        holding(MAX_CHECKPOINTS_PER_DATABASE - 1),
        checkpoint(6_000, Option.some(DatabaseId.make("orders-b"))),
      ),
    );
    expect(Result.isSuccess(addCallerCheckpoint(withHolds, checkpoint(6_001, Option.none())))).toBe(
      true,
    );
  });
});

const handle = (index: number): SstHandle => ({
  id: SstId.make(ulid(index)),
  firstKey: Uint8Array.of(1),
  lastKey: Uint8Array.of(2),
  sizeBytes: 100,
  rowCount: 1,
  metaOffset: 50,
  metaLength: 50,
  owner: Option.none(),
  view: Option.none(),
});

const entry = (index: number): ChangelogEntry => ({
  sst: handle(1_000_000 + index),
  firstSeq: SeqNum.make(index * 10 + 1),
  lastSeq: SeqNum.make(index * 10 + 10),
  createdAt: Timestamp.make(index),
});

describe("changelog entries one manifest keeps (D99)", () => {
  it("a flush past the bound drops the oldest entry and raises changelogAfterSeq past it", () => {
    const full = replaceManifestFields(holding(0), {
      changelog: Array.from({ length: MAX_CHANGELOG_ENTRIES }, (_, index) => entry(index)),
      lastL0Seq: SeqNum.make(MAX_CHANGELOG_ENTRIES * 10),
    });
    const before = changelogAfterSeqOf(full);
    const next = MAX_CHANGELOG_ENTRIES;
    const flushed = Result.getOrThrow(
      commitL0Flush(full, {
        writerEpoch: full.writerEpoch,
        sst: handle(next),
        lastSeq: SeqNum.make(next * 10 + 10),
        clockTick: Timestamp.make(next),
        changelog: Option.some(entry(next)),
      }),
    );
    expect(flushed.changelog.length).toBe(MAX_CHANGELOG_ENTRIES);
    expect(flushed.changelog[0]).toEqual(entry(1));
    expect(flushed.changelog.at(-1)).toEqual(entry(next));
    // The feed can no longer prove it holds entry 0's changes: a consumer behind them expires.
    expect(before).toBe(0);
    expect(changelogAfterSeqOf(flushed)).toBe(entry(0).lastSeq);
  });

  it("keeps every entry up to the bound", () => {
    const below = replaceManifestFields(holding(0), {
      changelog: Array.from({ length: MAX_CHANGELOG_ENTRIES - 1 }, (_, index) => entry(index)),
    });
    const flushed = Result.getOrThrow(
      commitL0Flush(below, {
        writerEpoch: below.writerEpoch,
        sst: handle(9_999),
        lastSeq: SeqNum.make(1),
        clockTick: Timestamp.make(1),
        changelog: Option.some(entry(MAX_CHANGELOG_ENTRIES)),
      }),
    );
    expect(flushed.changelog.length).toBe(MAX_CHANGELOG_ENTRIES);
    expect(flushed.changelog[0]).toEqual(entry(0));
  });
});
