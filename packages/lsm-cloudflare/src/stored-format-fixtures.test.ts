import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Exit, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { COMPACTOR_STATE_FORMAT_VERSION, CompactorState, RunningCompaction } from "./compactor.ts";

/**
 * The compactor's durable state as v1 writes it, every optional field set (D105). The next change
 * to it is checked against this shape.
 */
const RELEASE_SHAPE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "./fixtures/compactor-state-v1-release-shape.json"),
  "utf8",
);
const Json = Schema.fromJsonString(CompactorState);

describe("the v1 release compactor-state shape (D105)", () => {
  it("decodes, and writes back exactly what it read", () => {
    const decoded = Schema.decodeUnknownExit(Json)(RELEASE_SHAPE);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(decoded.value.formatVersion).toBe(COMPACTOR_STATE_FORMAT_VERSION);
    expect(JSON.parse(Schema.encodeSync(Json)(decoded.value))).toEqual(JSON.parse(RELEASE_SHAPE));
  });

  it("reads state written before lastProgressAt and collectionDueAt existed", () => {
    const older = JSON.parse(RELEASE_SHAPE);
    delete older.collectionDueAt;
    delete older.corruptSsts;
    delete older.formatVersion;
    delete older.running[0].lastProgressAt;
    const decoded = Schema.decodeUnknownExit(Json)(JSON.stringify(older));
    expect(Exit.isSuccess(decoded)).toBe(true);
  });
});

/**
 * The compactor state's schema at `f3b2ad2` and `c3807f7`, rebuilt from today's: a running job
 * had no `lastProgressAt` (D94). Every other field is unchanged since both commits (checked with
 * `git diff <commit> -- compactor.ts`).
 */
const { lastProgressAt: _progress, ...olderJob } = RunningCompaction.fields;
const OLDER = Schema.fromJsonString(
  Schema.Struct({ ...CompactorState.fields, running: Schema.Array(Schema.Struct(olderJob)) }),
);

describe("older code reads today's compactor state (D105)", () => {
  it("f3b2ad2 and c3807f7 decode it and drop lastProgressAt when they rewrite it; today's code then falls back to startedAt", () => {
    const rewritten = JSON.parse(
      Schema.encodeSync(OLDER)(Schema.decodeUnknownSync(OLDER)(RELEASE_SHAPE)),
    );
    expect(rewritten.running[0]).not.toHaveProperty("lastProgressAt");
    expect(rewritten.collectionDueAt).toBe(JSON.parse(RELEASE_SHAPE).collectionDueAt);
    const back = Schema.decodeUnknownExit(Json)(JSON.stringify(rewritten));
    if (Exit.isFailure(back)) throw new Error(String(back));
    expect(back.value.running[0]?.lastProgressAt._tag).toBe("None");
  });
});
