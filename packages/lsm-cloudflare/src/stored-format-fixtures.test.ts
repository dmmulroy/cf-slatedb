import { readFileSync } from "node:fs";

import { Exit, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { COMPACTOR_STATE_FORMAT_VERSION, CompactorState } from "./compactor.ts";

/**
 * The compactor's durable state as v1 writes it, every optional field set (D105). The next change
 * to it is checked against this shape.
 */
const RELEASE_SHAPE = readFileSync(
  new URL("./fixtures/compactor-state-v1-release-shape.json", import.meta.url),
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
