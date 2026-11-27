import { readFileSync } from "node:fs";

import { Exit, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardMap } from "./shard-map.ts";

/** A shard group's map as v1 stores it in its directory, with a split pending (D105). */
const RELEASE_SHAPE = readFileSync(
  new URL("./fixtures/shard-map-v1-release-shape.json", import.meta.url),
  "utf8",
);
const Json = Schema.fromJsonString(ShardMap);

describe("the v1 release shard-map shape (D105)", () => {
  it("decodes, and writes back exactly what it read", () => {
    const decoded = Schema.decodeUnknownExit(Json)(RELEASE_SHAPE);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(JSON.parse(Schema.encodeSync(Json)(decoded.value))).toEqual(JSON.parse(RELEASE_SHAPE));
  });
});
