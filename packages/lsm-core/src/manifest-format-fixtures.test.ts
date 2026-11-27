import { readFileSync } from "node:fs";

import { Exit, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { MANIFEST_FORMAT_VERSION, ManifestJson } from "./manifest.ts";

/**
 * The manifest as v1 writes it, every optional field set (D105): the shape a production
 * installation created at the release commit stores. The next change to the manifest is checked
 * against it, in both directions of a rollback.
 */
const RELEASE_SHAPE = readFileSync(
  new URL("./fixtures/manifest-v1-release-shape.json", import.meta.url),
  "utf8",
);

const decode = Schema.decodeUnknownExit(ManifestJson);
const encode = Schema.encodeSync(ManifestJson);

describe("the v1 release manifest shape (D105)", () => {
  it("decodes, and writes back exactly what it read: no field is renamed, dropped, or added", () => {
    const decoded = decode(RELEASE_SHAPE);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(decoded.value.formatVersion).toBe(MANIFEST_FORMAT_VERSION);
    // A new field (even one with a decoding default) changes what is written: update the fixture
    // only after deciding what older code does with it (docs/operations.md, "Rolling back").
    expect(JSON.parse(encode(decoded.value))).toEqual(JSON.parse(RELEASE_SHAPE));
  });

  it("is read by code that does not know a newer field, which drops that field when it rewrites", () => {
    // What v1 does with a manifest a later release wrote with an extra field and the same format:
    // it reads it, and its next commit writes the manifest without the field. A field whose loss
    // changes behavior must therefore raise MANIFEST_FORMAT_VERSION, which v1 refuses (D64).
    const newer = JSON.stringify({ ...JSON.parse(RELEASE_SHAPE), fieldFromALaterRelease: 7 });
    const decoded = decode(newer);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(JSON.parse(encode(decoded.value))).not.toHaveProperty("fieldFromALaterRelease");
    // A raised format is refused at the single decode point (ManifestReader), not here; the
    // schema itself still decodes it, so the check there must stay.
    const raised = JSON.stringify({ ...JSON.parse(RELEASE_SHAPE), formatVersion: 2 });
    expect(Exit.isSuccess(decode(raised))).toBe(true);
  });
});
