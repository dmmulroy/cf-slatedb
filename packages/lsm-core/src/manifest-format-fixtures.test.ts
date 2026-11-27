import { readFileSync } from "node:fs";

import { Exit, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseState, Manifest, MANIFEST_FORMAT_VERSION, ManifestJson } from "./manifest.ts";

/**
 * The manifest as v1 writes it, every stored field set (D105): open-and-sealed and removed. The
 * shape a production installation created at the release commit stores; the next change to the
 * manifest is checked against it, in both directions of a rollback.
 */
const fixture = (name: string) =>
  readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8");
const FIXTURES = {
  sealed: fixture("manifest-v1-release-shape.json"),
  removed: fixture("manifest-v1-release-removed.json"),
};

const decode = Schema.decodeUnknownExit(ManifestJson);
const encode = Schema.encodeSync(ManifestJson);

/**
 * The manifest schemas of two older commits, rebuilt from today's: `c3807f7` (what the previews
 * run) lacks `recentCompactionJobIds` (D103) and the `Removed` state's `removedAt` and `cleared`
 * (D91); `f3b2ad2` (the last soaked commit) also lacks `changelogAfterSeq` (D80). Every other
 * field's schema is unchanged since both (checked with `git diff <commit> -- manifest.ts`).
 */
const { recentCompactionJobIds: _jobs, state: _state, ...unchanged } = Manifest.fields;
const OLD_STATE = Schema.Union([
  Schema.TaggedStruct("Open", {}),
  Schema.TaggedStruct("Sealed", DatabaseState.members[1].fields),
  Schema.TaggedStruct("Removed", {}),
]);
const C3807F7 = Schema.fromJsonString(Schema.Struct({ ...unchanged, state: OLD_STATE }));
const { changelogAfterSeq: _feed, ...beforeD80 } = unchanged;
const F3B2AD2 = Schema.fromJsonString(Schema.Struct({ ...beforeD80, state: OLD_STATE }));

/** What older code writes after reading `text`: its own schema's encoding of what it decoded. */
const rewrittenBy = <T>(schema: Schema.Codec<T, string>, text: string) =>
  JSON.parse(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(text)));

describe("the v1 release manifest shapes (D105)", () => {
  for (const [name, text] of Object.entries(FIXTURES)) {
    it(`${name}: decodes, and writes back exactly what it read: no field renamed, dropped, or added`, () => {
      const decoded = decode(text);
      if (Exit.isFailure(decoded)) throw new Error(String(decoded));
      expect(decoded.value.formatVersion).toBe(MANIFEST_FORMAT_VERSION);
      // A new field (even one with a decoding default) changes what is written: update the
      // fixtures only after deciding what older code does with it (docs/operations.md).
      expect(JSON.parse(encode(decoded.value))).toEqual(JSON.parse(text));
    });
  }

  it("the removed fixture records its removal time and that its state is cleared (D91)", () => {
    const decoded = decode(FIXTURES.removed);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(decoded.value.state).toMatchObject({ _tag: "Removed", cleared: true });
  });
});

describe("older code reads today's manifests, and today's code reads what it writes back (D105)", () => {
  for (const [commit, schema, dropped] of [
    ["c3807f7", C3807F7, ["recentCompactionJobIds"]],
    ["f3b2ad2", F3B2AD2, ["recentCompactionJobIds", "changelogAfterSeq"]],
  ] as const) {
    it(`${commit} decodes both shapes and, rewriting them, drops ${dropped.join(" and ")} and the removal's fields`, () => {
      for (const text of Object.values(FIXTURES)) {
        const rewritten = rewrittenBy(schema, text);
        for (const field of dropped) expect(rewritten).not.toHaveProperty(field);
        // Today's code reads the rewrite with each field's default (D80, D91, D103).
        const back = decode(JSON.stringify(rewritten));
        if (Exit.isFailure(back)) throw new Error(String(back));
        expect(back.value.recentCompactionJobIds).toEqual([]);
      }
      const removed = rewrittenBy(schema, FIXTURES.removed);
      expect(removed.state).toEqual({ _tag: "Removed" });
      const back = decode(JSON.stringify(removed));
      if (Exit.isFailure(back)) throw new Error(String(back));
      // No removal time reads as removed when read; not cleared (D91's defaults).
      expect(back.value.state).toMatchObject({ _tag: "Removed", cleared: false });
    });
  }

  it("a field from a later release with the same format is read and then dropped by today's code", () => {
    // A field whose loss changes behavior must therefore raise MANIFEST_FORMAT_VERSION (D64).
    const newer = JSON.stringify({ ...JSON.parse(FIXTURES.sealed), fieldFromALaterRelease: 7 });
    const decoded = decode(newer);
    if (Exit.isFailure(decoded)) throw new Error(String(decoded));
    expect(JSON.parse(encode(decoded.value))).not.toHaveProperty("fieldFromALaterRelease");
    // A raised format is refused at the single decode point (ManifestReader), not by the schema.
    const raised = JSON.stringify({ ...JSON.parse(FIXTURES.sealed), formatVersion: 2 });
    expect(Exit.isSuccess(decode(raised))).toBe(true);
  });
});
