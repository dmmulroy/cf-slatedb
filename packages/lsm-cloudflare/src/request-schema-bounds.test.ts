import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DbHttpApi } from "./db-http-api.ts";
import { GcHttpApi } from "./gc-http-api.ts";
import { unboundedFields, unboundedRequestFields } from "./request-schema-bounds.ts";

describe("every request field is bounded (D100)", () => {
  it("finds an unbounded string, bytes, array, or record", () => {
    const loose = Schema.Struct({
      name: Schema.String,
      key: Schema.Uint8ArrayFromBase64,
      keys: Schema.Array(Schema.String.check(Schema.isMaxLength(4))),
      tags: Schema.Record(Schema.String, Schema.Number),
      maybe: Schema.OptionFromNullOr(Schema.String),
      ok: Schema.String.check(Schema.isMaxLength(10)),
    });
    expect(unboundedFields(loose.ast)).toEqual([
      "$.name (string)",
      "$.key (bytes)",
      "$.keys (array)",
      "$.tags (record)",
      "$.maybe (string)",
    ]);
  });

  it("the database API's and the GC API's requests have none", () => {
    expect(unboundedRequestFields(DbHttpApi)).toEqual([]);
    expect(unboundedRequestFields(GcHttpApi)).toEqual([]);
  });
});
