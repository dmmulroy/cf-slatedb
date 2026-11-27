import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  DEFAULT_LSM_SETTINGS,
  MAX_BATCH_BYTES,
  MAX_BATCH_OPERATIONS,
  MAX_CHANGELOG_ENTRIES,
  MAX_CHECKPOINTS_PER_DATABASE,
  MAX_KEY_BYTES,
  MAX_MERGED_VALUE_BYTES,
  MAX_TRANSACTION_READ_KEYS,
  MAX_TRANSACTION_READ_RANGES,
  MAX_TTL_MILLIS,
  MAX_VALUE_BYTES,
} from "@shaledb/lsm-core";
import { describe, expect, it } from "vite-plus/test";

import {
  MAX_CHANGELOG_RETENTION_MS,
  MAX_CHECKPOINT_NAME_LENGTH,
  MAX_SCAN_PAGE_ROWS,
  MAX_SEAL_SUCCESSORS,
} from "./db-http-api.ts";
import { MAX_REQUEST_BODY_BYTES } from "./db-http-request.ts";
import { SCAN_PAGE_EXAMINED_BYTES } from "./db-reader.ts";
import { MAX_MEMTABLE_PAGE_BYTES } from "./mem-table.ts";

/** Each constant `docs/api-compatibility.md`'s "Limits of v1" names, with its value. */
const LIMITS: Readonly<Record<string, number>> = {
  MAX_BATCH_OPERATIONS,
  MAX_BATCH_BYTES,
  MAX_KEY_BYTES,
  MAX_VALUE_BYTES,
  MAX_MERGED_VALUE_BYTES,
  MAX_TTL_MILLIS,
  MAX_SCAN_PAGE_ROWS,
  MAX_MEMTABLE_PAGE_BYTES,
  MAX_REQUEST_BODY_BYTES,
  MAX_TRANSACTION_READ_KEYS,
  MAX_TRANSACTION_READ_RANGES,
  MAX_SEAL_SUCCESSORS,
  MAX_CHECKPOINT_NAME_LENGTH,
  MAX_CHECKPOINTS_PER_DATABASE,
  MAX_CHANGELOG_ENTRIES,
  MAX_CHANGELOG_RETENTION_MS,
  SCAN_PAGE_EXAMINED_BYTES,
  gcMaxRetainedManifestLoads: DEFAULT_LSM_SETTINGS.gcMaxRetainedManifestLoads,
};

const policy = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../..", "docs/api-compatibility.md"),
  "utf8",
);
const section = policy.slice(
  policy.indexOf("## Limits of v1"),
  policy.indexOf("\n## ", policy.indexOf("## Limits of v1") + 1),
);
const rows = section
  .split("\n")
  .filter((line) => line.startsWith("| ") && !line.startsWith("| ---"));

describe("the Limits of v1 table in docs/api-compatibility.md", () => {
  it("states each listed constant's current value", () => {
    for (const [name, value] of Object.entries(LIMITS)) {
      const row = rows.find((line) => line.includes(`\`${name}\``));
      // A limit changed in code without the table, or a row removed, fails here.
      expect([name, row?.includes(value.toLocaleString("en-US"))]).toEqual([name, true]);
    }
  });

  it("names no constant this test does not check", () => {
    const named = rows.flatMap((line) =>
      [...line.matchAll(/`((?:MAX|SCAN)_[A-Z_]+)`/g)].map((match) => match[1]),
    );
    expect(named.filter((name) => name !== undefined && !(name in LIMITS))).toEqual([]);
  });
});
