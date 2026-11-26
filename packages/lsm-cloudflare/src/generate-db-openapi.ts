/**
 * Writes the public database API's OpenAPI specification to `docs/openapi/cf-slatedb-v1.json`
 * (`vp run generate:openapi`). Never edit that file by hand: change the contract (`DbHttpApi`) or
 * its examples (`db-openapi.ts`), regenerate, and inspect the diff.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { DB_OPENAPI_SPEC_PATH, dbOpenApiSpec } from "./db-openapi.ts";

const target = join(dirname(fileURLToPath(import.meta.url)), "../../..", DB_OPENAPI_SPEC_PATH);
mkdirSync(dirname(target), { recursive: true });
writeFileSync(target, `${JSON.stringify(dbOpenApiSpec(), null, 2)}\n`);
console.log(`Wrote ${DB_OPENAPI_SPEC_PATH}`);
