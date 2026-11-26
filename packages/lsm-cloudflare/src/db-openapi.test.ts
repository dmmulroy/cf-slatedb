import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vite-plus/test";

import { DB_OPENAPI_SPEC_PATH, dbOpenApiSpec } from "./db-openapi.ts";

const spec = dbOpenApiSpec();
const operations = Object.values(spec.paths).flatMap((item) =>
  [item.get, item.post, item.delete].flatMap((operation) =>
    operation === undefined ? [] : [operation],
  ),
);

/** Whether a JSON Schema object carries an example (the `examples` keyword). */
const hasExample = (schema: object) => "examples" in schema;

describe("the public API's OpenAPI specification (D64)", () => {
  it("is checked in exactly as generated", () => {
    const checkedIn: unknown = JSON.parse(
      readFileSync(
        join(dirname(fileURLToPath(import.meta.url)), "../../..", DB_OPENAPI_SPEC_PATH),
        "utf8",
      ),
    );
    // A contract change without `vp run generate:openapi` fails here.
    expect(checkedIn).toEqual(JSON.parse(JSON.stringify(spec)));
  });

  it("describes every operation and gives an example of everything a caller sends", () => {
    expect(operations.length).toBeGreaterThan(20);
    for (const operation of operations) {
      expect([operation.operationId, operation.summary !== undefined]).toEqual([
        operation.operationId,
        true,
      ]);
      expect([operation.operationId, operation.description !== undefined]).toEqual([
        operation.operationId,
        true,
      ]);
      for (const parameter of operation.parameters) {
        expect([operation.operationId, parameter.name, hasExample(parameter.schema)]).toEqual([
          operation.operationId,
          parameter.name,
          true,
        ]);
      }
      const body = operation.requestBody?.content["application/json"];
      if (body !== undefined) {
        expect([operation.operationId, hasExample(body.schema)]).toEqual([
          operation.operationId,
          true,
        ]);
      }
    }
  });

  it("declares the bearer scheme and x-request-id on every response, with error examples", () => {
    expect(spec.security).toEqual([{ bearerToken: [] }]);
    expect(spec.components.securitySchemes["bearerToken"]).toMatchObject({
      type: "http",
      scheme: "bearer",
    });
    for (const operation of operations) {
      for (const [status, response] of Object.entries(operation.responses)) {
        expect([
          operation.operationId,
          status,
          response.headers?.["x-request-id"] !== undefined,
        ]).toEqual([operation.operationId, status, true]);
        const json = response.content?.["application/json"];
        if (json !== undefined && !status.startsWith("2") && status !== "400" && status !== "409") {
          expect([operation.operationId, status, hasExample(json.schema)]).toEqual([
            operation.operationId,
            status,
            true,
          ]);
        }
      }
    }
  });
});
