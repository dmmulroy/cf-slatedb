import { RowValue, SeqNum, type Row } from "@shaledb/lsm-core";
import { Effect, Layer, Option, Stream } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { DbClient } from "./db-client.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { ReaderUnavailableError } from "./db-reader.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";

const row = (key: number): Row => ({
  key: Uint8Array.of(key),
  seq: SeqNum.make(1),
  value: RowValue.Value({ bytes: Uint8Array.of(key) }),
  createTs: Option.none(),
  expireTs: Option.none(),
});

/** A database client whose scans return keys 1..3, then fail as overtaken, naming key 4. */
const overtakenScans = Layer.effect(
  DbClient,
  Effect.map(DbClient, (client) =>
    DbClient.of({
      ...client,
      scan: (databaseId) =>
        Stream.concat(
          Stream.fromIterable([row(1), row(2), row(3)]),
          Stream.fail(
            new ReaderUnavailableError({
              databaseId,
              reason: "StorageUnavailable",
              message: "overtaken by a flush after returning rows",
              resumeFrom: Uint8Array.of(4),
            }),
          ),
        ),
    }),
  ),
);

describe("the public scan of a scan overtaken after it returned rows (D93)", () => {
  it("answers the rows it has and resumeFrom, as a page that ended early", async () => {
    const answer = await Effect.gen(function* () {
      const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
      const web = yield* serveLsmTestRequest(
        app,
        new Request("http://api.internal/v1/databases/orders/scan", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            range: { start: "", end: null },
            consistency: { _tag: "Latest" },
            snapshot: null,
            limit: 10,
          }),
        }),
      );
      return { status: web.status, body: JSON.parse(web.text) };
    }).pipe(
      Effect.scoped,
      Effect.provide(overtakenScans.pipe(Layer.provideMerge(dbServicesTestLayer()))),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(answer.status).toBe(200);
    expect(answer.body.rows.map((returned: { key: string }) => returned.key)).toEqual([
      "AQ==",
      "Ag==",
      "Aw==",
    ]);
    // The next request starts at key 4: no row is skipped or repeated.
    expect(answer.body.resumeFrom).toBe("BA==");
  });
});
