import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  RowValue,
  SeqNum,
  type Row,
} from "@shaledb/lsm-core";
import { Effect, Layer, Option, Stream } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { DbClient } from "./db-client.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { ReaderUnavailableError } from "./db-reader.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";

const DB = DatabaseId.make("orders");

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

const overtakenPage = (limit: number) =>
  Effect.gen(function* () {
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
          limit,
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

describe("the public scan of a scan overtaken after it returned rows (D93)", () => {
  it("answers a page that filled its limit before it ended early with its rows and resumeFrom (D130)", async () => {
    // Three rows, exactly the limit, then the early end: a full page, not a failure.
    const answer = await overtakenPage(3);
    expect(answer.status).toBe(200);
    expect(answer.body.rows.map((returned: { key: string }) => returned.key)).toEqual([
      "AQ==",
      "Ag==",
      "Aw==",
    ]);
    expect(answer.body.resumeFrom).toBe("BA==");
  });

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

describe("the public scan over a range of deletes (D108)", () => {
  it("answers bounded pages with resumeFrom, possibly empty, that reach the one live key", async () => {
    const pages = await Effect.gen(function* () {
      const lifecycle = yield* DatabaseLifecycle;
      const client = yield* DbClient;
      yield* lifecycle.create(DB, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      // 24,000 keys of 1 KiB values, then all but the last deleted: about 24 MiB of versions, more
      // than one page may examine (16 MiB), behind one visible row.
      const key = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);
      const total = 24_000;
      for (let start = 0; start < total; start += 1_000) {
        yield* client.write(DB, {
          operations: Array.from({ length: 1_000 }, (_, offset) => ({
            _tag: "Put" as const,
            key: key(start + offset),
            value: new Uint8Array(1024).fill(1),
            ttlMillis: Option.none(),
          })),
        });
      }
      yield* client.flush(DB);
      for (let start = 0; start < total - 1; start += 1_000) {
        yield* client.write(DB, {
          operations: Array.from({ length: Math.min(1_000, total - 1 - start) }, (_, offset) => ({
            _tag: "Delete" as const,
            key: key(start + offset),
          })),
        });
      }
      yield* client.flush(DB);
      const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
      const answers: Array<{ status: number; rows: Array<string>; resumeFrom: string | null }> = [];
      let start = "";
      for (let page = 0; page < 10; page++) {
        const web = yield* serveLsmTestRequest(
          app,
          new Request("http://api.internal/v1/databases/orders/scan", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              range: { start, end: null },
              consistency: { _tag: "Flushed" },
              snapshot: null,
              limit: 10,
            }),
          }),
        );
        const body = JSON.parse(web.text);
        answers.push({
          status: web.status,
          rows: body.rows?.map((row: { key: string }) => row.key) ?? [],
          resumeFrom: body.resumeFrom ?? null,
        });
        if (body.resumeFrom === null || body.resumeFrom === undefined) break;
        start = body.resumeFrom;
      }
      return { answers, last: Buffer.from(key(total - 1)).toString("base64") };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    // The first page stops at its share of the deletes, with no row and where to go on.
    expect(pages.answers[0]).toMatchObject({ status: 200, rows: [] });
    expect(pages.answers[0]?.resumeFrom).not.toBeNull();
    // Every page answers, and together they return exactly the one live key.
    expect(pages.answers.every((answer) => answer.status === 200)).toBe(true);
    expect(pages.answers.flatMap((answer) => answer.rows)).toEqual([pages.last]);
    expect(pages.answers.at(-1)?.resumeFrom).toBeNull();
  });
});
