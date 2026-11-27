import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { DbHttpApi } from "@shaledb/lsm-cloudflare";
import { DatabaseId } from "@shaledb/lsm-core";
import { Effect, Option } from "effect";
import { HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { ShardedHttpApi } from "../../src/shard-router-worker.ts";
import { ShardGroup } from "../../src/shard-map.ts";
import { useAnsweringCollector } from "./answering-collector.ts";
import { ROUTER_TEST_PORTS } from "./router-ports.ts";
import { routerTestStack } from "./router-stack.ts";

useAnsweringCollector(ROUTER_TEST_PORTS.collector);

const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "test-router",
});

const bytes = (text: string) => new TextEncoder().encode(text);
const decode = (key: Uint8Array) => new TextDecoder().decode(key);

/** The local Worker URL; a local stack always has one. */
const workerUrl = (url: string | undefined): string => {
  expect(url).toBeDefined();
  return url ?? "";
};

test.provider(
  "shard router Worker: a routed scan page ends at its work budget with resumeFrom, and a malformed request gets the public envelope (D79, D125)",
  (stack) =>
    Effect.gen(function* () {
      const deployed = yield* stack.deploy(routerTestStack);
      const httpClient = yield* HttpClient.HttpClient;
      const groups = yield* HttpApiClient.makeWith(ShardedHttpApi, {
        baseUrl: workerUrl(deployed.url),
        httpClient,
      });
      const databases = yield* HttpApiClient.makeWith(DbHttpApi, {
        baseUrl: workerUrl(deployed.apiUrl),
        httpClient,
      });
      const suffix = Date.now().toString(36);
      const group = ShardGroup.make(`bounds-${suffix}`);
      const databaseId = DatabaseId.make(`bounds-${suffix}-0`);
      // Creating the group creates its one shard's database (the whole key space).
      yield* groups.groups.create({ params: { group }, payload: { databaseId } });

      // 24,000 keys of 1 KiB values, written in batches straight to the shard's database, then all
      // but the last two deleted: 24 MiB of versions, past one routed page's 16 MiB budget.
      const keyOf = (index: number) => bytes(`k-${String(index).padStart(5, "0")}`);
      const value = new Uint8Array(1024).fill(1);
      for (let start = 0; start < 24_000; start += 1000) {
        yield* databases.databases.write({
          params: { databaseId },
          payload: {
            operations: Array.from({ length: 1000 }, (_, offset) => ({
              _tag: "Put" as const,
              key: keyOf(start + offset),
              value,
              ttlMillis: Option.none(),
            })),
          },
        });
      }
      for (let start = 0; start < 23_998; start += 1000) {
        yield* databases.databases.write({
          params: { databaseId },
          payload: {
            operations: Array.from({ length: Math.min(1000, 23_998 - start) }, (_, offset) => ({
              _tag: "Delete" as const,
              key: keyOf(start + offset),
            })),
          },
        });
      }

      // The first routed page is empty with a continuation; following it reaches the live keys.
      const page = (start: Uint8Array) =>
        groups.groups.scan({
          params: { group },
          payload: { range: { start, end: Option.none() }, limit: 1 },
        });
      const first = yield* page(new Uint8Array(0));
      expect(first.rows).toEqual([]);
      expect(Option.isSome(first.resumeFrom)).toBe(true);
      const found: Array<string> = [];
      let next = first.resumeFrom;
      for (let pages = 0; pages < 100 && Option.isSome(next); pages++) {
        const answer = yield* page(next.value);
        found.push(...answer.rows.map((row) => decode(row.key)));
        next = answer.resumeFrom;
      }
      expect(found).toEqual([decode(keyOf(23_998)), decode(keyOf(23_999))]);
      expect(Option.isNone(next)).toBe(true);

      // A malformed body is refused before any handler, in the public envelope (D79), by workerd's
      // own request and body handling.
      const path = `/v1/groups/${group}/put`;
      const malformed = yield* Effect.promise(() =>
        fetch(`${workerUrl(deployed.url)}${path}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: '{"key":1}',
        }),
      );
      const refused: unknown = yield* Effect.promise(() => malformed.json());
      expect(malformed.status).toBe(400);
      expect(refused).toMatchObject({
        _tag: "InvalidRequestPublicError",
        code: "InvalidRequest",
        operation: "routeRequest",
        details: { method: "POST", path },
        retryable: false,
      });
      expect(refused).toHaveProperty("requestId", expect.any(String));
      yield* stack.destroy();
    }),
  { timeout: 300_000 },
);
