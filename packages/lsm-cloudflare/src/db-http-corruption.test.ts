import { DatabaseId, DEFAULT_DATABASE_OPTIONS } from "@shaledb/lsm-core";
import { Effect, Option } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * A read of a corrupted SST answers the corruption runbook's StoredDataInvalid (D53, D128), not
 * InternalFailure. `Flushed` reads run in the API Worker without the writer, so the in-process
 * router serves them as the Worker does.
 */
const DB = DatabaseId.make("orders");

describe("a read that meets a corrupted SST (D128)", () => {
  it("answers StoredDataInvalid (500, not retryable) naming the database and the SST, for a get and a scan", async () => {
    const answers = await Effect.gen(function* () {
      yield* (yield* DatabaseLifecycle).create(DB, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      yield* (yield* WriterClient).write(DB, {
        operations: [
          { _tag: "Put", key: Uint8Array.of(1), value: Uint8Array.of(2), ttlMillis: Option.none() },
        ],
      });
      yield* (yield* WriterClient).flush(DB);
      const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
      const victim = manifest.l0[0];
      if (victim === undefined) return yield* Effect.die("expected one L0 SST");
      // Flip a byte inside the SST's first data block: its CRC32C no longer matches.
      const objects = yield* ObjectStore;
      const stored = Option.getOrThrow(yield* objects.get(sstKey(DB, victim.id)));
      const damaged = Uint8Array.from(stored);
      damaged[2] = (damaged[2] ?? 0) ^ 0xff;
      yield* objects.put(sstKey(DB, victim.id), damaged);
      const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
      const post = (path: string, body: unknown) =>
        serveLsmTestRequest(
          app,
          new Request(`http://api.internal/v1/databases/${DB}/${path}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        ).pipe(Effect.map((web) => ({ status: web.status, body: JSON.parse(web.text) })));
      return {
        victim: victim.id,
        get: yield* post("get", { key: "AQ==", consistency: { _tag: "Flushed" }, snapshot: null }),
        scan: yield* post("scan", {
          range: { start: "", end: null },
          consistency: { _tag: "Flushed" },
          snapshot: null,
          limit: 10,
        }),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    for (const answer of [answers.get, answers.scan]) {
      expect(answer.status).toBe(500);
      expect(answer.body).toMatchObject({
        code: "StoredDataInvalid",
        details: { databaseId: DB },
        retryable: false,
      });
      expect(answer.body.message).toContain(answers.victim);
    }
  });
});
