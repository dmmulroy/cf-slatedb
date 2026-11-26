import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  ManifestId,
  SstId,
} from "@shaledb/lsm-core";
import { Effect, Layer, Option } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { blockPath, metaPath } from "./block-server-http-api.ts";
import {
  blockCachePurgeTestLayer,
  blockServerHttpRouterLayer,
  IMMUTABLE_CACHE_CONTROL,
  LATEST_HINT_CACHE_CONTROL,
} from "./block-server-http-handlers.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import { objectReaderFromObjectStoreLayer } from "./object-reader.ts";
import { ObjectStore } from "./object-store.ts";

const DB = DatabaseId.make("orders");
const SST = SstId.make("01JD0000000000000000000AAA");

/** Calls the `BlockServer` router in-process and returns status, headers, and body bytes. */
const call = (method: "GET" | "POST", path: string) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(
      blockServerHttpRouterLayer.pipe(
        Layer.provide([objectReaderFromObjectStoreLayer, blockCachePurgeTestLayer]),
      ),
    );
    const response = yield* app.pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(new Request(`http://blocks.internal${path}`, { method })),
      ),
    );
    const web = HttpServerResponse.toWeb(response);
    return {
      status: web.status,
      cacheControl: web.headers.get("cache-control"),
      cacheTag: web.headers.get("cache-tag"),
      body: new Uint8Array(yield* Effect.promise(() => web.arrayBuffer())),
    };
  });

describe("BlockServer", () => {
  it("serves blocks, metadata, and manifests as immutable, tagged full responses, and 404s missing objects", async () => {
    const outcome = await Effect.gen(function* () {
      const objects = yield* ObjectStore;
      yield* objects.put(
        sstKey(DB, SST),
        Uint8Array.from({ length: 100 }, (_, index) => index),
      );
      const store = yield* ManifestStore;
      yield* store.initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      return {
        block: yield* call("GET", blockPath(DB, SST, 10, 5)),
        meta: yield* call("GET", metaPath(DB, SST, 90, 10)),
        manifest: yield* call("GET", `/v1/databases/${DB}/manifests/${ManifestId.make(1)}`),
        latest: yield* call("GET", `/v1/databases/${DB}/manifests/latest`),
        pastEnd: yield* call("GET", blockPath(DB, SST, 95, 10)),
        missing: yield* call("GET", blockPath(DB, SstId.make("01JD0000000000000000000BBB"), 0, 4)),
        purge: yield* call("POST", `/v1/databases/${DB}/purge`),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(outcome.block).toMatchObject({
      status: 200,
      cacheControl: IMMUTABLE_CACHE_CONTROL,
      cacheTag: "db:orders",
    });
    expect([...outcome.block.body]).toEqual([10, 11, 12, 13, 14]);
    expect([outcome.meta.status, outcome.meta.body.length]).toEqual([200, 10]);
    expect([outcome.manifest.status, outcome.manifest.cacheControl]).toEqual([
      200,
      IMMUTABLE_CACHE_CONTROL,
    ]);
    expect([outcome.latest.status, outcome.latest.cacheControl]).toEqual([
      200,
      LATEST_HINT_CACHE_CONTROL,
    ]);
    expect(JSON.parse(new TextDecoder().decode(outcome.latest.body))).toEqual({ manifestId: 1 });
    // A range past the end is not served as a short (cacheable) body.
    expect(outcome.pastEnd.status).toBe(404);
    expect(outcome.missing.status).toBe(404);
    expect(outcome.purge.status).toBe(200);
  });
});
