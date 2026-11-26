import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  commitL0Flush,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  SeqNum,
  SstId,
  Timestamp,
  WriterEpoch,
} from "@shaledb/lsm-core";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import { workerBackgroundWorkLayer } from "../../src/background-work.ts";
import { eventTelemetryFlushForStage } from "../../src/lsm-telemetry.ts";
import { ManifestReader } from "../../src/manifest-reader.ts";
import { ManifestStore, manifestStoreLayer } from "../../src/manifest-store.ts";
import { ObjectStore } from "../../src/object-store.ts";

/** Port of the object store probe Worker under local workerd. */
export const OBJECT_STORE_PROBE_PORT = 8792;

/**
 * Test-only Worker that exercises the R2-backed `ObjectStore` and `ManifestStore` against
 * Alchemy's local R2: `/put-if-absent/<key>` twice, and `/manifest-race/<db>/<n>` (n concurrent
 * flush commits against one database).
 */
export class ObjectStoreProbeWorker extends Cloudflare.Worker<
  ObjectStoreProbeWorker,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("ObjectStoreProbeWorker") {}

const handle = (n: number) => ({
  id: SstId.make(`01JD${String(n).padStart(22, "0")}`),
  firstKey: Uint8Array.of(n),
  lastKey: Uint8Array.of(n),
  sizeBytes: 100,
  rowCount: 1,
  metaOffset: 50,
  metaLength: 50,
  owner: Option.none(),
  view: Option.none(),
});

/** Worker Layer for {@link ObjectStoreProbeWorker}. */
export const objectStoreProbeWorkerLayer = ObjectStoreProbeWorker.make(
  {
    main: import.meta.url,
    compatibility: { date: "2026-10-15" },
    dev: { host: "127.0.0.1", port: OBJECT_STORE_PROBE_PORT, strictPort: true },
  },
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const store = yield* ManifestStore;
    const reader = yield* ManifestReader;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const [, action, first, second] = new URL(request.url, "http://probe").pathname.split("/");
        if (action === "put-if-absent" && first !== undefined) {
          const outcomes = [
            yield* objects.putIfAbsent(first, Uint8Array.of(1)),
            yield* objects.putIfAbsent(first, Uint8Array.of(2)),
          ];
          const stored = yield* objects.get(first);
          return yield* HttpServerResponse.json({
            outcomes,
            stored: Option.getOrNull(Option.map(stored, (bytes) => [...bytes])),
          });
        }
        if (action === "manifest-race" && first !== undefined && second !== undefined) {
          const databaseId = DatabaseId.make(first);
          yield* store.initialize(
            initialManifest(databaseId, {
              keyRange: Option.none(),
              databaseOptions: DEFAULT_DATABASE_OPTIONS,
            }),
          );
          const count = Number(second);
          yield* Effect.forEach(
            Array.from({ length: count }, (_, index) => index + 1),
            (n) =>
              store.update(databaseId, (manifest) =>
                commitL0Flush(manifest, {
                  writerEpoch: WriterEpoch.make(0),
                  sst: handle(n),
                  lastSeq: SeqNum.make(n),
                  clockTick: Timestamp.make(n),
                  changelog: Option.none(),
                }),
              ),
            { concurrency: "unbounded" },
          );
          const latest = yield* reader.loadLatest(databaseId);
          return yield* HttpServerResponse.json({ id: latest.id, l0: latest.manifest.l0.length });
        }
        return HttpServerResponse.text("not found", { status: 404 });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.succeed(HttpServerResponse.text(String(cause), { status: 500 })),
        ),
        withEventTelemetryFlush,
      ),
    };
  }).pipe(Effect.provide(manifestStoreLayer.pipe(Layer.provide(workerBackgroundWorkLayer)))),
);

/** Stack body for the object store workerd test. */
export const objectStoreProbeStack = Effect.gen(function* () {
  const worker = yield* ObjectStoreProbeWorker;
  return { url: worker.url };
}).pipe(Effect.provide(objectStoreProbeWorkerLayer));

export default objectStoreProbeWorkerLayer;
