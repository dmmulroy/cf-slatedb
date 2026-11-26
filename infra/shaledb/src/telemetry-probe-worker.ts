import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  bindLsmWorkerSettings,
  eventTelemetryFlushForStage,
  inLsmJurisdiction,
  isLocalLsmStage,
  lsmJurisdiction,
  lsmAccessGateFrom,
  lsmWorkerCompatibility,
  ObjectStore,
  objectKindOfKey,
  objectStoreLayer,
  withLsmTracePolicy,
} from "@shaledb/lsm-cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Effect, Option, Schema } from "effect";
import { HttpClientRequest, HttpServerRequest, HttpServerResponse } from "effect/http";

import { shaledbAccessTokens } from "./shaledb-access-tokens.ts";
import { shaledbWorkerSettings } from "./shaledb-stack-config.ts";
import { shaledbTelemetry } from "./shaledb-telemetry.ts";

interface TelemetryProbeServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
  readonly alarm: () => Effect.Effect<void, never, RuntimeContext>;
}

/**
 * Durable Object that proves span export from Durable Object fetch and alarm events under
 * `alchemy dev`: `GET /v1/probe/alarm` schedules an alarm whose event emits its own span.
 */
export class TelemetryProbeServer extends Cloudflare.DurableObject<
  TelemetryProbeServer,
  TelemetryProbeServerContract
>()("TelemetryProbeServer") {}

const telemetryProbeServerLayer = TelemetryProbeServer.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return Effect.succeed({
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url.endsWith("/alarm")) yield* state.storage.setAlarm(Date.now() + 50);
        return HttpServerResponse.text("probe-ok");
      }).pipe(Effect.withSpan("TelemetryProbeServer.fetch"), withEventTelemetryFlush),
      alarm: () =>
        Effect.void.pipe(Effect.withSpan("TelemetryProbeServer.alarm"), withEventTelemetryFlush),
    });
  }),
);

interface R2ProbeServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
}

/**
 * Durable Object that measures R2 latency from where it runs: placed with a location hint by
 * `GET /v1/probe/r2`, it times the engine's own object store calls (create-if-absent, read, write,
 * versioned read) against the LSM bucket under `probe/r2/`, deletes what it wrote, and reports its
 * data center. Local and preview stages only; it never touches a database's keys (D56).
 */
export class R2ProbeServer extends Cloudflare.DurableObject<R2ProbeServer, R2ProbeServerContract>()(
  "R2ProbeServer",
) {}

const R2_PROBE_MAX_SAMPLES = 50;

const R2ProbeQuery = Schema.Struct({
  samples: Schema.optionalKey(Schema.NumberFromString),
});
const decodeR2ProbeQuery = Schema.decodeUnknownEffect(R2ProbeQuery);

/** The data center this code runs in, from Cloudflare's trace endpoint (`colo=IAD`). */
const currentColo = Effect.tryPromise(() =>
  fetch("https://www.cloudflare.com/cdn-cgi/trace").then((response) => response.text()),
).pipe(
  Effect.map((text) => Option.fromUndefinedOr(/^colo=(\w+)$/m.exec(text)?.[1])),
  Effect.orElseSucceed(() => Option.none<string>()),
);

/** Query of `POST /v1/probe/damage`: the object to damage and the byte to invert. */
const DamageQuery = Schema.Struct({
  key: Schema.String.check(Schema.isStartingWith("db/")),
  offset: Schema.NumberFromString.pipe(
    Schema.decodeTo(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  ),
});
const decodeDamageQuery = Schema.decodeUnknownOption(DamageQuery);

/** Inverts one byte of a stored object (`POST /damage`), for scenarios that corrupt an SST (D62). */
const damageObject = (objects: ObjectStore["Service"], url: URL) =>
  Effect.gen(function* () {
    const query = decodeDamageQuery(Object.fromEntries(url.searchParams));
    if (Option.isNone(query)) {
      return HttpServerResponse.text("key must start with db/ and offset be a byte index", {
        status: 400,
      });
    }
    const stored = yield* objects.get(query.value.key);
    if (Option.isNone(stored) || query.value.offset >= stored.value.length) {
      return HttpServerResponse.text("no such object, or offset past its end", { status: 404 });
    }
    const damaged = Uint8Array.from(stored.value);
    damaged[query.value.offset] = (damaged[query.value.offset] ?? 0) ^ 0xff;
    yield* objects.put(query.value.key, damaged);
    return HttpServerResponse.text("damaged");
  });

/** Query of `GET /v1/probe/storage`: a database's prefix and the age that counts as old. */
const StorageQuery = Schema.Struct({
  prefix: Schema.String.check(Schema.isStartingWith("db/")),
  olderThanMs: Schema.NumberFromString.pipe(
    Schema.decodeTo(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  ),
});
const decodeStorageQuery = Schema.decodeUnknownOption(StorageQuery);

/**
 * What is stored under a prefix (`GET /storage`), by object kind: how many objects and bytes, and
 * how many of them were uploaded more than `olderThanMs` ago. The soak uses it to check that GC
 * keeps storage bounded (D65).
 */
const describeStorage = (objects: ObjectStore["Service"], url: URL) =>
  Effect.gen(function* () {
    const query = decodeStorageQuery(Object.fromEntries(url.searchParams));
    if (Option.isNone(query)) {
      return HttpServerResponse.text("prefix must start with db/ and olderThanMs be a duration", {
        status: 400,
      });
    }
    const now = yield* Clock.currentTimeMillis;
    const kinds: Record<
      string,
      { objects: number; bytes: number; older: number; olderBytes: number }
    > = {};
    let cursor = Option.none<string>();
    do {
      const page = yield* objects.list(query.value.prefix, cursor);
      for (const object of page.objects) {
        const kind = objectKindOfKey(object.key);
        const entry = (kinds[kind] ??= { objects: 0, bytes: 0, older: 0, olderBytes: 0 });
        entry.objects += 1;
        entry.bytes += object.sizeBytes;
        if (now - object.uploadedAt > query.value.olderThanMs) {
          entry.older += 1;
          entry.olderBytes += object.sizeBytes;
        }
      }
      cursor = page.cursor;
    } while (Option.isSome(cursor));
    return yield* HttpServerResponse.json({ prefix: query.value.prefix, kinds });
  });

const r2ProbeServerLayer = R2ProbeServer.make(
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return Effect.succeed({
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = new URL(request.url, "http://probe");
        if (url.pathname.endsWith("/damage")) return yield* damageObject(objects, url);
        if (url.pathname.endsWith("/storage")) return yield* describeStorage(objects, url);
        const query = yield* decodeR2ProbeQuery(
          Object.fromEntries(new URL(request.url, "http://probe").searchParams),
        ).pipe(Effect.option);
        const requested = Option.flatMap(query, (parsed) => Option.fromUndefinedOr(parsed.samples));
        const samples = Math.min(
          R2_PROBE_MAX_SAMPLES,
          Math.max(
            1,
            Option.getOrElse(requested, () => 10),
          ),
        );
        const run = crypto.randomUUID();
        const body = new Uint8Array(128).fill(1);
        const timings: Record<"putIfAbsent" | "get" | "put" | "getVersioned", Array<number>> = {
          putIfAbsent: [],
          get: [],
          put: [],
          getVersioned: [],
        };
        const timed = <A, E, R>(into: Array<number>, effect: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const started = yield* Clock.currentTimeMillis;
            const value = yield* effect;
            into.push((yield* Clock.currentTimeMillis) - started);
            return value;
          });
        const keys: Array<string> = [`probe/r2/${run}/latest`];
        for (let sample = 0; sample < samples; sample++) {
          const key = `probe/r2/${run}/${sample}`;
          keys.push(key);
          yield* timed(timings.putIfAbsent, objects.putIfAbsent(key, body));
          yield* timed(timings.get, objects.get(key));
          yield* timed(timings.put, objects.put(`probe/r2/${run}/latest`, body));
          yield* timed(timings.getVersioned, objects.getVersioned(`probe/r2/${run}/latest`));
        }
        yield* objects.delete(keys);
        const colo = yield* currentColo;
        return yield* HttpServerResponse.json({
          colo: Option.getOrNull(colo),
          samples,
          timingsMs: timings,
        });
      }).pipe(
        Effect.catchCause(() =>
          Effect.succeed(HttpServerResponse.text("r2-probe-failed", { status: 500 })),
        ),
        Effect.withSpan("R2ProbeServer.fetch"),
        withEventTelemetryFlush,
      ),
    });
  }),
);

/**
 * Whether a stage deploys the probe Worker, a test tool: local and preview stages only. Production
 * (and any other live stage) never has it, so its fault injection (`POST /v1/probe/damage`) cannot
 * reach production data (D62).
 */
export const deploysTelemetryProbe = (stage: string): boolean =>
  isLocalLsmStage(stage) || /^preview-\d+$/.test(stage);

/** Location hints a Durable Object can be created with. */
const R2ProbeRegion = Schema.Literals([
  "wnam",
  "enam",
  "sam",
  "weur",
  "eeur",
  "apac",
  "oc",
  "afr",
  "me",
]);
const decodeR2ProbeRegion = Schema.decodeUnknownOption(R2ProbeRegion);

/**
 * Worker serving `GET /v1/probe` and `GET /v1/probe/alarm`, each a traced HTTP call to
 * {@link TelemetryProbeServer}, so the trace CLI can check Worker → Durable Object propagation, and
 * `GET /v1/probe/r2?region=<hint>&samples=<n>`, which measures R2 latency from a
 * {@link R2ProbeServer} created in that region, and `POST /v1/probe/damage?key=<db/...>&offset=<n>`,
 * which inverts one byte of a stored object, so a scenario can corrupt an SST and exercise the
 * repair (D62). Deployed on local and preview stages only ({@link deploysTelemetryProbe}).
 */
export class TelemetryProbeWorker extends Cloudflare.Worker<
  TelemetryProbeWorker,
  { readonly fetch: HttpEffect<RuntimeContext> },
  TelemetryProbeServer | R2ProbeServer
>()("TelemetryProbeWorker") {}

/** Worker Layer for {@link TelemetryProbeWorker}. */
export const telemetryProbeWorkerLayer = TelemetryProbeWorker.make(
  Effect.gen(function* () {
    return {
      main: import.meta.url,
      compatibility: yield* lsmWorkerCompatibility,
      dev: { host: "127.0.0.1", port: 8790 },
    };
  }),
  Effect.gen(function* () {
    const namespace = yield* TelemetryProbeServer;
    const r2Probes = yield* R2ProbeServer;
    const jurisdiction = yield* lsmJurisdiction;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const withAccessGate = yield* lsmAccessGateFrom(shaledbAccessTokens);
    // The probe addresses its Durable Object in the Stack's jurisdiction, like the engine Workers.
    yield* bindLsmWorkerSettings(shaledbWorkerSettings);
    return {
      fetch: withAccessGate(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const url = new URL(request.url, "http://probe");
          if (url.pathname.endsWith("/storage")) {
            const storage = Cloudflare.toHttpClient(
              inLsmJurisdiction(r2Probes, jurisdiction).getByName("damage"),
            );
            const described = yield* storage.execute(
              HttpClientRequest.get(`http://probe/storage${url.search}`),
            );
            return HttpServerResponse.text(yield* described.text, {
              status: described.status,
              contentType: "application/json",
            });
          }
          if (url.pathname.endsWith("/damage") && request.method === "POST") {
            // The object store needs a Durable Object's runtime context; any R2 probe object will do.
            const damager = Cloudflare.toHttpClient(
              inLsmJurisdiction(r2Probes, jurisdiction).getByName("damage"),
            );
            const damaged = yield* damager.execute(
              HttpClientRequest.post(`http://probe/damage${url.search}`),
            );
            return HttpServerResponse.text(yield* damaged.text, { status: damaged.status });
          }
          if (url.pathname.endsWith("/r2")) {
            const region = decodeR2ProbeRegion(url.searchParams.get("region"));
            if (Option.isNone(region)) {
              return HttpServerResponse.text("region must be a Durable Object location hint", {
                status: 400,
              });
            }
            // A new object per run, so the hint places it (a hint only applies at creation).
            const probe = Cloudflare.toHttpClient(
              inLsmJurisdiction(r2Probes, jurisdiction).getByName(
                `r2-${region.value}-${crypto.randomUUID()}`,
                { locationHint: region.value },
              ),
            );
            const measured = yield* probe.execute(
              HttpClientRequest.get(`http://probe/r2${url.search}`),
            );
            return HttpServerResponse.text(yield* measured.text, {
              status: measured.status,
              contentType: "application/json",
            });
          }
          const client = Cloudflare.toHttpClient(
            inLsmJurisdiction(namespace, jurisdiction).getByName("probe"),
          );
          const path = request.url.endsWith("/alarm") ? "/alarm" : "/";
          const response = yield* client.execute(HttpClientRequest.get(`http://probe${path}`));
          return HttpServerResponse.text(`worker-saw:${yield* response.text}`);
        }).pipe(
          Effect.catchCause(() =>
            Effect.succeed(HttpServerResponse.text("probe-failed", { status: 500 })),
          ),
        ),
      ).pipe(withEventTelemetryFlush),
    };
  }).pipe(
    Effect.provide(telemetryProbeServerLayer),
    Effect.provide(r2ProbeServerLayer),
    Effect.provide(objectStoreLayer),
    Effect.provide(withLsmTracePolicy(shaledbTelemetry("shaledb-telemetry-probe"))),
  ),
);

export default telemetryProbeWorkerLayer;
