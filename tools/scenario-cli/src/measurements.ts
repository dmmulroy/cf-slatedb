import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { DatabaseId } from "@shaledb/lsm-core";
import { DbHttpApi, GcHttpApi } from "@shaledb/lsm-cloudflare";
import { ShardedHttpApi, ShardGroup } from "@shaledb/sharding";
import { Clock, Context, Data, Effect, Option, Schedule, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { percentile, summarizeHealth, summarizeValues } from "./health-report.ts";

export { exceptionType, percentile } from "./health-report.ts";

import {
  pullStageSpansSinceStart,
  runTraceCliCaptured,
  runTraceCliAsync,
  ScenarioStack,
  uniqueId,
} from "./scenario.ts";

/**
 * A measurement could not run or its data could not be read: `step` names what it was doing and
 * `reason` the failure's tag (an API error, or `SchemaError` for data it could not read). Run the
 * measurement again; it creates its own database each time.
 */
export class MeasurementError extends Data.TaggedError("MeasurementError")<{
  readonly step: string;
  readonly reason: string;
  readonly message: string;
}> {}

/**
 * Where a live stage's tail records and spans can be read: a preview's from `preview-cli
 * collector`, or any live stage's from `--collector-url` and `SHALEDB_TRACE_TOKEN`.
 */
export interface IMeasurementCollector {
  /**
   * The preview stage, or the collector URL's host when it was named by URL; names the stage's
   * measurement store, so the two forms never share one.
   */
  readonly stage: string;
  readonly url: string;
  readonly token: string;
}

/** The preview stage's trace collector, for tail records (CPU time) and spans. */
export class MeasurementCollector extends Context.Service<
  MeasurementCollector,
  IMeasurementCollector
>()("@shaledb/scenario/MeasurementCollector") {}

const summarize = summarizeValues;

const dbApi = Effect.gen(function* () {
  return yield* HttpApiClient.makeWith(DbHttpApi, {
    baseUrl: (yield* ScenarioStack).api,
    httpClient: yield* HttpClient.HttpClient,
  });
});

const shardsApi = Effect.gen(function* () {
  return yield* HttpApiClient.makeWith(ShardedHttpApi, {
    baseUrl: (yield* ScenarioStack).shards,
    httpClient: yield* HttpClient.HttpClient,
  });
});

const failed =
  (step: string) =>
  (error: { readonly _tag: string; readonly message?: string }): MeasurementError =>
    new MeasurementError({
      step,
      reason: error._tag,
      message: `Measurement step "${step}" failed with ${error._tag}: ${error.message ?? "no detail"}. Run the measurement again.`,
    });

const createDatabase = (databaseId: DatabaseId) =>
  Effect.flatMap(dbApi, (client) =>
    client.databases
      .createDatabase({
        payload: {
          databaseId,
          keyRange: Option.none(),
          mergeOperator: Option.none(),
          changelogRetentionMs: Option.none(),
          budgetGroup: Option.none(),
        },
      })
      .pipe(Effect.mapError(failed("create the database"))),
  );

/** Deterministic, distinct bytes for a value of `size` bytes (not all zeros, not compressible). */
const valueBytes = (size: number, seed: number) => {
  const bytes = new Uint8Array(size);
  let state = (seed * 2654435761) >>> 0;
  for (let index = 0; index < size; index++) {
    state = (state * 1664525 + 1013904223) >>> 0;
    bytes[index] = state >>> 24;
  }
  return bytes;
};

const keyBytes = (worker: number, sequence: number, row: number) =>
  new TextEncoder().encode(
    `w${worker.toString().padStart(3, "0")}-${sequence.toString().padStart(8, "0")}-${row
      .toString()
      .padStart(5, "0")}`,
  );

/** Options of {@link measureWrites}. */
export interface WriteLoad {
  readonly batchRows: number;
  readonly valueBytes: number;
  readonly concurrency: number;
  readonly seconds: number;
}

/**
 * Sends batched writes to one new database from `concurrency` loops for `seconds`, and reports the
 * acknowledged rows per second, request latency as the client saw it, and failures by error tag.
 *
 * @returns The report (failed writes are counted, not raised), or `MeasurementError` when the
 *   database cannot be created or read back.
 */
export const measureWrites = (load: WriteLoad) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-writes"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const value = valueBytes(load.valueBytes, 7);
    const latencies: Array<number> = [];
    const failures = new Map<string, number>();
    let rows = 0;
    const started = yield* Clock.currentTimeMillis;
    const deadline = started + load.seconds * 1000;
    yield* Effect.forEach(
      Array.from({ length: load.concurrency }, (_, worker) => worker),
      (worker) =>
        Effect.gen(function* () {
          let sequence = 0;
          while ((yield* Clock.currentTimeMillis) < deadline) {
            sequence += 1;
            const operations = Array.from({ length: load.batchRows }, (_, row) => ({
              _tag: "Put" as const,
              key: keyBytes(worker, sequence, row),
              value,
              ttlMillis: Option.none(),
            }));
            const sent = yield* Clock.currentTimeMillis;
            const outcome = yield* Effect.result(
              client.databases.write({ params: { databaseId }, payload: { operations } }),
            );
            const took = (yield* Clock.currentTimeMillis) - sent;
            if (outcome._tag === "Success") {
              latencies.push(took);
              rows += load.batchRows;
            } else {
              const tag = outcome.failure._tag;
              failures.set(tag, (failures.get(tag) ?? 0) + 1);
              if (tag === "WriteStalledPublicError") yield* Effect.sleep("200 millis");
            }
          }
        }),
      { concurrency: "unbounded", discard: true },
    );
    const elapsedSeconds = ((yield* Clock.currentTimeMillis) - started) / 1000;
    const status = yield* client.databases
      .status({ params: { databaseId } })
      .pipe(Effect.mapError(failed("status after the load")));
    // CPU per batch from the Tail Worker: the API Worker's request and the writer's (D61).
    yield* Effect.sleep("15 seconds");
    const records = (yield* readInvocationRecords(started)).filter((record) =>
      Option.exists(record.path, (path) => path.includes(`/databases/${databaseId}/batches`)),
    );
    const writerRecords = records.filter((record) => Option.isSome(record.durableObjectId));
    const apiRecords = records.filter((record) => Option.isNone(record.durableObjectId));
    const writerCpuSeconds =
      writerRecords.reduce((sum, record) => sum + record.cpuTimeMs, 0) / 1000;
    return {
      databaseId,
      load,
      elapsedSeconds,
      acknowledgedRows: rows,
      rowsPerSecond: rows / elapsedSeconds,
      requestsPerSecond: latencies.length / elapsedSeconds,
      megabytesPerSecond: (rows * (load.valueBytes + 20)) / elapsedSeconds / 1_048_576,
      latencyMs: summarize(latencies),
      failures: Object.fromEntries(failures),
      after: { l0Count: status.l0Count, unflushedBytes: status.unflushedBytes },
      writerCpuMsPerBatch: summarize(writerRecords.map((record) => record.cpuTimeMs)),
      apiCpuMsPerBatch: summarize(apiRecords.map((record) => record.cpuTimeMs)),
      writerCpuSeconds,
      writerBusyFraction: writerCpuSeconds / elapsedSeconds,
    };
  });

/** Schema of the Tail Worker's invocation records (`infra/shaledb/src/cpu-tail-worker.ts`). */
const InvocationRecord = Schema.Struct({
  scriptName: Schema.OptionFromNullOr(Schema.String),
  durableObjectId: Schema.OptionFromNullOr(Schema.String),
  eventKind: Schema.String,
  method: Schema.OptionFromNullOr(Schema.String),
  path: Schema.OptionFromNullOr(Schema.String),
  status: Schema.OptionFromNullOr(Schema.Number),
  outcome: Schema.String,
  eventTimestamp: Schema.OptionFromNullOr(Schema.Number),
  cpuTimeMs: Schema.Number,
  wallTimeMs: Schema.Number,
  // Absent in records from before D71, which were all kept.
  sampleRate: Schema.optional(Schema.Number),
});
export type InvocationRecord = typeof InvocationRecord.Type;

/**
 * How many invocations a kept record stands for: the Tail Worker keeps every invocation that did
 * not end `ok` and a sampled fraction of the rest (D71).
 */
export const invocationsRepresentedBy = (record: InvocationRecord): number => {
  const rate = record.sampleRate ?? 1;
  return rate > 0 ? 1 / rate : 1;
};

const TailPage = Schema.Struct({
  batches: Schema.Array(Schema.Struct({ key: Schema.String, body: Schema.String })),
  next: Schema.NullOr(Schema.String),
  more: Schema.Boolean,
});
const decodeTailPage = Schema.decodeUnknownEffect(TailPage);
const decodeRecords = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(InvocationRecord)),
);

/**
 * Reads every invocation record the stage's Tail Worker stored after `sinceMs`.
 *
 * @returns The records, or `MeasurementError` when the collector cannot be read or a stored record
 *   does not decode.
 */
/** Where a tail-record listing starts so that it covers `sinceMs` (keys start with arrival time). */
export const tailCursorAt = (sinceMs: number) =>
  `tails/${(sinceMs - 1).toString().padStart(15, "0")}`;

/**
 * Reads the Tail Worker's invocation records after `after` (a tail key, see {@link tailCursorAt}),
 * a page at a time, passing each page's records and the key to continue from after it to
 * `onPage`. Each page is retried a few times, so one failed request does not lose a long read.
 * `onPage` runs synchronously once per page, so a read interrupted between pages has handed over
 * every record before the last key it handed over, and none after it.
 *
 * @returns The key to continue from next time, or `MeasurementError`.
 */
export const forEachInvocationRecord = (
  after: string,
  onPage: (records: ReadonlyArray<InvocationRecord>, cursor: string) => void,
) =>
  Effect.gen(function* () {
    const collector = yield* MeasurementCollector;
    const client = yield* HttpClient.HttpClient;
    let cursor = after;
    while (true) {
      const url = new URL("/v1/tails/batches", collector.url);
      url.searchParams.set("limit", "500");
      url.searchParams.set("after", cursor);
      const page = yield* client
        .execute(
          HttpClientRequest.get(url.toString()).pipe(
            HttpClientRequest.bearerToken(collector.token),
          ),
        )
        .pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(decodeTailPage),
          Effect.timeout("60 seconds"),
          Effect.retry({ times: 4, schedule: Schedule.exponential("500 millis") }),
          Effect.mapError(failed("read tail records")),
        );
      const records: Array<InvocationRecord> = [];
      for (const batch of page.batches) {
        const body = Base64.decode(batch.body);
        if (body._tag === "Failure") continue;
        records.push(
          ...(yield* decodeRecords(new TextDecoder().decode(body.success)).pipe(
            Effect.mapError(failed("decode tail records")),
          )),
        );
      }
      if (page.next !== null) cursor = page.next;
      onPage(records, cursor);
      if (!page.more || page.batches.length === 0) break;
    }
    return cursor;
    // Its own client: the stage's HTTP client carries the access token, not the collector's.
  }).pipe(Effect.provide(FetchHttpClient.layer));

/** Every invocation record since `sinceMs` (see {@link forEachInvocationRecord}). */
export const readInvocationRecords = (sinceMs: number) =>
  Effect.gen(function* () {
    const records: Array<InvocationRecord> = [];
    yield* forEachInvocationRecord(tailCursorAt(sinceMs), (page) => {
      records.push(...page);
    });
    return records;
  });

const TRACE_CLI = fileURLToPath(new URL("../../trace-cli/src/trace-cli.ts", import.meta.url));

const FlushSpans = Schema.Array(
  Schema.Struct({
    durationMs: Schema.Number,
    attributes: Schema.Record(Schema.String, Schema.Unknown),
  }),
);
const decodeFlushSpans = Schema.decodeUnknownEffect(Schema.fromJsonString(FlushSpans));

/** Pulls the stage's spans, then returns the spans named `name` for one database. */
const pulledSpans = (name: string, databaseId: string) =>
  Effect.gen(function* () {
    const { stage } = yield* MeasurementCollector;
    yield* pullStageSpansSinceStart(stage);
    const result = yield* Effect.sync(() =>
      spawnSync(
        process.execPath,
        [TRACE_CLI, "grep", "--name", name, "--db", databaseId, "--json"],
        {
          encoding: "utf8",
          maxBuffer: 64 * 1024 * 1024,
        },
      ),
    );
    return yield* decodeFlushSpans(result.stdout).pipe(
      Effect.mapError(failed(`read ${name} spans`)),
    );
  });

/**
 * Writes `megabytes` of rows to a new database, flushes it, and reports the CPU time of the writer
 * invocations that flushed (the `/flush` request and any alarm), per MiB of SST written.
 *
 * @returns The report, or `MeasurementError` when a write, the flush, or reading tail records or
 *   spans fails.
 */
export const measureFlushCpu = (megabytes: number, rowValueBytes = 1000) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-flush"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    // About 2 MB of values per batch whatever the value size, so the same bytes are written as
    // many small rows or few large ones: flush CPU that follows the row count is per-row work.
    // (At most 10,000 rows, a write batch's limit, so small values go in smaller batches.)
    const batchRows = Math.min(10_000, Math.max(1, Math.round(2_000_000 / rowValueBytes)));
    const batches = Math.ceil((megabytes * 1_048_576) / (batchRows * rowValueBytes));
    const value = valueBytes(rowValueBytes, 11);
    const since = yield* Clock.currentTimeMillis;
    yield* Effect.forEach(
      Array.from({ length: batches }, (_, index) => index),
      (sequence) =>
        client.databases
          .write({
            params: { databaseId },
            payload: {
              operations: Array.from({ length: batchRows }, (_, row) => ({
                _tag: "Put" as const,
                key: keyBytes(0, sequence, row),
                value,
                ttlMillis: Option.none(),
              })),
            },
          })
          .pipe(Effect.mapError(failed("write"))),
      { concurrency: 4, discard: true },
    );
    const before = yield* client.databases
      .status({ params: { databaseId } })
      .pipe(Effect.mapError(failed("status before the flush")));
    yield* client.databases
      .flush({ params: { databaseId } })
      .pipe(Effect.mapError(failed("flush")));
    // Tail events arrive after each invocation finishes; spans after each response.
    yield* Effect.sleep("15 seconds");
    const records = yield* readInvocationRecords(since);
    const writerWrites = records.filter(
      (record) =>
        Option.isSome(record.durableObjectId) &&
        Option.exists(record.path, (path) => path.includes(`/databases/${databaseId}/batches`)),
    );
    const writer = Option.flatMap(
      Option.fromUndefinedOr(writerWrites[0]),
      (record) => record.durableObjectId,
    );
    const flushing = records.filter(
      (record) =>
        Option.isSome(writer) &&
        Option.contains(record.durableObjectId, writer.value) &&
        (record.eventKind === "alarm" ||
          Option.exists(record.path, (path) => path.endsWith(`/databases/${databaseId}/flush`))),
    );
    const flushes = yield* pulledSpans("DbWriter.flush", databaseId);
    const sstBytes = flushes.reduce(
      (total, span) =>
        total + (typeof span.attributes["sst.size"] === "number" ? span.attributes["sst.size"] : 0),
      0,
    );
    const cpuMs = flushing.reduce((total, record) => total + record.cpuTimeMs, 0);
    const wallMs = flushing.reduce((total, record) => total + record.wallTimeMs, 0);
    return {
      databaseId,
      megabytesWritten: megabytes,
      valueBytes: rowValueBytes,
      rowsWritten: batches * batchRows,
      unflushedBytesBeforeFlush: before.unflushedBytes,
      flushes: flushes.length,
      sstMiB: sstBytes / 1_048_576,
      flushInvocations: flushing.map((record) => ({
        kind: record.eventKind,
        cpuTimeMs: record.cpuTimeMs,
        wallTimeMs: record.wallTimeMs,
        outcome: record.outcome,
      })),
      cpuMs,
      wallMs,
      cpuMsPerMiB: sstBytes === 0 ? 0 : cpuMs / (sstBytes / 1_048_576),
      cpuMicrosPerRow: (cpuMs * 1000) / (batches * batchRows),
      writeBatches: {
        rowsPerBatch: batchRows,
        writerCpuMs: summarize(writerWrites.map((record) => record.cpuTimeMs)),
        writerWallMs: summarize(writerWrites.map((record) => record.wallTimeMs)),
      },
    };
  });

/** Options of {@link measureManifestCas}. */
export interface ManifestCasLoad {
  /** Small flushes to run, one after another. */
  readonly flushes: number;
  /** Rows written before each flush (100-byte values). */
  readonly rowsPerFlush: number;
}

/**
 * Runs `flushes` small flushes on a new database, one at a time (a small write, then `POST
 * /flush`), and reports how long each manifest CAS and each flush took on the server
 * (`ManifestStore.update` by how it started: from the writer's own view or a loaded manifest;
 * `DbWriter.flush`), and the flush request as the client saw it.
 *
 * @returns The report, or `MeasurementError` when a write, a flush, or reading spans fails.
 */
export const measureManifestCas = (load: ManifestCasLoad) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-cas"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const value = valueBytes(100, 7);
    const flushClientMs: Array<number> = [];
    for (let sequence = 0; sequence < load.flushes; sequence++) {
      yield* client.databases
        .write({
          params: { databaseId },
          payload: {
            operations: Array.from({ length: load.rowsPerFlush }, (_, row) => ({
              _tag: "Put" as const,
              key: keyBytes(0, sequence, row),
              value,
              ttlMillis: Option.none(),
            })),
          },
        })
        .pipe(Effect.mapError(failed("write")));
      const started = yield* Clock.currentTimeMillis;
      yield* client.databases
        .flush({ params: { databaseId } })
        .pipe(Effect.mapError(failed("flush")));
      flushClientMs.push((yield* Clock.currentTimeMillis) - started);
    }
    // Spans export after each response.
    yield* Effect.sleep("10 seconds");
    const updates = yield* pulledSpans("ManifestStore.update", databaseId);
    const flushes = yield* pulledSpans("DbWriter.flush", databaseId);
    const loads = yield* pulledSpans("ManifestReader.loadLatest", databaseId);
    const startedFrom = (start: string) =>
      updates
        .filter((span) => span.attributes["manifest.start"] === start)
        .map((span) => span.durationMs);
    return {
      databaseId,
      flushes: load.flushes,
      rowsPerFlush: load.rowsPerFlush,
      manifestUpdateMs: summarize(updates.map((span) => span.durationMs)),
      manifestUpdateFromOwnViewMs: summarize(startedFrom("OwnView")),
      manifestUpdateLoadedMs: summarize(startedFrom("Loaded")),
      casAttempts: summarize(
        updates.map((span) =>
          typeof span.attributes["manifest.cas_attempts"] === "number"
            ? span.attributes["manifest.cas_attempts"]
            : 0,
        ),
      ),
      loadLatestMs: summarize(loads.map((span) => span.durationMs)),
      writerFlushMs: summarize(flushes.map((span) => span.durationMs)),
      flushClientMs: summarize(flushClientMs),
    };
  });

/** Durable Object location hints the R2 placement probe measures from. */
const R2_PROBE_REGIONS = [
  "wnam",
  "enam",
  "sam",
  "weur",
  "eeur",
  "apac",
  "oc",
  "afr",
  "me",
] as const;

const R2ProbeReport = Schema.Struct({
  colo: Schema.NullOr(Schema.String),
  samples: Schema.Number,
  timingsMs: Schema.Struct({
    putIfAbsent: Schema.Array(Schema.Number),
    get: Schema.Array(Schema.Number),
    put: Schema.Array(Schema.Number),
    getVersioned: Schema.Array(Schema.Number),
  }),
});
const decodeR2ProbeReport = Schema.decodeUnknownEffect(Schema.fromJsonString(R2ProbeReport));

/**
 * Measures R2 latency to the stage's LSM bucket from a Durable Object created in each region (the
 * probe Worker's `GET /v1/probe/r2`): `samples` rounds of create-if-absent, read, write, and
 * versioned read per region, with the data center each object landed in.
 *
 * @returns Per region: the data center and each operation's latency percentiles, or
 *   `MeasurementError` when a probe fails.
 */
export const measureR2Placement = (samples: number) =>
  Effect.gen(function* () {
    const { probe } = yield* ScenarioStack;
    const http = yield* HttpClient.HttpClient;
    const regions = yield* Effect.forEach(R2_PROBE_REGIONS, (region) =>
      Effect.gen(function* () {
        const response = yield* http
          .execute(
            HttpClientRequest.get(
              `${probe.replace(/\/$/, "")}/v1/probe/r2?region=${region}&samples=${samples}`,
            ),
          )
          .pipe(Effect.mapError(failed(`probe ${region}`)));
        const report = yield* decodeR2ProbeReport(yield* response.text.pipe(Effect.orDie)).pipe(
          Effect.mapError(failed(`read the ${region} probe`)),
        );
        return {
          region,
          colo: report.colo,
          putIfAbsentMs: summarize(report.timingsMs.putIfAbsent),
          getMs: summarize(report.timingsMs.get),
          putMs: summarize(report.timingsMs.put),
          getVersionedMs: summarize(report.timingsMs.getVersioned),
        };
      }),
    );
    return { samples, regions };
  });

/** Options of {@link measureGcScale}. */
export interface GcScaleLoad {
  /** Databases to create, each with a compaction's worth of garbage. */
  readonly databases: number;
  /** Seconds to wait after the last compaction for collections (gcMinAge plus margin). */
  readonly settleSeconds: number;
}

const GcSpans = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    attributes: Schema.Struct({
      "database.id": Schema.optionalKey(Schema.String),
      "gc.deleted_ssts": Schema.optionalKey(Schema.Number),
    }),
  }),
);
const decodeGcSpans = Schema.decodeUnknownEffect(Schema.fromJsonString(GcSpans));

/**
 * Creates `databases` databases, each flushed four times so its writer wakes its compactor and it
 * compacts (leaving the four L0 SSTs as garbage), waits for each compactor to collect on its own
 * (deploy with `--dev-timings`: `gcMinAge` 10 s), runs the maintenance sweep page by page until it
 * wraps, and reports how many databases were collected and every invocation's outcome (D60).
 *
 * @returns The report, or `MeasurementError` when a database cannot be created or written, the sweep
 *   fails, or tail records or spans cannot be read.
 */
export const measureGcScale = (load: GcScaleLoad) =>
  Effect.gen(function* () {
    const group = uniqueId("ms-gc");
    const databases = Array.from({ length: load.databases }, (_, index) =>
      DatabaseId.make(`${group}-${index.toString().padStart(4, "0")}`),
    );
    const client = yield* dbApi;
    const since = yield* Clock.currentTimeMillis;
    const value = valueBytes(100, 3);
    yield* Effect.forEach(
      databases,
      (databaseId) =>
        Effect.gen(function* () {
          yield* createDatabase(databaseId);
          for (let flush = 0; flush < 4; flush++) {
            yield* client.databases
              .write({
                params: { databaseId },
                payload: {
                  operations: [
                    {
                      _tag: "Put" as const,
                      key: keyBytes(0, flush, 0),
                      value,
                      ttlMillis: Option.none(),
                    },
                  ],
                },
              })
              .pipe(Effect.mapError(failed("write")));
            yield* client.databases
              .flush({ params: { databaseId } })
              .pipe(Effect.mapError(failed("flush")));
          }
        }),
      { concurrency: 16, discard: true },
    );
    const loadedMs = (yield* Clock.currentTimeMillis) - since;
    yield* Effect.sleep(`${load.settleSeconds} seconds`);
    const { gc } = yield* ScenarioStack;
    const gcClient = yield* HttpApiClient.makeWith(GcHttpApi, {
      baseUrl: gc,
      httpClient: yield* HttpClient.HttpClient,
    });
    const pages: Array<number> = [];
    for (let page = 0; page < 10_000; page++) {
      const swept = yield* gcClient.gc.sweep().pipe(Effect.mapError(failed("sweep")));
      pages.push(swept.visited);
      if (Option.isNone(swept.cursor)) break;
    }
    // Spans and tail records arrive after each invocation.
    yield* Effect.sleep("20 seconds");
    const { stage } = yield* MeasurementCollector;
    yield* pullStageSpansSinceStart(stage);
    const grepped = yield* Effect.sync(() =>
      spawnSync(
        process.execPath,
        [TRACE_CLI, "grep", "--name", "GarbageCollector.collect", "--json"],
        {
          encoding: "utf8",
          maxBuffer: 256 * 1024 * 1024,
        },
      ),
    );
    const collections = (yield* decodeGcSpans(grepped.stdout).pipe(
      Effect.mapError(failed("read GarbageCollector.collect spans")),
    )).filter((span) => (span.attributes["database.id"] ?? "").startsWith(group));
    const collected = new Set(
      collections
        .filter((span) => (span.attributes["gc.deleted_ssts"] ?? 0) >= 4)
        .map((span) => span.attributes["database.id"] ?? ""),
    );
    const records = yield* readInvocationRecords(since);
    const byScriptOutcome = new Map<string, number>();
    for (const record of records) {
      const script = Option.getOrElse(record.scriptName, () => "?").replace(/-preview-\d.*$/, "");
      const key = `${script} ${record.outcome}`;
      byScriptOutcome.set(key, (byScriptOutcome.get(key) ?? 0) + 1);
    }
    const sweeps = records.filter((record) =>
      Option.exists(record.scriptName, (name) => name.includes("gcworker")),
    );
    return {
      databases: load.databases,
      loadSeconds: loadedMs / 1000,
      sweepPages: pages,
      databasesCollected: collected.size,
      collectionRuns: collections.length,
      invocations: records.length,
      outcomes: Object.fromEntries(byScriptOutcome),
      notOk: records.filter((record) => record.outcome !== "ok").length,
      gcWorkerCpuMs: summarize(sweeps.map((record) => record.cpuTimeMs)),
      gcWorkerWallMs: summarize(sweeps.map((record) => record.wallTimeMs)),
      compactorCpuMs: summarize(
        records
          .filter((record) =>
            Option.exists(record.scriptName, (name) => name.includes("compactorworker")),
          )
          .map((record) => record.cpuTimeMs),
      ),
    };
  });

/**
 * Writes `megabytesPerFlush` of 1 KB rows and flushes it, four times, so the writer wakes the
 * compactor and four L0 SSTs of production size compact; reports the CPU of every flush and of
 * every compactor invocation (its alarms, and the compaction Workflow's steps), to size the Workers'
 * CPU limits (D61).
 *
 * @returns The report, or `MeasurementError` when a write, flush, or reading tail records fails.
 */
export const measureCompactionCpu = (megabytesPerFlush: number) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-compact"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const batchRows = 2000;
    const rowValueBytes = 1000;
    const batches = Math.ceil((megabytesPerFlush * 1_048_576) / (batchRows * rowValueBytes));
    const value = valueBytes(rowValueBytes, 13);
    const since = yield* Clock.currentTimeMillis;
    for (let flush = 0; flush < 4; flush++) {
      yield* Effect.forEach(
        Array.from({ length: batches }, (_, index) => index),
        (sequence) =>
          client.databases
            .write({
              params: { databaseId },
              payload: {
                operations: Array.from({ length: batchRows }, (_, row) => ({
                  _tag: "Put" as const,
                  key: keyBytes(flush, sequence, row),
                  value,
                  ttlMillis: Option.none(),
                })),
              },
            })
            .pipe(Effect.mapError(failed("write"))),
        { concurrency: 4, discard: true },
      );
      yield* client.databases
        .flush({ params: { databaseId } })
        .pipe(Effect.mapError(failed("flush")));
    }
    // Wait for the compaction to finish, polling the manifest's L0 (the writer's status shows its own,
    // possibly older, view of the manifest).
    let compacted = false;
    for (let attempt = 0; attempt < 120 && !compacted; attempt++) {
      yield* Effect.sleep("5 seconds");
      const compaction = yield* client.databases
        .compaction({ params: { databaseId } })
        .pipe(Effect.mapError(failed("compaction status")));
      compacted = compaction.l0Ssts === 0 && compaction.sortedRuns.length > 0;
    }
    const compactedAfterSeconds = ((yield* Clock.currentTimeMillis) - since) / 1000;
    yield* Effect.sleep("20 seconds");
    const records = yield* readInvocationRecords(since);
    const isCompactor = (record: InvocationRecord) =>
      Option.exists(record.scriptName, (name) => name.includes("compactorworker"));
    const writerFlushes = records.filter(
      (record) =>
        Option.isSome(record.durableObjectId) &&
        !isCompactor(record) &&
        (record.eventKind === "alarm" ||
          Option.exists(record.path, (path) => path.endsWith(`/databases/${databaseId}/flush`))),
    );
    const compactor = records.filter(isCompactor);
    const byKind = new Map<string, Array<number>>();
    for (const record of compactor) {
      const kind = record.eventKind;
      byKind.set(kind, [...(byKind.get(kind) ?? []), record.cpuTimeMs]);
    }
    const writerWrites = records.filter(
      (record) =>
        Option.isSome(record.durableObjectId) &&
        Option.exists(record.path, (path) => path.endsWith(`/databases/${databaseId}/batches`)),
    );
    const sumOf = (values: ReadonlyArray<number>) =>
      values.reduce((total, value) => total + value, 0);
    return {
      databaseId,
      megabytesPerFlush,
      compacted,
      compactedAfterSeconds,
      // A flush's work is split between the alarm that starts it and the flush request that joins
      // it, so one flush's CPU is the sum over both, not either record alone.
      flushCpuMsPerFlush: sumOf(writerFlushes.map((record) => record.cpuTimeMs)) / 4,
      writeCpuMsTotal: sumOf(writerWrites.map((record) => record.cpuTimeMs)),
      flushCpuMs: summarize(writerFlushes.map((record) => record.cpuTimeMs)),
      flushWallMs: summarize(writerFlushes.map((record) => record.wallTimeMs)),
      compactorCpuMsByEventKind: Object.fromEntries(
        [...byKind.entries()].map(([kind, values]) => [kind, summarize(values)]),
      ),
      compactorOutcomes: [...new Set(compactor.map((record) => record.outcome))],
      writerOutcomes: [...new Set(writerFlushes.map((record) => record.outcome))],
    };
  });

/**
 * Fills one writer with `megabytes` of 1 KB rows without flushing (deploy with `l0SstSize` at least
 * that large), then flushes it, and reports every writer invocation's CPU and outcome: whether a
 * Durable Object holding that much in SQLite, and flushing it, stays within its memory and CPU
 * limits (design §12, D61).
 *
 * @returns The report, or `MeasurementError` when the database cannot be created or tail records
 *   cannot be read (refused writes are counted, not raised).
 */
export const measureWriterMemory = (megabytes: number) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-mem"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const batchRows = 2000;
    const value = valueBytes(1000, 17);
    const batches = Math.ceil((megabytes * 1_048_576) / (batchRows * 1000));
    const since = yield* Clock.currentTimeMillis;
    const refused = new Map<string, number>();
    yield* Effect.forEach(
      Array.from({ length: batches }, (_, index) => index),
      (sequence) =>
        client.databases
          .write({
            params: { databaseId },
            payload: {
              operations: Array.from({ length: batchRows }, (_, row) => ({
                _tag: "Put" as const,
                key: keyBytes(0, sequence, row),
                value,
                ttlMillis: Option.none(),
              })),
            },
          })
          .pipe(
            Effect.catchCause((cause) =>
              Effect.sync(() => {
                // The error's tag (`WriteStalledPublicError`, `StorageUnavailablePublicError`).
                const tag = /Fail\((\w+)/.exec(String(cause))?.[1] ?? String(cause).slice(0, 60);
                refused.set(tag, (refused.get(tag) ?? 0) + 1);
              }),
            ),
          ),
      { concurrency: 4, discard: true },
    );
    // A writer that was reset under the load answers 503 while it starts again: wait for it, so
    // the report says what the load did instead of failing on the first refused request.
    const before = yield* client.databases
      .status({ params: { databaseId } })
      .pipe(
        Effect.retry({ times: 20, schedule: Schedule.spaced("1 second") }),
        Effect.mapError(failed("status before the flush")),
      );
    const flushStarted = yield* Clock.currentTimeMillis;
    const flushed = yield* Effect.result(client.databases.flush({ params: { databaseId } }));
    const flushClientMs = (yield* Clock.currentTimeMillis) - flushStarted;
    yield* Effect.sleep("20 seconds");
    const records = (yield* readInvocationRecords(since)).filter((record) =>
      Option.isSome(record.durableObjectId),
    );
    const outcomes = new Map<string, number>();
    for (const record of records)
      outcomes.set(record.outcome, (outcomes.get(record.outcome) ?? 0) + 1);
    const flushes = records.filter(
      (record) =>
        record.eventKind === "alarm" ||
        Option.exists(record.path, (path) => path.endsWith(`/databases/${databaseId}/flush`)),
    );
    return {
      databaseId,
      megabytes,
      unflushedBytesBeforeFlush: before.unflushedBytes,
      refusedWrites: Object.fromEntries(refused),
      flushSucceeded: flushed._tag === "Success",
      flushClientMs,
      writerOutcomes: Object.fromEntries(outcomes),
      flushCpuMs: summarize(flushes.map((record) => record.cpuTimeMs)),
      flushWallMs: summarize(flushes.map((record) => record.wallTimeMs)),
      writeCpuMs: summarize(
        records
          .filter((record) => Option.exists(record.path, (path) => path.includes("/batches")))
          .map((record) => record.cpuTimeMs),
      ),
    };
  });

/**
 * Starts `databases` compactions at once: brings each database to three L0 SSTs, then writes and
 * flushes the fourth on every database concurrently, so every writer wakes its compactor and each
 * launches a compaction Workflow together (deploy with a `compactionBudgetCapacity` above
 * `databases`). Reports how many finished, how long the last took, and whether any start failed
 * (design §12: the Workflows concurrent-instance limit, D61).
 *
 * @returns The report, or `MeasurementError` when a database cannot be created or written.
 */
export const measureWorkflowBurst = (databases: number) =>
  Effect.gen(function* () {
    const group = uniqueId("ms-wf");
    const ids = Array.from({ length: databases }, (_, index) =>
      DatabaseId.make(`${group}-${index.toString().padStart(4, "0")}`),
    );
    const client = yield* dbApi;
    const value = valueBytes(100, 9);
    const writeAndFlush = (databaseId: DatabaseId, round: number) =>
      Effect.gen(function* () {
        yield* client.databases
          .write({
            params: { databaseId },
            payload: {
              operations: [
                {
                  _tag: "Put" as const,
                  key: keyBytes(0, round, 0),
                  value,
                  ttlMillis: Option.none(),
                },
              ],
            },
          })
          .pipe(Effect.mapError(failed("write")));
        yield* client.databases
          .flush({ params: { databaseId } })
          .pipe(Effect.mapError(failed("flush")));
      });
    yield* Effect.forEach(
      ids,
      (databaseId) =>
        Effect.gen(function* () {
          yield* createDatabase(databaseId);
          for (let round = 0; round < 3; round++) yield* writeAndFlush(databaseId, round);
        }),
      { concurrency: 16, discard: true },
    );
    const since = yield* Clock.currentTimeMillis;
    // The fourth flush everywhere at once: every compactor launches its Workflow together.
    yield* Effect.forEach(ids, (databaseId) => writeAndFlush(databaseId, 3), {
      concurrency: "unbounded",
      discard: true,
    });
    const burstSeconds = ((yield* Clock.currentTimeMillis) - since) / 1000;
    const pending = new Set(ids);
    let maxRunning = 0;
    for (let attempt = 0; attempt < 120 && pending.size > 0; attempt++) {
      yield* Effect.sleep("5 seconds");
      let running = 0;
      yield* Effect.forEach(
        [...pending],
        (databaseId) =>
          Effect.gen(function* () {
            const status = yield* client.databases
              .compaction({ params: { databaseId } })
              .pipe(Effect.option);
            if (Option.isNone(status)) return;
            if (status.value.l0Ssts === 0 && status.value.sortedRuns.length > 0)
              pending.delete(databaseId);
            else if (Option.exists(status.value.compactor, (state) => state.running.length > 0))
              running++;
          }),
        { concurrency: 32, discard: true },
      );
      maxRunning = Math.max(maxRunning, running);
    }
    const allDoneSeconds = ((yield* Clock.currentTimeMillis) - since) / 1000;
    yield* Effect.sleep("15 seconds");
    const records = (yield* readInvocationRecords(since)).filter((record) =>
      Option.exists(record.scriptName, (name) => name.includes("compactorworker")),
    );
    const outcomes = new Map<string, number>();
    for (const record of records)
      outcomes.set(record.outcome, (outcomes.get(record.outcome) ?? 0) + 1);
    return {
      databases,
      burstSeconds,
      compacted: databases - pending.size,
      notCompacted: pending.size,
      allDoneSeconds,
      maxRunningObserved: maxRunning,
      compactorOutcomes: Object.fromEntries(outcomes),
    };
  });

/** One span as `trace-cli grep --json` prints it (the fields the health report reads). */
const GreppedSpan = Schema.Struct({
  traceId: Schema.String,
  name: Schema.String,
  startTimeUnixNano: Schema.String,
  durationMs: Schema.Number,
  status: Schema.optional(
    Schema.Struct({
      code: Schema.optional(Schema.Unknown),
      message: Schema.optional(Schema.NullOr(Schema.String)),
    }),
  ),
  attributes: Schema.Record(Schema.String, Schema.Unknown),
  events: Schema.optional(
    Schema.Array(
      Schema.Struct({
        name: Schema.String,
        attributes: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
      }),
    ),
  ),
});
const decodeGreppedSpans = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(GreppedSpan)),
);

/** The trace store a measurement reads a stage's spans from: its own, so stages never mix. */
const measurementStore = (stage: string) =>
  fileURLToPath(new URL(`../../../.traces/measure/${stage}`, import.meta.url));

/**
 * Pulls the stage's spans from the last `minutes` into its measurement store (resuming where the
 * last pull stopped). It names the collector by URL whichever form resolved it, with the token in
 * the trace CLI's environment, never on its command line.
 */
export const pullMeasurementSpans = (minutes: number) =>
  Effect.gen(function* () {
    const collector = yield* MeasurementCollector;
    const store = measurementStore(collector.stage);
    const pulled = yield* runTraceCliAsync(
      [
        "pull",
        "--collector-url",
        collector.url,
        "--store",
        store,
        "--since-minutes",
        String(minutes),
      ],
      { SHALEDB_TRACE_TOKEN: collector.token },
    );
    if (pulled.status !== 0) {
      return yield* new MeasurementError({
        step: "trace-cli pull",
        reason: "TraceCliFailed",
        message: `Pulling ${collector.stage}'s spans failed: ${pulled.stderr.slice(-500)}. Run the measurement again.`,
      });
    }
    return store;
  });

/** Spans `trace-cli grep` finds in `store` with `args` that started at or after `sinceMs`. */
export const spansSince = (store: string, sinceMs: number, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    // In its own process without blocking this one, so a time budget around it can cut it off.
    const result = yield* runTraceCliCaptured(["grep", "--json", "--store", store, ...args]);
    const step = `trace-cli grep ${args.join(" ")}`;
    if (result.status !== 0) {
      return yield* new MeasurementError({
        step,
        reason: "TraceCliFailed",
        message: `Measurement step "${step}" failed: ${result.stderr || result.stdout}. Run the measurement again.`,
      });
    }
    const spans = yield* decodeGreppedSpans(result.stdout || "[]").pipe(
      Effect.mapError(failed(step)),
    );
    return spans.filter((span) => Number(BigInt(span.startTimeUnixNano) / 1_000_000n) >= sinceMs);
  });

/** Invocations by outcome, most frequent first, scaled up from the kept records (D71). */
const estimatedOutcomes = (records: ReadonlyArray<InvocationRecord>) =>
  Object.fromEntries(
    [
      ...records.reduce(
        (counts, record) =>
          counts.set(
            record.outcome,
            (counts.get(record.outcome) ?? 0) + invocationsRepresentedBy(record),
          ),
        new Map<string, number>(),
      ),
    ]
      .map(([outcome, count]): readonly [string, number] => [outcome, Math.round(count)])
      .sort((left, right) => right[1] - left[1]),
  );

/** Options of {@link measureBaseline}. */
export interface BaselineLoad {
  /** Rows per write batch. */
  readonly batchRows: number;
  /** Bytes per value (keys are 20 bytes). */
  readonly valueBytes: number;
  /** Loops writing batches of new keys, each sending its next batch when the last is answered. */
  readonly writers: number;
  /** Loops reading one random preloaded key at a time. */
  readonly readers: number;
  readonly seconds: number;
}

/** What {@link measureBaseline} runs the same workload against. */
const BASELINE_TARGETS = [
  "shaledb, reads at Latest",
  "shaledb, reads at Flushed",
  "plain Durable Object SQLite",
] as const;
type BaselineTarget = (typeof BASELINE_TARGETS)[number];

/** Batches written before the timed run, so readers have keys to read. */
const BASELINE_PRELOAD_BATCHES = 20;

/**
 * R2 requests one object store span stands for, by the class Cloudflare bills them in: class A
 * changes or lists, class B reads. A multipart upload is a create, its parts, and a complete.
 */
const r2RequestsOfSpan = (span: typeof GreppedSpan.Type): { classA: number; classB: number } => {
  const parts = span.attributes["object.parts"];
  switch (span.name) {
    case "ObjectStore.putMultipart":
      return typeof parts === "number" && parts > 1
        ? { classA: parts + 2, classB: 0 }
        : { classA: 1, classB: 0 };
    case "ObjectStore.put":
    case "ObjectStore.putIfAbsent":
    case "ObjectStore.putIfVersion":
    case "ObjectStore.list":
    case "ObjectStore.listPrefixes":
      return { classA: 1, classB: 0 };
    case "ObjectStore.get":
    case "ObjectStore.getRange":
    case "ObjectStore.getVersioned":
    case "ObjectStore.head":
    case "ObjectReader.get":
    case "ObjectReader.getRange":
      return { classA: 0, classB: 1 };
    default:
      return { classA: 0, classB: 0 };
  }
};

/**
 * Runs one workload against shaledb (with its reads at `Latest`, then at `Flushed`) and against
 * a plain Durable Object SQLite table on the same stage (the probe Worker's `PlainSqliteServer`),
 * each on a new database: `writers` loops of `batchRows`-row batches and `readers` loops of point
 * reads, for `seconds`, after the same preload (D75).
 *
 * **Details**
 *
 * Per target it reports what the client saw (acknowledged rows per second; write and read
 * latency percentiles; failures) and what the stage did for it during the timed run: invocations
 * and CPU per Worker and Durable Object from tail records, and for shaledb the R2 requests its
 * spans show, by billing class. Deploy the stage at 100% sampling, or the counts are a sample.
 * The preload is flushed on shaledb, so its reads find rows in an SST, as they would in use.
 *
 * @returns The report per target, or `MeasurementError` when a database cannot be set up or the
 *   stage's records cannot be read (failed requests during the run are counted, not raised).
 */
export const measureBaseline = (load: BaselineLoad) =>
  Effect.gen(function* () {
    const stack = yield* ScenarioStack;
    const http = yield* HttpClient.HttpClient;
    const client = yield* dbApi;
    const value = valueBytes(load.valueBytes, 23);
    const preloadKey = (index: number) =>
      keyBytes(999, Math.floor(index / load.batchRows), index % load.batchRows);
    const preloadedKeys = BASELINE_PRELOAD_BATCHES * load.batchRows;

    const runTarget = (target: BaselineTarget) =>
      Effect.gen(function* () {
        const name = uniqueId("ms-base");
        const databaseId = DatabaseId.make(name);
        const plain = target === "plain Durable Object SQLite";
        const plainRequest = (operation: "batches" | "get", body: unknown) =>
          http
            .execute(
              HttpClientRequest.post(`${stack.probe}/v1/probe/sqlite/${name}/${operation}`).pipe(
                HttpClientRequest.bodyText(JSON.stringify(body), "application/json"),
              ),
            )
            .pipe(
              Effect.flatMap((response) => Effect.map(response.text, () => response.status)),
              Effect.mapError(failed(`plain SQLite ${operation}`)),
              Effect.flatMap((status) =>
                status === 200
                  ? Effect.void
                  : Effect.fail(
                      new MeasurementError({
                        step: `plain SQLite ${operation}`,
                        reason: `Status${status}`,
                        message: `The plain SQLite baseline answered ${status} to ${operation}. Run the measurement again.`,
                      }),
                    ),
              ),
            );
        const writeBatch = (
          keys: ReadonlyArray<Uint8Array>,
        ): Effect.Effect<void, { readonly _tag: string; readonly message?: string }> =>
          plain
            ? plainRequest("batches", {
                operations: keys.map((key) => ({
                  key: Base64.encode(key),
                  value: Base64.encode(value),
                })),
              })
            : Effect.asVoid(
                client.databases.write({
                  params: { databaseId },
                  payload: {
                    operations: keys.map((key) => ({
                      _tag: "Put" as const,
                      key,
                      value,
                      ttlMillis: Option.none(),
                    })),
                  },
                }),
              );
        const readKey = (
          key: Uint8Array,
        ): Effect.Effect<void, { readonly _tag: string; readonly message?: string }> =>
          plain
            ? plainRequest("get", { key: Base64.encode(key) })
            : Effect.asVoid(
                client.databases.get({
                  params: { databaseId },
                  payload: {
                    key,
                    consistency:
                      target === "shaledb, reads at Flushed"
                        ? { _tag: "Flushed" as const }
                        : { _tag: "Latest" as const },
                    snapshot: Option.none(),
                  },
                }),
              );

        if (!plain) yield* createDatabase(databaseId);
        for (let batch = 0; batch < BASELINE_PRELOAD_BATCHES; batch++) {
          yield* writeBatch(
            Array.from({ length: load.batchRows }, (_, row) =>
              preloadKey(batch * load.batchRows + row),
            ),
          ).pipe(Effect.mapError(failed("preload")));
        }
        if (!plain) {
          yield* client.databases
            .flush({ params: { databaseId } })
            .pipe(Effect.mapError(failed("flush the preload")));
        }

        const writeLatencies: Array<number> = [];
        const readLatencies: Array<number> = [];
        const failures = new Map<string, number>();
        let rows = 0;
        const fail = (kind: string, error: { readonly _tag: string }) =>
          failures.set(`${kind}: ${error._tag}`, (failures.get(`${kind}: ${error._tag}`) ?? 0) + 1);
        const started = yield* Clock.currentTimeMillis;
        const deadline = started + load.seconds * 1000;
        const writer = (worker: number) =>
          Effect.gen(function* () {
            let sequence = 0;
            while ((yield* Clock.currentTimeMillis) < deadline) {
              sequence += 1;
              const keys = Array.from({ length: load.batchRows }, (_, row) =>
                keyBytes(worker, sequence, row),
              );
              const sent = yield* Clock.currentTimeMillis;
              const outcome = yield* Effect.result(writeBatch(keys));
              if (outcome._tag === "Success") {
                writeLatencies.push((yield* Clock.currentTimeMillis) - sent);
                rows += load.batchRows;
              } else {
                fail("write", outcome.failure);
                yield* Effect.sleep("200 millis");
              }
            }
          });
        const reader = Effect.gen(function* () {
          while ((yield* Clock.currentTimeMillis) < deadline) {
            const key = preloadKey(Math.floor(Math.random() * preloadedKeys));
            const sent = yield* Clock.currentTimeMillis;
            const outcome = yield* Effect.result(readKey(key));
            if (outcome._tag === "Success") {
              readLatencies.push((yield* Clock.currentTimeMillis) - sent);
            } else {
              fail("read", outcome.failure);
              yield* Effect.sleep("200 millis");
            }
          }
        });
        yield* Effect.all(
          [
            ...Array.from({ length: load.writers }, (_, worker) => writer(worker)),
            ...Array.from({ length: load.readers }, () => reader),
          ],
          { concurrency: "unbounded", discard: true },
        );
        const ended = yield* Clock.currentTimeMillis;
        return { target, name, started, ended, rows, writeLatencies, readLatencies, failures };
      });

    const runs = [];
    for (const target of BASELINE_TARGETS) runs.push(yield* runTarget(target));

    // Tail records and spans arrive after each invocation finishes.
    yield* Effect.sleep("25 seconds");
    const [first] = runs;
    const since = first === undefined ? yield* Clock.currentTimeMillis : first.started;
    const records = yield* readInvocationRecords(since);
    const store = yield* pullMeasurementSpans(
      Math.ceil(((yield* Clock.currentTimeMillis) - since) / 60_000) + 1,
    );
    const objectSpans = [
      ...(yield* spansSince(store, since, ["--name-prefix", "ObjectStore."])),
      ...(yield* spansSince(store, since, ["--name-prefix", "ObjectReader."])),
    ];
    const blockFetches = yield* spansSince(store, since, ["--name-prefix", "BlockSource."]);

    return {
      load,
      preloadedRows: preloadedKeys,
      targets: runs.map((run) => {
        const inRun = (atMs: number) => atMs >= run.started && atMs <= run.ended + 2_000;
        const own = records.filter(
          (record) =>
            Option.exists(record.eventTimestamp, inRun) &&
            Option.exists(record.path, (path) => path.includes(`/${run.name}/`)),
        );
        // A Durable Object's requests carry the caller's path on shaledb, and only `/batches` or
        // `/get` on the baseline: every record of the objects this run's requests reached is its.
        const objects = new Set(own.flatMap((record) => Option.toArray(record.durableObjectId)));
        const plainObject = records.filter(
          (record) =>
            run.target === "plain Durable Object SQLite" &&
            Option.exists(record.eventTimestamp, inRun) &&
            Option.isSome(record.durableObjectId) &&
            Option.exists(record.scriptName, (script) => script.includes("telemetryprobe")),
        );
        const durable = [
          ...records.filter(
            (record) =>
              Option.exists(record.eventTimestamp, inRun) &&
              Option.exists(record.durableObjectId, (id) => objects.has(id)),
          ),
          ...plainObject,
        ];
        const workers = own.filter((record) => Option.isNone(record.durableObjectId));
        const cpu = (list: ReadonlyArray<InvocationRecord>) =>
          Math.round(list.reduce((sum, record) => sum + record.cpuTimeMs, 0));
        const spanInRun = (span: typeof GreppedSpan.Type) =>
          inRun(Number(BigInt(span.startTimeUnixNano) / 1_000_000n)) &&
          span.attributes["database.id"] === run.name;
        const traces = new Set(blockFetches.filter(spanInRun).map((span) => span.traceId));
        // Object store spans carry no database id: they belong to a run through their trace, or
        // (the writer's flush and the manifest loads) through their time, the only run going on.
        const r2 = objectSpans
          .filter((span) => inRun(Number(BigInt(span.startTimeUnixNano) / 1_000_000n)))
          .reduce(
            (sum, span) => {
              const requests = r2RequestsOfSpan(span);
              return {
                classA: sum.classA + requests.classA,
                classB: sum.classB + requests.classB,
              };
            },
            { classA: 0, classB: 0 },
          );
        const seconds = (run.ended - run.started) / 1000;
        return {
          target: run.target,
          name: run.name,
          seconds,
          acknowledgedRowsPerSecond: Math.round(run.rows / seconds),
          writeBatchesPerSecond: Number((run.writeLatencies.length / seconds).toFixed(1)),
          readsPerSecond: Number((run.readLatencies.length / seconds).toFixed(1)),
          writeLatencyMs: summarize(run.writeLatencies),
          readLatencyMs: summarize(run.readLatencies),
          failures: Object.fromEntries(run.failures),
          workerInvocations: { count: workers.length, cpuMs: cpu(workers) },
          durableObjectInvocations: { count: durable.length, cpuMs: cpu(durable) },
          r2Requests: run.target === "plain Durable Object SQLite" ? { classA: 0, classB: 0 } : r2,
          tracesWithBlockFetches: traces.size,
        };
      }),
    };
  });

/** The operations {@link measureLatency} reports, each a span the API Worker records per request. */
const LATENCY_OPERATIONS: ReadonlyArray<{
  readonly operation: string;
  readonly spanName: string;
  readonly consistency?: "Latest" | "AtLeast" | "Flushed";
}> = [
  { operation: "get Latest", spanName: "DbClient.getRow", consistency: "Latest" },
  { operation: "get AtLeast", spanName: "DbClient.getRow", consistency: "AtLeast" },
  { operation: "get Flushed", spanName: "DbClient.getRow", consistency: "Flushed" },
  { operation: "scan", spanName: "DbClient.scan" },
  { operation: "write batch", spanName: "WriterClient.write" },
  { operation: "transaction commit", spanName: "WriterClient.commitTransaction" },
];

/**
 * Server-side latency of a live stage's reads, scans, writes, and transaction commits over the
 * last `minutes`, from its sampled spans: the span each operation records in the API Worker, so
 * the caller's network and the trace export after the response are not in it (D67).
 *
 * **Details**
 *
 * `blockReads` counts the `BlockSource.block` and `BlockSource.meta` spans in the operation's
 * trace: the blocks and SST metadata it fetched from the `BlockServer` because the isolate's cache
 * did not hold them. Run it after a workload (a soak, for example), with `minutes` covering it.
 *
 * @returns Per operation: how many were sampled, their latency percentiles in milliseconds, and
 *   their block-read percentiles; or `MeasurementError` when the spans cannot be pulled or read.
 */
export const measureLatency = (minutes: number) =>
  Effect.gen(function* () {
    const sinceMs = (yield* Clock.currentTimeMillis) - minutes * 60_000;
    const store = yield* pullMeasurementSpans(minutes);
    const blockReadsByTrace = new Map<string, number>();
    for (const name of ["BlockSource.block", "BlockSource.meta"]) {
      for (const span of yield* spansSince(store, sinceMs, ["--name", name])) {
        blockReadsByTrace.set(span.traceId, (blockReadsByTrace.get(span.traceId) ?? 0) + 1);
      }
    }
    const spansByName = new Map<string, ReadonlyArray<typeof GreppedSpan.Type>>();
    for (const spanName of new Set(LATENCY_OPERATIONS.map((operation) => operation.spanName))) {
      spansByName.set(spanName, yield* spansSince(store, sinceMs, ["--name", spanName]));
    }
    return {
      minutes,
      operations: LATENCY_OPERATIONS.map(({ operation, spanName, consistency }) => {
        const spans = (spansByName.get(spanName) ?? []).filter(
          (span) =>
            consistency === undefined || span.attributes["read.consistency"] === consistency,
        );
        const blockReads = spans.map((span) => blockReadsByTrace.get(span.traceId) ?? 0);
        return {
          operation,
          sampled: spans.length,
          latencyMs: summarize(spans.map((span) => span.durationMs)),
          blockReads: { p50: percentile(blockReads, 0.5), p90: percentile(blockReads, 0.9) },
        };
      }),
    };
  });

/**
 * Head sampling on a live stage (D63): sends `requests` writes, each its own trace, to one new
 * database, then reports what fraction of traces was kept and whether each kept trace is whole: the
 * API Worker's `WriterClient.write` and the writer Durable Object's `DbWriter.write` in the same
 * trace, never one without the other. Deploy with `--trace-sample-rate` to measure a fraction.
 *
 * @returns The report, or `MeasurementError` when the database cannot be created or written.
 */
export const measureSampling = (requests: number) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-sample"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const value = valueBytes(16, 3);
    const sinceMs = yield* Clock.currentTimeMillis;
    for (let request = 0; request < requests; request++) {
      yield* client.databases
        .write({
          params: { databaseId },
          payload: {
            operations: [
              { _tag: "Put", key: keyBytes(0, 0, request), value, ttlMillis: Option.none() },
            ],
          },
        })
        .pipe(Effect.mapError(failed("write")));
    }
    // Live stages export after each response: give the last ones time to arrive.
    yield* Effect.sleep("20 seconds");
    const store = yield* pullMeasurementSpans(10);
    const tracesOf = (name: string) =>
      spansSince(store, sinceMs, ["--name", name, "--db", databaseId]).pipe(
        Effect.map((spans) => new Set(spans.map((span) => span.traceId))),
      );
    const apiSide = yield* tracesOf("WriterClient.write");
    const writerSide = yield* tracesOf("DbWriter.write");
    const whole = [...apiSide].filter((trace) => writerSide.has(trace)).length;
    return {
      databaseId,
      requests,
      tracesKept: apiSide.size,
      keptFraction: apiSide.size / requests,
      wholeTraces: whole,
      apiSideOnly: apiSide.size - whole,
      writerSideOnly: [...writerSide].filter((trace) => !apiSide.has(trace)).length,
    };
  });

/**
 * A live stage's health over the last `minutes` (D63, D82), from its tail records and its spans:
 * what `docs/operations.md` says to watch.
 *
 * - Invocations per Worker: outcomes and CPU, from the tail records. Any `exceededCpu`,
 *   `exceededMemory`, or `exception` is an issue.
 * - Everything else is judged by {@link summarizeHealth} from spans: stalled writes per database
 *   and reason, flushes, the L0 backlog, corrupted SSTs, fenced compactors, multipart uploads with
 *   a re-sent or unverified part, failed collections and sweeps, and the most frequent error spans.
 *
 * Spans are sampled in production (P4), so span counts there are a sample; tail records of
 * invocations that did not end `ok` are not (D71).
 *
 * @returns The report with an `issues` list (empty when healthy), or `MeasurementError` when tail
 *   records cannot be read.
 */
export const measureHealth = (minutes: number) =>
  Effect.gen(function* () {
    const sinceMs = (yield* Clock.currentTimeMillis) - minutes * 60_000;
    const records = yield* readInvocationRecords(sinceMs);
    const store = yield* pullMeasurementSpans(minutes);
    const workers = new Map<string, Array<InvocationRecord>>();
    for (const record of records) {
      const worker = Option.getOrElse(record.scriptName, () => "unknown");
      workers.set(worker, [...(workers.get(worker) ?? []), record]);
    }
    const invocations = Object.fromEntries(
      [...workers].map(([worker, list]) => [
        worker,
        {
          // Estimated from the kept records: every invocation that did not end `ok` is kept,
          // and each kept `ok` record stands for `1 / sampleRate` invocations (D71).
          outcomes: estimatedOutcomes(list),
          recordsKept: list.length,
          cpuMs: summarize(list.map((record) => record.cpuTimeMs)),
        },
      ]),
    );
    const limitHits = records
      .filter((record) => ["exceededCpu", "exceededMemory", "exception"].includes(record.outcome))
      .map((record) => ({
        worker: Option.getOrElse(record.scriptName, () => "a Worker"),
        outcome: record.outcome,
        eventKind: record.eventKind,
        cpuTimeMs: record.cpuTimeMs,
      }));
    const named = (name: string, ...more: ReadonlyArray<string>) =>
      spansSince(store, sinceMs, ["--name", name, ...more]);
    const report = summarizeHealth(limitHits, {
      refusedWrites: yield* named("DbWriter.write", "--error"),
      flushes: yield* named("DbWriter.flush"),
      failedFlushes: yield* named("DbWriter.flush", "--error"),
      polls: yield* named("Compactor.poll"),
      uploads: yield* named("ObjectStore.putMultipart"),
      failedCollections: yield* named("GarbageCollector.collect", "--error"),
      failedSweeps: yield* named("MaintenanceSweep.sweepPage", "--error"),
      errors: yield* spansSince(store, sinceMs, ["--error"]),
    });
    return { minutes, ...report, invocations };
  });

/** Options of {@link measureSplitPause}. */
export interface SplitLoad {
  readonly writers: number;
  readonly secondsBefore: number;
  readonly secondsAfter: number;
}

/**
 * Writes through the shard router from `writers` loops (one row per request, keys spread over
 * `a`–`z`), splits the shard at `m` midway, and reports how long writes paused: the longest put and
 * the longest gap between acknowledgements while the split ran, against the latency before it.
 *
 * @returns The report (failed puts are counted, not raised), or `MeasurementError` when the group
 *   cannot be created, the split fails, or its spans cannot be read.
 */
export const measureSplitPause = (load: SplitLoad) =>
  Effect.gen(function* () {
    const group = ShardGroup.make(uniqueId("ms-split"));
    const databaseId = DatabaseId.make(`${group}-0`);
    const shards = yield* shardsApi;
    yield* shards.groups
      .create({ params: { group }, payload: { databaseId } })
      .pipe(Effect.mapError(failed("create the group")));
    type Put = { readonly sent: number; readonly done: number; readonly ok: boolean };
    const puts: Array<Put> = [];
    const value = valueBytes(100, 3);
    const started = yield* Clock.currentTimeMillis;
    const stopAt = { ms: Number.POSITIVE_INFINITY };
    const writers = Effect.forEach(
      Array.from({ length: load.writers }, (_, worker) => worker),
      (worker) =>
        Effect.gen(function* () {
          let sequence = 0;
          while ((yield* Clock.currentTimeMillis) < stopAt.ms) {
            sequence += 1;
            const letter = String.fromCharCode(97 + ((worker * 7 + sequence) % 26));
            const sent = yield* Clock.currentTimeMillis;
            const outcome = yield* Effect.result(
              shards.groups.put({
                params: { group },
                payload: {
                  key: new TextEncoder().encode(`${letter}-${worker}-${sequence}`),
                  value,
                },
              }),
            );
            puts.push({
              sent,
              done: yield* Clock.currentTimeMillis,
              ok: outcome._tag === "Success",
            });
          }
        }),
      { concurrency: "unbounded", discard: true },
    );
    const split = Effect.gen(function* () {
      yield* Effect.sleep(`${load.secondsBefore} seconds`);
      const splitStarted = yield* Clock.currentTimeMillis;
      yield* shards.groups
        .split({
          params: { group },
          payload: { databaseId, atKey: new TextEncoder().encode("m") },
        })
        .pipe(Effect.mapError(failed("split")));
      const splitDone = yield* Clock.currentTimeMillis;
      stopAt.ms = splitDone + load.secondsAfter * 1000;
      return { splitStarted, splitDone };
    });
    const [, window] = yield* Effect.all([writers, split], { concurrency: "unbounded" });
    const before = puts.filter((put) => put.ok && put.done < window.splitStarted);
    const during = puts.filter(
      (put) => put.done >= window.splitStarted && put.sent <= window.splitDone,
    );
    const acks = puts
      .filter((put) => put.ok)
      .map((put) => put.done)
      .sort((left, right) => left - right);
    let longestGap = 0;
    for (let index = 1; index < acks.length; index++) {
      const previous = acks[index - 1] ?? 0;
      const current = acks[index] ?? 0;
      if (current >= window.splitStarted && previous <= window.splitDone) {
        longestGap = Math.max(longestGap, current - previous);
      }
    }
    const splits = yield* pulledSpans("ShardSplit.split", databaseId);
    const seals = yield* pulledSpans("DbWriter.seal", databaseId);
    return {
      group,
      load,
      elapsedSeconds: ((yield* Clock.currentTimeMillis) - started) / 1000,
      splitClientMs: window.splitDone - window.splitStarted,
      splitSpanMs: splits.map((span) => span.durationMs),
      sealSpanMs: seals.map((span) => span.durationMs),
      putsBeforeSplitMs: summarize(before.map((put) => put.done - put.sent)),
      putsDuringSplitMs: summarize(during.map((put) => put.done - put.sent)),
      failedPuts: puts.filter((put) => !put.ok).length,
      totalPuts: puts.length,
      longestAckGapDuringSplitMs: longestGap,
    };
  });

/** Options of {@link measureTransactionAborts}. */
export interface TransactionLoad {
  /** Concurrent transaction loops (each: begin, read two keys, commit one put). */
  readonly transactors: number;
  /** Concurrent plain-write loops (100-row batches) that keep the writer flushing. */
  readonly writers: number;
  readonly seconds: number;
  /** How long each transaction stays open between its first read and the rest (0: none). */
  readonly holdSeconds: number;
  /** Flush the database this often while the load runs (0: only flushes the writer takes itself). */
  readonly flushEverySeconds: number;
}

/**
 * Runs `SerializableSnapshot` transactions over the stateless HTTP API (begin, two reads, commit)
 * on disjoint keys while plain writes load the same database, optionally holding each transaction
 * open across forced flushes, and counts how many abort with `SnapshotTooOld`. The transactors never
 * conflict with each other or with the writers, so every such abort is a snapshot the writer could
 * no longer check (since D58 only one that outlives `transactionRetentionMs`).
 *
 * @returns The report (aborts are counted by reason, not raised), or `MeasurementError` when the
 *   database cannot be created or read back.
 */
export const measureTransactionAborts = (load: TransactionLoad) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-txn"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const started = yield* Clock.currentTimeMillis;
    const deadline = started + load.seconds * 1000;
    const durations: Array<number> = [];
    const outcomes = new Map<string, number>();
    const record = (outcome: string) => outcomes.set(outcome, (outcomes.get(outcome) ?? 0) + 1);
    const value = valueBytes(100, 5);
    const writers = Effect.forEach(
      Array.from({ length: load.writers }, (_, worker) => worker),
      (worker) =>
        Effect.gen(function* () {
          let sequence = 0;
          while ((yield* Clock.currentTimeMillis) < deadline) {
            sequence += 1;
            yield* Effect.ignore(
              client.databases.write({
                params: { databaseId },
                payload: {
                  operations: Array.from({ length: 100 }, (_, row) => ({
                    _tag: "Put" as const,
                    key: keyBytes(100 + worker, sequence, row),
                    value,
                    ttlMillis: Option.none(),
                  })),
                },
              }),
            );
          }
        }),
      { concurrency: "unbounded", discard: true },
    );
    const transactors = Effect.forEach(
      Array.from({ length: load.transactors }, (_, worker) => worker),
      (worker) =>
        Effect.gen(function* () {
          let sequence = 0;
          while ((yield* Clock.currentTimeMillis) < deadline) {
            sequence += 1;
            const own = (name: string) => new TextEncoder().encode(`txn-${worker}-${name}`);
            const began = yield* Clock.currentTimeMillis;
            const outcome = yield* Effect.result(
              Effect.gen(function* () {
                const { readSeq, manifestId } = yield* client.databases.beginTransaction({
                  params: { databaseId },
                });
                for (const [index, name] of ["a", "b"].entries()) {
                  if (index === 1 && load.holdSeconds > 0) {
                    yield* Effect.sleep(`${load.holdSeconds} seconds`);
                  }
                  yield* client.databases.transactionGet({
                    params: { databaseId },
                    payload: { key: own(name), readSeq, manifestId },
                  });
                }
                return yield* client.databases.commitTransaction({
                  params: { databaseId },
                  payload: {
                    readSeq,
                    isolation: "SerializableSnapshot",
                    batch: {
                      operations: [
                        {
                          _tag: "Put",
                          key: own("a"),
                          value: new TextEncoder().encode(String(sequence)),
                          ttlMillis: Option.none(),
                        },
                      ],
                    },
                    readKeys: [own("a"), own("b")],
                    readRanges: [],
                  },
                });
              }),
            );
            durations.push((yield* Clock.currentTimeMillis) - began);
            if (outcome._tag === "Success") record("Committed");
            else if (outcome.failure._tag === "TransactionConflictPublicError") {
              record(outcome.failure.details.reason);
            } else record(outcome.failure._tag);
          }
        }),
      { concurrency: "unbounded", discard: true },
    );
    let forcedFlushes = 0;
    const flusher = Effect.gen(function* () {
      if (load.flushEverySeconds <= 0) return;
      while ((yield* Clock.currentTimeMillis) < deadline) {
        yield* Effect.sleep(`${load.flushEverySeconds} seconds`);
        const flushed = yield* Effect.result(client.databases.flush({ params: { databaseId } }));
        if (flushed._tag === "Success") forcedFlushes += 1;
      }
    });
    yield* Effect.all([writers, transactors, flusher], { concurrency: "unbounded", discard: true });
    const status = yield* client.databases
      .status({ params: { databaseId } })
      .pipe(Effect.mapError(failed("status after the load")));
    const total = [...outcomes.values()].reduce((sum, count) => sum + count, 0);
    return {
      databaseId,
      load,
      transactions: total,
      outcomes: Object.fromEntries(outcomes),
      snapshotTooOldRate: (outcomes.get("SnapshotTooOld") ?? 0) / Math.max(1, total),
      transactionMs: summarize(durations),
      forcedFlushes,
      l0SstsAtEnd: status.l0Count,
    };
  });
