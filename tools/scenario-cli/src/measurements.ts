import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { DatabaseId } from "@cf-slatedb/lsm-core";
import { DbHttpApi } from "@cf-slatedb/lsm-cloudflare";
import { ShardedHttpApi, ShardGroup } from "@cf-slatedb/sharding";
import { Clock, Context, Data, Effect, Option, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { ScenarioStack, uniqueId } from "./scenario.ts";

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

/** Where a preview stage's tail records and spans can be read (`preview-cli collector`). */
export interface IMeasurementCollector {
  readonly stage: string;
  readonly url: string;
  readonly token: string;
}

/** The preview stage's trace collector, for tail records (CPU time) and spans. */
export class MeasurementCollector extends Context.Service<
  MeasurementCollector,
  IMeasurementCollector
>()("@cf-slatedb/scenario/MeasurementCollector") {}

/** Percentile (nearest rank) of `values`, in their unit; `0` when empty. */
export const percentile = (values: ReadonlyArray<number>, fraction: number): number => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil(fraction * sorted.length) - 1));
  return sorted[rank] ?? 0;
};

const summarize = (values: ReadonlyArray<number>) => ({
  count: values.length,
  p50: percentile(values, 0.5),
  p90: percentile(values, 0.9),
  p99: percentile(values, 0.99),
  max: values.length === 0 ? 0 : Math.max(...values),
});

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
    };
  });

/** Schema of the Tail Worker's invocation records (`infra/cf-slatedb/src/cpu-tail-worker.ts`). */
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
});
type InvocationRecord = typeof InvocationRecord.Type;

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
export const readInvocationRecords = (sinceMs: number) =>
  Effect.gen(function* () {
    const collector = yield* MeasurementCollector;
    const client = yield* HttpClient.HttpClient;
    // Keys start with the arrival time, so the listing can start just before `sinceMs`.
    let after = Option.some(`tails/${(sinceMs - 1).toString().padStart(15, "0")}`);
    const records: Array<InvocationRecord> = [];
    while (true) {
      const url = new URL("/v1/tails/batches", collector.url);
      url.searchParams.set("limit", "100");
      if (Option.isSome(after)) url.searchParams.set("after", after.value);
      const page = yield* client
        .execute(
          HttpClientRequest.get(url.toString()).pipe(
            HttpClientRequest.bearerToken(collector.token),
          ),
        )
        .pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(decodeTailPage),
          Effect.mapError(failed("read tail records")),
        );
      for (const batch of page.batches) {
        const body = Base64.decode(batch.body);
        if (body._tag === "Failure") continue;
        records.push(
          ...(yield* decodeRecords(new TextDecoder().decode(body.success)).pipe(
            Effect.mapError(failed("decode tail records")),
          )),
        );
      }
      if (page.next !== null) after = Option.some(page.next);
      if (!page.more || page.batches.length === 0) break;
    }
    return records;
    // Its own client: the stage's HTTP client carries the access token, not the collector's.
  }).pipe(Effect.provide(FetchHttpClient.layer));

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
    yield* Effect.sync(() =>
      spawnSync(process.execPath, [TRACE_CLI, "pull", "--stage", stage], { encoding: "utf8" }),
    );
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
export const measureFlushCpu = (megabytes: number) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("ms-flush"));
    yield* createDatabase(databaseId);
    const client = yield* dbApi;
    const batchRows = 2000;
    const rowValueBytes = 1000;
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
      writeBatches: {
        rowsPerBatch: batchRows,
        writerCpuMs: summarize(writerWrites.map((record) => record.cpuTimeMs)),
        writerWallMs: summarize(writerWrites.map((record) => record.wallTimeMs)),
      },
    };
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
}

/**
 * Runs `SerializableSnapshot` transactions over the stateless HTTP API (begin, two reads, commit)
 * on disjoint keys while plain writes load the same database, and counts how many abort because a
 * flush landed during them (`SnapshotTooOld`, D41/D50). The transactors never conflict with each
 * other or with the writers, so every abort is the flush limit.
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
                const { readSeq } = yield* client.databases.beginTransaction({
                  params: { databaseId },
                });
                for (const name of ["a", "b"]) {
                  yield* client.databases.transactionGet({
                    params: { databaseId },
                    payload: { key: own(name), readSeq },
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
    yield* Effect.all([writers, transactors], { concurrency: "unbounded", discard: true });
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
      flushesDuringRun: status.l0Count,
    };
  });
