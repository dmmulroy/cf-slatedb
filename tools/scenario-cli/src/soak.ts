import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { DatabaseId, SeqNum } from "@shaledb/lsm-core";
import { DbHttpApi, GcHttpApi } from "@shaledb/lsm-cloudflare";
import { ShardedHttpApi, ShardGroup } from "@shaledb/sharding";
import { Clock, Duration, Effect, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import {
  exceptionType,
  forEachInvocationRecord,
  measureHealth,
  measurementStorePath,
  pullMeasurementSpans,
  spansPulledThroughMs,
  spansSince,
} from "./measurements.ts";
import { ScenarioStack, uniqueId } from "./scenario.ts";
import {
  ChangeLedger,
  FeedFollower,
  MergeKeyModel,
  rowSignature,
  ScanTraversal,
  type SoakChange,
} from "./soak-checks.ts";
import { soakPassed, type SoakPhase, writeReportFile } from "./soak-report.ts";
import {
  catchUpTails,
  every,
  makeSpanPuller,
  readTailsWithin,
  readWithin,
  TailTally,
  whileWorkloadRuns,
} from "./soak-telemetry.ts";

const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));

/** How the soak runs (`scenario-cli soak`). */
export interface SoakOptions {
  readonly minutes: number;
  readonly databases: number;
  /** Minute at which the stage is redeployed with production settings, evicting every Durable Object. */
  readonly redeployAtMinute: Option.Option<number>;
  /** Minutes at which the sharded group's shard holding the middle key is split. */
  readonly splitAtMinutes: ReadonlyArray<number>;
  /** Keys per database the writers overwrite. */
  readonly keysPerDatabase: number;
  /**
   * The trace sample rate the stage was deployed with (`preview-cli deploy --trace-sample-rate`),
   * which the midway redeploy keeps. Soak at production's rate (0.1): at 100% a busy stage exports
   * faster than its spans can be read back.
   */
  readonly traceSampleRate: Option.Option<number>;
  /**
   * Where the report is kept up to date while the soak runs (`--report-file`): replaced atomically
   * at least every minute during the workload and after each phase, so a stopped run leaves it.
   */
  readonly reportFile: Option.Option<string>;
  /**
   * The time each final telemetry read (the tail-record catch-up, the health report, the span pull,
   * each span search) may take before it is cut off and recorded as a failure.
   */
  readonly telemetryBudgetMinutes: number;
}

/** How long one read of tail records or spans may take while the workload runs. */
const WATCHER_READ_BUDGET = "5 minutes";

/** How often the soak pulls the stage's spans while the workload runs (D132). */
const SPAN_PULL_INTERVAL = "30 seconds";

const text = new TextDecoder();
const bytesOf = (value: string) => new TextEncoder().encode(value);
const stringOf = (value: Uint8Array) => text.decode(value);

/** A key's acknowledged history, newest last (the last few writes only). */
interface KeyHistory {
  readonly writes: Array<{ readonly seq: number; readonly value: Option.Option<string> }>;
  /** Its history is no longer complete (written more times than kept, or a batch was replayed). */
  truncated: boolean;
}

const HISTORY_KEPT = 6;

/**
 * One database's model of what it must hold: every acknowledged write, the values of the batch in
 * flight, and counters of what was checked.
 */
export class DatabaseModel {
  readonly keys = new Map<string, KeyHistory>();
  readonly pending = new Map<string, Option.Option<string>>();
  /** Every batch sent, for the change-feed follower (D106). */
  readonly ledger = new ChangeLedger();
  /**
   * Keys written only by requests the soak aborted (D106): each holds its value or nothing, for
   * good (the soak writes each such key once).
   */
  readonly maybe = new Map<string, string>();
  lastAckedSeq = 0;
  counterCommitted = 0;
  counterUncertain = 0;
  readonly databaseId: DatabaseId;
  constructor(databaseId: DatabaseId) {
    this.databaseId = databaseId;
  }

  record(key: string, seq: number, value: Option.Option<string>, replayed: boolean) {
    const history = this.keys.get(key) ?? { writes: [], truncated: false };
    history.writes.push({ seq, value });
    if (history.writes.length > HISTORY_KEPT) {
      history.writes.shift();
      history.truncated = true;
    }
    if (replayed) history.truncated = true;
    this.keys.set(key, history);
  }

  current(key: string): Option.Option<Option.Option<string>> {
    const writes = this.keys.get(key)?.writes;
    const last = writes?.[writes.length - 1];
    return last === undefined ? Option.none() : Option.some(last.value);
  }

  /** Whether a read that started when the key's newest acknowledged seq was `fromSeq` may see `value`. */
  accepts(key: string, fromSeq: number, value: Option.Option<string>) {
    const same = (candidate: Option.Option<string>) =>
      Option.getOrNull(candidate) === Option.getOrNull(value);
    const aborted = this.maybe.get(key);
    if (aborted !== undefined) return Option.isNone(value) || same(Option.some(aborted));
    const history = this.keys.get(key);
    if (history === undefined)
      return Option.isNone(value) || same(this.pending.get(key) ?? Option.none());
    const atStart = history.writes.filter((write) => write.seq <= fromSeq).at(-1);
    const candidates = [
      ...(atStart === undefined ? [] : [atStart.value]),
      ...history.writes.filter((write) => write.seq > fromSeq).map((write) => write.value),
      ...Option.toArray(Option.fromUndefinedOr(this.pending.get(key))),
    ];
    if (atStart === undefined && history.truncated) return true;
    return candidates.some(same) || (atStart === undefined && Option.isNone(value));
  }

  /** The value the key held at `seq`, when the kept history knows it. */
  valueAt(key: string, seq: number): Option.Option<Option.Option<string>> {
    const history = this.keys.get(key);
    if (history === undefined || history.truncated) return Option.none();
    const write = history.writes.filter((entry) => entry.seq <= seq).at(-1);
    if (write === undefined)
      return history.writes.length > 0 ? Option.some(Option.none()) : Option.none();
    return Option.some(write.value);
  }

  newestSeq(key: string) {
    return this.keys.get(key)?.writes.at(-1)?.seq ?? 0;
  }
}

/** Latency samples per operation kind, and what failed. */
class SoakLedger {
  readonly latencies = new Map<string, Array<number>>();
  readonly failures: Array<string> = [];
  readonly checks = new Map<string, number>();
  readonly mismatches: Array<string> = [];
  readonly retried = new Map<string, number>();
  /** When each retry, timeout, or failure happened, for finding them in traces and tail records. */
  readonly events: Array<string> = [];
  note(kind: string, what: string) {
    if (this.events.length < 300) this.events.push(`${new Date().toISOString()} ${kind}: ${what}`);
  }
  time(kind: string, ms: number) {
    const list = this.latencies.get(kind) ?? [];
    list.push(ms);
    this.latencies.set(kind, list);
  }
  checked(kind: string) {
    this.checks.set(kind, (this.checks.get(kind) ?? 0) + 1);
  }
  retry(kind: string) {
    this.retried.set(kind, (this.retried.get(kind) ?? 0) + 1);
  }
  mismatch(message: string) {
    if (this.mismatches.length < 50) this.mismatches.push(message);
  }
}

const percentile = (values: ReadonlyArray<number>, fraction: number) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))] ?? 0;
};

/** Times `effect` into the ledger under `kind`. */
const timed = <A, E, R>(ledger: SoakLedger, kind: string, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const started = yield* Clock.currentTimeMillis;
    const result = yield* effect;
    ledger.time(kind, (yield* Clock.currentTimeMillis) - started);
    return result;
  });

/** Whether a failed call may succeed when sent again (a public error says so, or it never got an answer). */
const isRetryable = (error: unknown) =>
  typeof error === "object" &&
  error !== null &&
  (("retryable" in error && error.retryable === true) ||
    ("_tag" in error &&
      (error._tag === "HttpClientError" ||
        error._tag === "SchemaError" ||
        error._tag === "TimeoutError" ||
        // The sharded API's retryable error (its envelope has no `retryable` field).
        error._tag === "ShardedRequestUnavailableError")));

/** A batch larger than the writer's change-feed page of 1,000 rows (D93b). */
const BIG_BATCH_ROWS = 1_500;

/** How long one request may take before the soak counts it as lost and retries it. */
const REQUEST_TIMEOUT = "90 seconds";

const tagOf = (error: unknown) =>
  typeof error === "object" && error !== null && "_tag" in error && typeof error._tag === "string"
    ? error._tag
    : "unknown";

/** The error's tag, with the database error behind it when the sharded API names one. */
const describe = (error: unknown) =>
  typeof error === "object" &&
  error !== null &&
  "reason" in error &&
  typeof error.reason === "string"
    ? `${tagOf(error)} (${error.reason})`
    : tagOf(error);

/**
 * Sends `effect` until it succeeds, waiting a second between tries, for failures that may succeed
 * when retried (each counted under `kind`). Gives up after `attempts`, recording the failure.
 */
const untilSuccess = <A, E, R>(
  ledger: SoakLedger,
  kind: string,
  effect: Effect.Effect<A, E, R>,
  attempts = 120,
) =>
  Effect.gen(function* () {
    for (let attempt = 1; ; attempt++) {
      const result = yield* Effect.result(effect.pipe(Effect.timeout(REQUEST_TIMEOUT)));
      if (result._tag === "Success") return Option.some(result.success);
      if (!isRetryable(result.failure) || attempt >= attempts) {
        ledger.failures.push(`${kind}: ${describe(result.failure)}`);
        ledger.note(kind, `failed: ${describe(result.failure)}`);
        return Option.none<A>();
      }
      ledger.retry(kind);
      ledger.note(kind, `retry ${attempt}: ${describe(result.failure)}`);
      yield* Effect.sleep("1 second");
    }
  });

/** Redeploys the stage with production settings without blocking the soak's fibers. */
const redeploy = (stage: string, traceSampleRate: Option.Option<number>) =>
  Effect.callback<number>((resume) => {
    const child = spawn(
      process.execPath,
      [
        fileURLToPath(new URL("../../preview-cli/src/preview-cli.ts", import.meta.url)),
        "deploy",
        stage,
        ...Option.match(traceSampleRate, {
          onNone: () => [],
          onSome: (rate) => ["--trace-sample-rate", String(rate)],
        }),
      ],
      { cwd: REPO_ROOT, stdio: ["ignore", "ignore", "ignore"] },
    );
    child.on("exit", (code) => resume(Effect.succeed(code ?? -1)));
  });

/**
 * Exceptions the soak's workload provokes on purpose, so their error spans are expected:
 * - `DatabaseSealedError`: a router's stale map sends a key to a shard a split just sealed; it waits
 *   for the new map and retries (D59).
 * - `DatabaseNotFoundError`, `DatabaseRemovedError`: the remover's check that a removed database
 *   no longer answers. For `gcMinAge` after a removal its writer, starting for that check, finds
 *   the removal's tombstone and refuses (D91); the caller is answered `DatabaseNotFound`. Seen on
 *   the first live run of the D106 workload, as the one unexpected error.
 * - `TransactionConflictError`: concurrent increments of one counter; the loser retries.
 * - `WriterFencedError`: the redeploy replaces writers; a request on the old one is retried.
 * - `HttpClientError`: the calling side of one of the above.
 * - `DatabaseHasDependentsError`, `DatabaseRecentlyRemovedError`: the fork-hold and recreate
 *   checks (D106) provoke them on purpose.
 * Any other exception in the soak's window fails it.
 */
const EXPECTED_EXCEPTIONS = new Set([
  // D106: the refused deletion of a fork's hold, and the refused recreation of a removed id.
  "DatabaseHasDependentsError",
  "DatabaseRecentlyRemovedError",
  "DatabaseSealedError",
  "DatabaseNotFoundError",
  "DatabaseRemovedError",
  "TransactionConflictError",
  "WriterFencedError",
  "HttpClientError",
]);

const StorageReport = Schema.Struct({
  kinds: Schema.Record(
    Schema.String,
    Schema.Struct({
      objects: Schema.Number,
      bytes: Schema.Number,
      older: Schema.Number,
      olderBytes: Schema.Number,
    }),
  ),
});
const decodeStorageReport = Schema.decodeUnknownEffect(Schema.fromJsonString(StorageReport));

/**
 * The soak (v1 workstream 10, D65): runs a mixed workload against several databases and a sharded
 * group for `minutes`, with splits, checkpoints, pinned snapshots, removals, compaction, GC, and a
 * redeploy midway, while a model records every acknowledged write. At the end it reads every key
 * back at `Latest` and `Flushed`, checks counters, snapshots, and storage, and summarizes the
 * stage's health.
 *
 * @returns The report: `passed`, what failed, latencies, and the checks made.
 */
export const soak = (options: SoakOptions) =>
  Effect.gen(function* () {
    const stack = yield* ScenarioStack;
    const http = yield* HttpClient.HttpClient;
    const api = yield* HttpApiClient.makeWith(DbHttpApi, { baseUrl: stack.api, httpClient: http });
    const gcApi = yield* HttpApiClient.makeWith(GcHttpApi, { baseUrl: stack.gc, httpClient: http });
    const shards = yield* HttpApiClient.makeWith(ShardedHttpApi, {
      baseUrl: stack.shards,
      httpClient: http,
    });
    const ledger = new SoakLedger();
    const startedAt = yield* Clock.currentTimeMillis;
    const deadline = startedAt + options.minutes * 60_000;
    const running = Effect.map(Clock.currentTimeMillis, (now) => now < deadline);
    /** Sleeps for `ms`, or until the deadline when that comes first: loops end on time. */
    const pause = (ms: number) =>
      Effect.flatMap(Clock.currentTimeMillis, (now) =>
        Effect.sleep(`${Math.max(0, Math.min(ms, deadline - now))} millis`),
      );
    const prefix = uniqueId("soak");
    const models = Array.from(
      { length: options.databases },
      (_, index) => new DatabaseModel(DatabaseId.make(`${prefix}-${index}`)),
    );
    const keyOf = (index: number) => `k${index.toString().padStart(6, "0")}`;
    const COUNTER = "counter";

    // Every soak database keeps its changelog longer than the soak runs, so a follower that keeps
    // up is never behind retention: `Expired` is a failure (D106).
    const changelogRetentionMs = Math.min(30 * 24 * 3_600_000, (options.minutes + 60) * 60_000);
    const createWith = (
      databaseId: DatabaseId,
      extra: { readonly changelog?: boolean; readonly concat?: boolean } = {},
    ) =>
      untilSuccess(
        ledger,
        "create",
        api.databases.createDatabase({
          payload: {
            databaseId,
            keyRange: Option.none(),
            mergeOperator: extra.concat === true ? Option.some("Concat" as const) : Option.none(),
            changelogRetentionMs:
              extra.changelog === true ? Option.some(changelogRetentionMs) : Option.none(),
            budgetGroup: Option.none(),
          },
        }),
      );
    for (const model of models) yield* createWith(model.databaseId, { changelog: true });

    let abortedWrites = 0;
    /**
     * Sends one small batch on keys no other write touches, and aborts it after a few milliseconds
     * (the fetch is cancelled; the writer may still commit it). The model then holds either value
     * for those keys, and the feed may show the batch once (D106).
     */
    const abortedWrite = (model: DatabaseModel) =>
      Effect.gen(function* () {
        const round = abortedWrites++;
        const changes: Array<SoakChange> = [0, 1, 2].map((index) => ({
          _tag: "Put",
          key: `u${round.toString().padStart(6, "0")}-${index}`,
          value: `aborted-${round}-${index}`,
        }));
        for (const change of changes)
          if (change._tag === "Put") model.maybe.set(change.key, change.value);
        model.ledger.maybe(changes);
        const sent = yield* Effect.result(
          api.databases
            .write({
              params: { databaseId: model.databaseId },
              payload: {
                operations: changes.map((change) => ({
                  _tag: "Put" as const,
                  key: bytesOf(change.key),
                  value: bytesOf(change._tag === "Put" ? change.value : ""),
                  ttlMillis: Option.none(),
                })),
              },
            })
            .pipe(Effect.timeout(`${2 + Math.floor(Math.random() * 20)} millis`)),
        );
        if (sent._tag === "Success") {
          model.ledger.ack(sent.success.seq, changes);
          ledger.checked("write.aborted_landed");
        } else ledger.checked("write.aborted");
      });

    /** Writes batches to one database: puts and deletes on its keys, each acknowledged write recorded. */
    const writer = (model: DatabaseModel) =>
      Effect.gen(function* () {
        let counter = 0;
        let batches = 0;
        while (yield* running) {
          batches += 1;
          // Now and then a batch larger than the writer's 1,000-row page (D93b), on its own keys.
          const big = batches % 60 === 0;
          const size = big ? BIG_BATCH_ROWS : 20 + Math.floor(Math.random() * 120);
          const chosen = new Set<number>();
          if (big) for (let index = 0; index < size; index++) chosen.add(index);
          while (chosen.size < size)
            chosen.add(Math.floor(Math.random() * options.keysPerDatabase));
          const operations = [...chosen].map((index) => {
            const key = big ? `big${index.toString().padStart(5, "0")}` : keyOf(index);
            counter += 1;
            const value =
              Math.random() < 0.1
                ? Option.none<string>()
                : Option.some(`v${counter}-${"x".repeat(64 + (counter % 128))}`);
            model.pending.set(key, value);
            return { key, value };
          });
          const changes = operations.map(({ key, value }): SoakChange =>
            Option.match(value, {
              onNone: () => ({ _tag: "Delete", key }),
              onSome: (text) => ({ _tag: "Put", key, value: text }),
            }),
          );
          if (big) ledger.checked("write.big_batch");
          let replayed = false;
          const receipt = yield* untilSuccess(
            ledger,
            "write",
            timed(
              ledger,
              "write",
              api.databases.write({
                params: { databaseId: model.databaseId },
                payload: {
                  operations: operations.map(({ key, value }) =>
                    Option.match(value, {
                      onNone: () => ({ _tag: "Delete" as const, key: bytesOf(key) }),
                      onSome: (text) => ({
                        _tag: "Put" as const,
                        key: bytesOf(key),
                        value: bytesOf(text),
                        ttlMillis: Option.none(),
                      }),
                    }),
                  ),
                },
              }),
            ).pipe(
              // A failed attempt may have committed: the feed may show it once (D106).
              Effect.tapError(() =>
                Effect.sync(() => {
                  replayed = true;
                  model.ledger.maybe(changes);
                }),
              ),
            ),
          );
          if (Option.isSome(receipt)) {
            for (const { key, value } of operations)
              model.record(key, receipt.value.seq, value, replayed);
            model.lastAckedSeq = Math.max(model.lastAckedSeq, receipt.value.seq);
            model.ledger.ack(receipt.value.seq, changes);
          }
          for (const { key } of operations) model.pending.delete(key);
          // One write in fifty is aborted by the client mid-request (D106; D74b's and D85's
          // class): on its own keys, written once, so each holds its value or nothing.
          if (Math.random() < 0.02) yield* abortedWrite(model);
          yield* Effect.sleep("700 millis");
        }
      });

    /** Reads random keys at every consistency and checks each against the model. */
    const reader = (model: DatabaseModel) =>
      Effect.gen(function* () {
        while (yield* running) {
          const key = keyOf(Math.floor(Math.random() * options.keysPerDatabase));
          const fromSeq = model.newestSeq(key);
          const roll = Math.random();
          const consistency =
            roll < 0.6
              ? ({ _tag: "Latest" } as const)
              : roll < 0.85
                ? ({ _tag: "AtLeast", seq: SeqNum.make(model.lastAckedSeq) } as const)
                : ({ _tag: "Flushed" } as const);
          const got = yield* untilSuccess(
            ledger,
            "get",
            timed(
              ledger,
              `get.${consistency._tag}`,
              api.databases.get({
                params: { databaseId: model.databaseId },
                payload: { key: bytesOf(key), consistency, snapshot: Option.none() },
              }),
            ),
          );
          if (Option.isSome(got) && consistency._tag !== "Flushed") {
            const value = Option.flatMap(got.value.row, (row) =>
              row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none(),
            );
            ledger.checked(`get.${consistency._tag}`);
            if (!model.accepts(key, fromSeq, value)) {
              ledger.mismatch(
                `${model.databaseId} ${key} at ${consistency._tag}: read ${Option.getOrNull(value)?.slice(0, 12)}`,
              );
            }
          }
          yield* Effect.sleep("250 millis");
        }
      });

    /** Scans a random range at Latest, checking every row returned against the model. */
    const scanner = (model: DatabaseModel) =>
      Effect.gen(function* () {
        while (yield* running) {
          const from = Math.floor(Math.random() * options.keysPerDatabase);
          const result = yield* untilSuccess(
            ledger,
            "scan",
            timed(
              ledger,
              "scan",
              api.databases.scan({
                params: { databaseId: model.databaseId },
                payload: {
                  range: {
                    start: bytesOf(keyOf(from)),
                    end: Option.some(bytesOf(keyOf(from + 200))),
                  },
                  consistency: { _tag: "Latest" },
                  snapshot: Option.none(),
                  limit: 100,
                },
              }),
            ),
          );
          if (Option.isSome(result)) {
            ledger.checked("scan");
            for (const row of result.value.rows) {
              const key = stringOf(row.key);
              if (key === COUNTER) continue;
              const value =
                row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none();
              if (!model.accepts(key, 0, value)) ledger.mismatch(`${model.databaseId} scan ${key}`);
            }
          }
          yield* Effect.sleep("5 seconds");
        }
      });

    /** Increments one counter per database in SerializableSnapshot transactions. */
    const transactor = (model: DatabaseModel) =>
      Effect.gen(function* () {
        while (yield* running) {
          const begun = yield* untilSuccess(
            ledger,
            "transaction",
            api.databases.beginTransaction({ params: { databaseId: model.databaseId } }),
          );
          if (Option.isNone(begun)) continue;
          const read = yield* untilSuccess(
            ledger,
            "transaction",
            api.databases.transactionGet({
              params: { databaseId: model.databaseId },
              payload: {
                readSeq: begun.value.readSeq,
                manifestId: begun.value.manifestId,
                key: bytesOf(COUNTER),
              },
            }),
          );
          if (Option.isNone(read)) continue;
          const current = Option.match(read.value.row, {
            onNone: () => 0,
            onSome: (row) => (row.value._tag === "Value" ? Number(stringOf(row.value.bytes)) : 0),
          });
          const change: SoakChange = { _tag: "Put", key: COUNTER, value: String(current + 1) };
          // One commit in twenty is aborted by the client after a few milliseconds (D106): its
          // outcome is unknown, like a commit that timed out.
          const aborted = Math.random() < 0.05;
          const commit = yield* Effect.result(
            Effect.timeout(
              aborted ? `${2 + Math.floor(Math.random() * 20)} millis` : REQUEST_TIMEOUT,
            )(
              timed(
                ledger,
                "transaction.commit",
                api.databases.commitTransaction({
                  params: { databaseId: model.databaseId },
                  payload: {
                    readSeq: begun.value.readSeq,
                    isolation: "SerializableSnapshot",
                    batch: {
                      operations: [
                        {
                          _tag: "Put",
                          key: bytesOf(COUNTER),
                          value: bytesOf(String(current + 1)),
                          ttlMillis: Option.none(),
                        },
                      ],
                    },
                    readKeys: [bytesOf(COUNTER)],
                    readRanges: [],
                  },
                }),
              ),
            ),
          );
          if (commit._tag === "Success") {
            model.counterCommitted += 1;
            model.ledger.ack(commit.success.seq, [change]);
            ledger.checked(aborted ? "transaction.aborted_landed" : "transaction.commit");
          } else if (tagOf(commit.failure) === "TransactionConflictPublicError") {
            ledger.checked("transaction.conflict");
          } else if (isRetryable(commit.failure)) {
            model.counterUncertain += 1;
            model.ledger.maybe([change]);
            if (aborted) ledger.checked("transaction.aborted");
            ledger.retry("transaction.commit");
            ledger.note("transaction.commit", `uncertain: ${tagOf(commit.failure)}`);
          } else {
            ledger.failures.push(`transaction.commit: ${tagOf(commit.failure)}`);
          }
          yield* Effect.sleep("2 seconds");
        }
      });

    /** Pins a snapshot every few minutes and reads sampled keys at it later. */
    const snapshotter = (model: DatabaseModel) =>
      Effect.gen(function* () {
        yield* pause(3 * 60_000);
        while (yield* running) {
          const pinned = yield* untilSuccess(
            ledger,
            "pinSnapshot",
            api.databases.pinSnapshot({
              params: { databaseId: model.databaseId },
              payload: { lifetimeMs: 20 * 60_000 },
            }),
          );
          if (Option.isSome(pinned)) {
            const seq = pinned.value.snapshot.snapshotSeq;
            const sample = Array.from({ length: 30 }, () =>
              keyOf(Math.floor(Math.random() * options.keysPerDatabase)),
            );
            const expected = sample.flatMap((key) =>
              Option.match(model.valueAt(key, seq), {
                onNone: () => [],
                onSome: (value) => [{ key, value }],
              }),
            );
            yield* pause(4 * 60_000);
            for (const { key, value } of expected) {
              const got = yield* untilSuccess(
                ledger,
                "get.snapshot",
                timed(
                  ledger,
                  "get.Snapshot",
                  api.databases.get({
                    params: { databaseId: model.databaseId },
                    payload: {
                      key: bytesOf(key),
                      consistency: { _tag: "Latest" },
                      snapshot: Option.some(pinned.value.snapshot),
                    },
                  }),
                ),
              );
              if (Option.isNone(got)) continue;
              const read = Option.flatMap(got.value.row, (row) =>
                row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none(),
              );
              ledger.checked("get.Snapshot");
              if (Option.getOrNull(read) !== Option.getOrNull(value)) {
                ledger.mismatch(`${model.databaseId} ${key} at snapshot ${seq}`);
              }
            }
            yield* untilSuccess(
              ledger,
              "deleteCheckpoint",
              api.databases.deleteCheckpoint({
                params: { databaseId: model.databaseId, checkpointId: pinned.value.checkpointId },
              }),
            );
          }
          yield* pause(6 * 60_000);
        }
      });

    /** Creates, uses, and removes short-lived databases. */
    const remover = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        yield* pause(9 * 60_000);
        if (!(yield* running)) break;
        const databaseId = DatabaseId.make(`${prefix}-tmp-${round++}`);
        yield* untilSuccess(
          ledger,
          "create",
          api.databases.createDatabase({
            payload: {
              databaseId,
              keyRange: Option.none(),
              mergeOperator: Option.none(),
              changelogRetentionMs: Option.none(),
              budgetGroup: Option.none(),
            },
          }),
        );
        yield* untilSuccess(
          ledger,
          "write",
          api.databases.write({
            params: { databaseId },
            payload: {
              operations: [
                { _tag: "Put", key: bytesOf("a"), value: bytesOf("1"), ttlMillis: Option.none() },
              ],
            },
          }),
        );
        yield* untilSuccess(ledger, "flush", api.databases.flush({ params: { databaseId } }));
        yield* untilSuccess(
          ledger,
          "remove",
          timed(ledger, "remove", api.databases.removeDatabase({ params: { databaseId } })),
        );
        const after = yield* Effect.result(api.databases.status({ params: { databaseId } }));
        ledger.checked("remove");
        if (after._tag === "Success" || tagOf(after.failure) !== "DatabaseNotFoundPublicError") {
          ledger.mismatch(`${databaseId} still answers after removal`);
        }
        // Recreating the id within gcMinAge of its removal is refused [D90, on audit-lifecycle's
        // branch: before it merges, the create succeeds and this check reports it]. Sent raw, so
        // the answer is read whatever this build's client knows of the error.
        const recreated = yield* http
          .execute(
            HttpClientRequest.post(`${stack.api}/v1/databases`).pipe(
              HttpClientRequest.bodyJsonUnsafe({
                databaseId,
                keyRange: null,
                mergeOperator: null,
                changelogRetentionMs: null,
                budgetGroup: null,
              }),
            ),
          )
          .pipe(
            Effect.flatMap((response) =>
              Effect.map(response.text, (body) => ({ status: response.status, body })),
            ),
            Effect.timeout(REQUEST_TIMEOUT),
            Effect.result,
          );
        ledger.checked("remove.recreate_refused (needs D90)");
        if (recreated._tag === "Failure") {
          ledger.failures.push(`remove.recreate: ${describe(recreated.failure)}`);
        } else if (
          recreated.success.status !== 409 ||
          !recreated.success.body.includes("DatabaseRecentlyRemoved")
        ) {
          ledger.mismatch(
            `${databaseId}: recreating it right after its removal answered ${recreated.success.status}, not 409 DatabaseRecentlyRemoved (D90)`,
          );
        }
      }
    });

    // ---------------------------------------------------------------------------------------------
    // D106: workloads for the classes of defect today's audits found. Each is checked against a
    // model and counted in `checks`; each stops with the workload.
    // ---------------------------------------------------------------------------------------------

    /** A feed row's signature, for the follower. */
    const feedRow = (row: {
      readonly key: Uint8Array;
      readonly seq: number;
      readonly value:
        | { readonly _tag: "Value"; readonly bytes: Uint8Array }
        | { readonly _tag: "Merge"; readonly operand: Uint8Array }
        | { readonly _tag: "Tombstone" };
    }) => ({
      seq: row.seq,
      signature: rowSignature(
        stringOf(row.key),
        row.value._tag,
        row.value._tag === "Value"
          ? stringOf(row.value.bytes)
          : row.value._tag === "Merge"
            ? stringOf(row.value.operand)
            : "",
      ),
    });

    /**
     * Reads one page of `databaseId`'s change feed into `follower`, recording what is wrong.
     * `Expired` and `Disabled` are failures: every soak database keeps its changelog longer than the
     * soak runs. @returns The number of rows read, or none when the page could not be read.
     */
    const followOnce = (databaseId: DatabaseId, follower: FeedFollower) =>
      Effect.gen(function* () {
        const page = yield* untilSuccess(
          ledger,
          "feed",
          timed(
            ledger,
            "feed",
            api.databases.changes({
              params: { databaseId },
              query: { afterSeq: SeqNum.make(follower.cursor), limit: 500 },
            }),
          ),
        );
        if (Option.isNone(page)) return Option.none<number>();
        for (const problem of follower.page({
          rows: page.value.rows.map(feedRow),
          nextAfterSeq: page.value.nextAfterSeq,
        }))
          ledger.mismatch(`${databaseId} feed: ${problem}`);
        ledger.checked("feed.page");
        return Option.some(page.value.rows.length);
      });

    /** Checks every acknowledged batch at or below the follower's cursor was delivered (once each). */
    const reportedMissing = new Set<string>();
    const checkFeed = (databaseId: DatabaseId, follower: FeedFollower, ledgerOf: ChangeLedger) => {
      for (const problem of follower.missing(ledgerOf)) {
        const message = `${databaseId} feed: ${problem}`;
        if (reportedMissing.has(message)) continue;
        reportedMissing.add(message);
        ledger.mismatch(message);
      }
      ledger.checked("feed.delivered");
    };

    /** One follower per soak database, for the whole workload (stopped with it). */
    const followers = models.map(() => new FeedFollower(0));
    const feedFollower = (model: DatabaseModel, follower: FeedFollower) =>
      Effect.gen(function* () {
        let pages = 0;
        for (;;) {
          const read = yield* followOnce(model.databaseId, follower);
          pages += 1;
          if (pages % 60 === 0) checkFeed(model.databaseId, follower, model.ledger);
          // Keep up when behind, rest when caught up.
          if (Option.getOrElse(read, () => 0) < 500) yield* Effect.sleep("2 seconds");
        }
      });

    // The documented way to follow a database through a split (D80): a database with a feed is
    // sealed, checkpointed, and forked into two successors partway through; its follower reads the
    // sealed source to its end, then each successor from the source's last sequence number.
    const splitSource = new DatabaseModel(DatabaseId.make(`${prefix}-feedsplit`));
    const splitLeft = new DatabaseModel(DatabaseId.make(`${prefix}-feedsplit-l`));
    const splitRight = new DatabaseModel(DatabaseId.make(`${prefix}-feedsplit-r`));
    const SPLIT_AT = "m";
    yield* createWith(splitSource.databaseId, { changelog: true });
    let splitDone = false;
    let splitFromSeq = 0;
    const splitFollowers = {
      source: new FeedFollower(0),
      left: Option.none<FeedFollower>(),
      right: Option.none<FeedFollower>(),
    };
    const splitWriter = Effect.gen(function* () {
      let round = 0;
      const splitAtMs = startedAt + Math.max(2, options.minutes * 0.4) * 60_000;
      while (yield* running) {
        if (!splitDone && (yield* Clock.currentTimeMillis) >= splitAtMs) {
          // Seal, checkpoint, fork both halves, release the caller's checkpoint.
          const sealed = yield* untilSuccess(
            ledger,
            "feedsplit.seal",
            api.databases.seal({
              params: { databaseId: splitSource.databaseId },
              payload: { successors: [splitLeft.databaseId, splitRight.databaseId] },
            }),
          );
          const checkpoint = yield* untilSuccess(
            ledger,
            "feedsplit.checkpoint",
            api.databases.createCheckpoint({
              params: { databaseId: splitSource.databaseId },
              payload: { name: Option.some("feed split"), lifetimeMs: Option.none() },
            }),
          );
          if (Option.isNone(sealed) || Option.isNone(checkpoint)) return;
          for (const [successor, range] of [
            [splitLeft, { start: new Uint8Array(0), end: Option.some(bytesOf(SPLIT_AT)) }],
            [splitRight, { start: bytesOf(SPLIT_AT), end: Option.none() }],
          ] as const) {
            yield* untilSuccess(
              ledger,
              "feedsplit.fork",
              api.databases.fork({
                params: { databaseId: splitSource.databaseId },
                payload: { forkId: successor.databaseId, checkpointId: checkpoint.value.id, range },
              }),
            );
          }
          yield* untilSuccess(
            ledger,
            "feedsplit.deleteCheckpoint",
            api.databases.deleteCheckpoint({
              params: { databaseId: splitSource.databaseId, checkpointId: checkpoint.value.id },
            }),
          );
          splitFromSeq = sealed.value.flushedSeq;
          splitDone = true;
          ledger.checked("feedsplit.split");
        }
        // Five keys a batch, on one side of the split point; after the split, sent to that side.
        const right = Math.random() < 0.5;
        const target = !splitDone ? splitSource : right ? splitRight : splitLeft;
        const changes: Array<SoakChange> = Array.from({ length: 5 }, (_, index) => ({
          _tag: "Put",
          key: `${right ? "r" : "a"}${((round * 5 + index) % 400).toString().padStart(4, "0")}`,
          value: `s${round}-${index}`,
        }));
        round += 1;
        const receipt = yield* untilSuccess(
          ledger,
          "feedsplit.write",
          api.databases
            .write({
              params: { databaseId: target.databaseId },
              payload: {
                operations: changes.map((change) => ({
                  _tag: "Put" as const,
                  key: bytesOf(change.key),
                  value: bytesOf(change._tag === "Put" ? change.value : ""),
                  ttlMillis: Option.none(),
                })),
              },
            })
            .pipe(Effect.tapError(() => Effect.sync(() => target.ledger.maybe(changes)))),
        );
        if (Option.isSome(receipt)) target.ledger.ack(receipt.value.seq, changes);
        yield* Effect.sleep("1 second");
      }
    });
    const splitFollower = Effect.gen(function* () {
      for (;;) {
        // The source to its end: an empty page once it is sealed.
        if (Option.isNone(splitFollowers.left)) {
          const read = yield* followOnce(splitSource.databaseId, splitFollowers.source);
          if (splitDone && Option.exists(read, (rows) => rows === 0)) {
            const status = yield* untilSuccess(
              ledger,
              "feedsplit.status",
              api.databases.status({ params: { databaseId: splitSource.databaseId } }),
            );
            if (Option.isSome(status) && status.value.sealed) {
              // The successors' feeds start at the source's last sequence number.
              if (status.value.lastSeq !== splitFromSeq)
                ledger.mismatch(
                  `${splitSource.databaseId} sealed at ${splitFromSeq}, status says ${status.value.lastSeq}`,
                );
              splitFollowers.left = Option.some(new FeedFollower(status.value.lastSeq));
              splitFollowers.right = Option.some(new FeedFollower(status.value.lastSeq));
              ledger.checked("feedsplit.source_ended");
            }
          }
        } else {
          for (const [successor, follower] of [
            [splitLeft, splitFollowers.left],
            [splitRight, splitFollowers.right],
          ] as const) {
            if (Option.isSome(follower)) yield* followOnce(successor.databaseId, follower.value);
          }
        }
        yield* Effect.sleep("2 seconds");
      }
    });

    // Merge operands with TTLs (D81): three Concat keys built from a base and one-character
    // operands, some expiring after 10 to 40 s, read back at Latest after they expire and after the
    // database's flushes and compactions.
    /**
     * How far the soak's clock and the database's may disagree around an expiry (and a read's own
     * duration is added). Cloudflare's machines keep NTP time within milliseconds; the laptop
     * running the soak is usually within a second of it. 5 s covers both, and a read whose time
     * falls inside the window around an expiry accepts the value either side of it.
     */
    const CLOCK_SKEW_MS = 5_000;
    const mergeDatabase = DatabaseId.make(`${prefix}-merge`);
    yield* createWith(mergeDatabase, { concat: true });
    const mergeKeys = ["m0", "m1", "m2"].map((name) => ({ name, model: new MergeKeyModel() }));
    const mergeWriter = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        const target = mergeKeys[round % mergeKeys.length];
        round += 1;
        if (target === undefined) continue;
        // A new base every so often, or when the key's state is unknown or long.
        const rebase =
          target.model.unknown || target.model.operands.length >= 24 || round % 20 === 0;
        const ttlMillis =
          rebase || Math.random() < 0.5
            ? Option.none<number>()
            : Option.some(10_000 + Math.floor(Math.random() * 30_000));
        const payload = rebase ? `B${round}.` : String.fromCharCode(97 + (round % 26));
        let failed = false;
        const receipt = yield* untilSuccess(
          ledger,
          "merge.write",
          api.databases
            .write({
              params: { databaseId: mergeDatabase },
              payload: {
                operations: [
                  rebase
                    ? {
                        _tag: "Put" as const,
                        key: bytesOf(target.name),
                        value: bytesOf(payload),
                        ttlMillis,
                      }
                    : {
                        _tag: "Merge" as const,
                        key: bytesOf(target.name),
                        operand: bytesOf(payload),
                        ttlMillis,
                      },
                ],
              },
            })
            .pipe(Effect.tapError(() => Effect.sync(() => (failed = true)))),
        );
        if (Option.isSome(receipt)) {
          const expireAt = Option.getOrUndefined(
            Option.map(ttlMillis, (ttl) => receipt.value.createTs + ttl),
          );
          // A merge sent twice (a retry after a lost answer) may have applied twice: unknown until
          // the next base. A base put twice is the same base.
          if (rebase) target.model.put(receipt.value.seq, payload, expireAt);
          else if (failed) target.model.unknown = true;
          else target.model.merge(receipt.value.seq, payload, expireAt);
        } else target.model.unknown = true;
        if (round % 2 === 0) yield* readMergeKeys;
        // Flushes often, so operands meet compaction (four flushes make a compaction).
        if (round % 10 === 0)
          yield* untilSuccess(
            ledger,
            "merge.flush",
            api.databases.flush({ params: { databaseId: mergeDatabase } }),
          );
        yield* Effect.sleep("1500 millis");
      }
    });
    /** Reads every merge key once (from the writer's fiber: no merge is in flight meanwhile). */
    const readMergeKeys = Effect.gen(function* () {
      {
        for (const target of mergeKeys) {
          const before = yield* Clock.currentTimeMillis;
          const got = yield* untilSuccess(
            ledger,
            "merge.get",
            api.databases.get({
              params: { databaseId: mergeDatabase },
              payload: {
                key: bytesOf(target.name),
                consistency: { _tag: "Latest" },
                snapshot: Option.none(),
              },
            }),
          );
          const after = yield* Clock.currentTimeMillis;
          if (Option.isNone(got)) continue;
          const acceptable = target.model.acceptable(before, after, CLOCK_SKEW_MS);
          if (acceptable === undefined) {
            if (target.model.unknown) ledger.checked("merge.not_judged");
            else
              ledger.mismatch(
                `${mergeDatabase} ${target.name}: cannot decide, too many operands expire near the read`,
              );
            continue;
          }
          const read = Option.match(got.value.row, {
            onNone: () => "",
            onSome: (row) =>
              row.value._tag === "Value" ? stringOf(row.value.bytes) : `<${row.value._tag}>`,
          });
          ledger.checked("merge.get");
          if (
            target.model.operands.some(
              (operand) => operand.expireAt !== undefined && operand.expireAt < before,
            )
          )
            ledger.checked("merge.get_after_expiry");
          if (!acceptable.has(read))
            ledger.mismatch(
              `${mergeDatabase} ${target.name}: read ${JSON.stringify(read.slice(0, 40))}, expected one of ${JSON.stringify([...acceptable].map((value) => value.slice(0, 40)))}`,
            );
        }
      }
    });

    // Scans with small limits continued by `resumeFrom` at Latest while flushes run (D93): across
    // one traversal no key repeats or comes out of order, every value is one the model allows, and a
    // key that held a value throughout is returned.
    const pager = (model: DatabaseModel) =>
      Effect.gen(function* () {
        while (yield* running) {
          const from = Math.floor(Math.random() * Math.max(1, options.keysPerDatabase - 300));
          const startKey = keyOf(from);
          const endKey = keyOf(from + 300);
          const inRange = [...model.keys.keys()].filter((key) => key >= startKey && key < endKey);
          const startSeqs = new Map(inRange.map((key) => [key, model.newestSeq(key)]));
          const traversal = new ScanTraversal();
          let start: Uint8Array = bytesOf(startKey);
          let complete = false;
          for (let page = 0; page < 200; page++) {
            const read = yield* untilSuccess(
              ledger,
              "scan.paged",
              api.databases.scan({
                params: { databaseId: model.databaseId },
                payload: {
                  range: { start, end: Option.some(bytesOf(endKey)) },
                  consistency: { _tag: "Latest" },
                  snapshot: Option.none(),
                  limit: 7,
                },
              }),
            );
            if (Option.isNone(read)) break;
            const keys = read.value.rows.map((row) => stringOf(row.key));
            for (const problem of traversal.page(keys))
              ledger.mismatch(`${model.databaseId} paged scan: ${problem}`);
            for (const row of read.value.rows) {
              const key = stringOf(row.key);
              const value =
                row.value._tag === "Value"
                  ? Option.some(stringOf(row.value.bytes))
                  : Option.none<string>();
              if (!model.accepts(key, startSeqs.get(key) ?? 0, value))
                ledger.mismatch(
                  `${model.databaseId} paged scan ${key}: read ${Option.getOrNull(value)?.slice(0, 12)}`,
                );
            }
            if (read.value.rows.length < 7 && Option.isSome(read.value.resumeFrom))
              ledger.checked("scan.paged_early_end");
            if (Option.isNone(read.value.resumeFrom)) {
              complete = true;
              break;
            }
            start = read.value.resumeFrom.value;
          }
          if (!complete) {
            ledger.mismatch(
              `${model.databaseId} paged scan of ${startKey}.. did not finish in 200 pages`,
            );
          } else {
            // Held a value before the scan and was not written during it (nor in flight).
            const stable = inRange.filter(
              (key) =>
                model.newestSeq(key) === startSeqs.get(key) &&
                !model.pending.has(key) &&
                Option.exists(model.current(key), Option.isSome),
            );
            for (const problem of traversal.skipped(stable))
              ledger.mismatch(`${model.databaseId} paged scan: ${problem}`);
            ledger.checked("scan.paged");
          }
          yield* Effect.sleep("20 seconds");
        }
      });

    // Bursts of explicit flushes on a database with few rows (what found D78): eight writes, each
    // flushed at once, every four minutes; ninety seconds later its compactor must not be fenced
    // and L0 must be below the write limit.
    const burstDatabase = DatabaseId.make(`${prefix}-burst`);
    yield* createWith(burstDatabase);
    const burster = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        for (let index = 0; index < 8; index++) {
          yield* untilSuccess(
            ledger,
            "burst.write",
            api.databases.write({
              params: { databaseId: burstDatabase },
              payload: {
                operations: [
                  {
                    _tag: "Put",
                    key: bytesOf(`b${index}`),
                    value: bytesOf(`${round}`),
                    ttlMillis: Option.none(),
                  },
                ],
              },
            }),
          );
          yield* untilSuccess(
            ledger,
            "burst.flush",
            api.databases.flush({ params: { databaseId: burstDatabase } }),
          );
        }
        round += 1;
        ledger.checked("burst.flushes");
        yield* pause(90_000);
        const status = yield* untilSuccess(
          ledger,
          "burst.compaction",
          api.databases.compaction({ params: { databaseId: burstDatabase } }),
        );
        if (Option.isSome(status)) {
          ledger.checked("burst.compactor");
          if (Option.exists(status.value.compactor, (compactor) => compactor.fenced))
            ledger.mismatch(`${burstDatabase}: its compactor is fenced after a flush burst (D78)`);
          if (status.value.l0Ssts >= 16)
            ledger.mismatch(`${burstDatabase}: ${status.value.l0Ssts} L0 SSTs after a flush burst`);
        }
        yield* pause(150_000);
      }
    });

    // A database written once and then only read (D76's shape): four flushed batches (one
    // compaction), then a Latest get every 20 s (which keeps its writer in memory) and a
    // transaction every 3 minutes, each checked against what was written. D76 needs the
    // compaction's replaced manifest collected (gcMinAge, 1 h, after the compaction) while the writer
    // stays up: a soak of more than about 65 minutes reaches it; a shorter one checks the rest.
    const idleDatabase = DatabaseId.make(`${prefix}-idle`);
    yield* createWith(idleDatabase);
    const idleValues = new Map<string, string>();
    for (let batch = 0; batch < 4; batch++) {
      const operations = Array.from({ length: 10 }, (_, index) => {
        const key = `i${(batch * 10 + index).toString().padStart(3, "0")}`;
        idleValues.set(key, `idle-${batch}-${index}`);
        return {
          _tag: "Put" as const,
          key: bytesOf(key),
          value: bytesOf(`idle-${batch}-${index}`),
          ttlMillis: Option.none(),
        };
      });
      yield* untilSuccess(
        ledger,
        "idle.write",
        api.databases.write({ params: { databaseId: idleDatabase }, payload: { operations } }),
      );
      yield* untilSuccess(
        ledger,
        "idle.flush",
        api.databases.flush({ params: { databaseId: idleDatabase } }),
      );
    }
    const idleKeys = [...idleValues.keys()];
    const idleReader = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        const key = idleKeys[Math.floor(Math.random() * idleKeys.length)] ?? "i000";
        const got = yield* untilSuccess(
          ledger,
          "idle.get",
          api.databases.get({
            params: { databaseId: idleDatabase },
            payload: {
              key: bytesOf(key),
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
            },
          }),
        );
        if (Option.isSome(got)) {
          ledger.checked("idle.get");
          const read = Option.flatMap(got.value.row, (row) =>
            row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none(),
          );
          if (Option.getOrNull(read) !== (idleValues.get(key) ?? null))
            ledger.mismatch(`${idleDatabase} ${key}: read ${Option.getOrNull(read)}`);
        }
        round += 1;
        if (round % 9 === 0) {
          const begun = yield* untilSuccess(
            ledger,
            "idle.transaction",
            api.databases.beginTransaction({ params: { databaseId: idleDatabase } }),
          );
          if (Option.isSome(begun)) {
            for (const sample of idleKeys.slice(0, 5)) {
              const read = yield* untilSuccess(
                ledger,
                "idle.transaction",
                api.databases.transactionGet({
                  params: { databaseId: idleDatabase },
                  payload: {
                    readSeq: begun.value.readSeq,
                    manifestId: begun.value.manifestId,
                    key: bytesOf(sample),
                  },
                }),
              );
              if (Option.isNone(read)) continue;
              const value = Option.flatMap(read.value.row, (row) =>
                row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none(),
              );
              if (Option.getOrNull(value) !== (idleValues.get(sample) ?? null))
                ledger.mismatch(
                  `${idleDatabase} transaction read of ${sample}: ${Option.getOrNull(value)}`,
                );
            }
            ledger.checked(
              (yield* Clock.currentTimeMillis) - startedAt > 65 * 60_000
                ? "idle.transaction_after_gc_age"
                : "idle.transaction",
            );
          }
        }
        yield* pause(20_000);
      }
    });

    // A fork from a checkpoint early in the soak (D77): the source keeps changing, the fork must
    // keep the state at the checkpoint. The caller's checkpoint is deleted after the fork; deleting
    // the fork's hold must be refused [needs audit-lifecycle's hold fix (its STATUS.md, 01:06):
    // before it, the deletion succeeds and the check reports it].
    const forkSource = DatabaseId.make(`${prefix}-forksrc`);
    const forkCopy = DatabaseId.make(`${prefix}-fork`);
    yield* createWith(forkSource);
    const forkValues = new Map<string, string>();
    const forkOperations = Array.from({ length: 200 }, (_, index) => {
      const key = `f${index.toString().padStart(3, "0")}`;
      forkValues.set(key, `fork-${index}`);
      return {
        _tag: "Put" as const,
        key: bytesOf(key),
        value: bytesOf(`fork-${index}`),
        ttlMillis: Option.none(),
      };
    });
    yield* untilSuccess(
      ledger,
      "fork.write",
      api.databases.write({
        params: { databaseId: forkSource },
        payload: { operations: forkOperations },
      }),
    );
    const forkCheckpoint = yield* untilSuccess(
      ledger,
      "fork.checkpoint",
      api.databases.createCheckpoint({
        params: { databaseId: forkSource },
        payload: { name: Option.some("soak fork"), lifetimeMs: Option.none() },
      }),
    );
    let forkReady = false;
    if (Option.isSome(forkCheckpoint)) {
      const forked = yield* untilSuccess(
        ledger,
        "fork.fork",
        api.databases.fork({
          params: { databaseId: forkSource },
          payload: {
            forkId: forkCopy,
            checkpointId: forkCheckpoint.value.id,
            range: { start: new Uint8Array(0), end: Option.none() },
          },
        }),
      );
      forkReady = Option.isSome(forked);
      yield* untilSuccess(
        ledger,
        "fork.deleteCheckpoint",
        api.databases.deleteCheckpoint({
          params: { databaseId: forkSource, checkpointId: forkCheckpoint.value.id },
        }),
      );
      const listed = yield* untilSuccess(
        ledger,
        "fork.listCheckpoints",
        api.databases.listCheckpoints({ params: { databaseId: forkSource } }),
      );
      const hold = Option.flatMap(listed, (list) =>
        Option.fromUndefinedOr(
          list.checkpoints.find((checkpoint) => Option.contains(checkpoint.holder, forkCopy)),
        ),
      );
      if (Option.isNone(hold)) {
        ledger.mismatch(`${forkSource}: no hold for fork ${forkCopy} after the fork`);
      } else {
        const deleted = yield* Effect.result(
          api.databases
            .deleteCheckpoint({ params: { databaseId: forkSource, checkpointId: hold.value.id } })
            .pipe(Effect.timeout(REQUEST_TIMEOUT)),
        );
        ledger.checked("fork.hold_delete_refused (needs audit-lifecycle's hold fix)");
        if (deleted._tag === "Success")
          ledger.mismatch(
            `${forkSource}: deleting fork ${forkCopy}'s hold succeeded; it must be refused (DatabaseHasDependents)`,
          );
        else if (tagOf(deleted.failure) !== "DatabaseHasDependentsPublicError")
          ledger.mismatch(
            `${forkSource}: deleting the fork's hold failed with ${describe(deleted.failure)}, not DatabaseHasDependents`,
          );
      }
    }
    /** Reads `keys` from the fork and compares them with the checkpoint's state. */
    const readFork = (keys: ReadonlyArray<string>, kind: string) =>
      Effect.forEach(
        keys,
        (key) =>
          Effect.gen(function* () {
            const got = yield* untilSuccess(
              ledger,
              kind,
              api.databases.get({
                params: { databaseId: forkCopy },
                payload: {
                  key: bytesOf(key),
                  consistency: { _tag: "Latest" },
                  snapshot: Option.none(),
                },
              }),
            );
            if (Option.isNone(got)) return;
            const read = Option.flatMap(got.value.row, (row) =>
              row.value._tag === "Value" ? Option.some(stringOf(row.value.bytes)) : Option.none(),
            );
            if (Option.getOrNull(read) !== (forkValues.get(key) ?? null))
              ledger.mismatch(
                `${forkCopy} ${key}: read ${Option.getOrNull(read)}, the checkpoint held ${forkValues.get(key)}`,
              );
          }),
        { concurrency: 8, discard: true },
      );
    const forkKeys = [...forkValues.keys()];
    const forkWorkload = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        // The source moves on: every fork key overwritten, flushed, compacted in time.
        yield* untilSuccess(
          ledger,
          "fork.source_write",
          api.databases.write({
            params: { databaseId: forkSource },
            payload: {
              operations: forkKeys
                .slice((round * 20) % 200, ((round * 20) % 200) + 20)
                .map((key) => ({
                  _tag: "Put" as const,
                  key: bytesOf(key),
                  value: bytesOf(`after-${round}`),
                  ttlMillis: Option.none(),
                })),
            },
          }),
        );
        if (round % 3 === 0)
          yield* untilSuccess(
            ledger,
            "fork.source_flush",
            api.databases.flush({ params: { databaseId: forkSource } }),
          );
        if (forkReady && round % 30 === 0) {
          yield* readFork(
            forkKeys.filter((_, index) => index % 10 === round % 10),
            "fork.get",
          );
          ledger.checked("fork.read");
        }
        round += 1;
        yield* pause(10_000);
      }
    });

    // The sharded group: single-key writes through the router, split twice while writes continue.
    const group = ShardGroup.make(`${prefix}-group`);
    const groupModel = new DatabaseModel(DatabaseId.make(`${group}-0`));
    yield* untilSuccess(
      ledger,
      "shards.create",
      shards.groups.create({
        params: { group },
        payload: { databaseId: DatabaseId.make(`${group}-0`) },
      }),
    );
    const groupWriter = Effect.gen(function* () {
      let counter = 0;
      while (yield* running) {
        const key = keyOf(Math.floor(Math.random() * 2_000));
        const value = `g${counter++}`;
        groupModel.pending.set(key, Option.some(value));
        const done = yield* untilSuccess(
          ledger,
          "shards.put",
          timed(
            ledger,
            "shards.put",
            shards.groups.put({
              params: { group },
              payload: { key: bytesOf(key), value: bytesOf(value) },
            }),
          ),
        );
        groupModel.pending.delete(key);
        if (Option.isSome(done)) groupModel.record(key, counter, Option.some(value), false);
        yield* Effect.sleep("150 millis");
      }
    });
    const splitter = Effect.gen(function* () {
      for (const minute of options.splitAtMinutes) {
        // A split planned after the workload's end is skipped: it must not hold the workload open.
        if (minute >= options.minutes) continue;
        const wait = startedAt + minute * 60_000 - (yield* Clock.currentTimeMillis);
        if (wait > 0) yield* Effect.sleep(`${wait} millis`);
        const map = yield* untilSuccess(
          ledger,
          "shards.map",
          shards.groups.map({ params: { group } }),
        );
        if (Option.isNone(map)) continue;
        // Split the shard holding the middle of the key space.
        const middle = keyOf(1_000 + Math.floor(Math.random() * 200));
        const holder = map.value.shards.find(
          (shard) =>
            stringOf(shard.range.start) <= middle &&
            Option.match(shard.range.end, {
              onNone: () => true,
              onSome: (end) => middle < stringOf(end),
            }),
        );
        if (holder === undefined) continue;
        yield* untilSuccess(
          ledger,
          "shards.split",
          timed(
            ledger,
            "shards.split",
            shards.groups.split({
              params: { group },
              payload: { databaseId: holder.databaseId, atKey: bytesOf(middle) },
            }),
          ),
          5,
        );
        ledger.checked("shards.split");
      }
    });

    // Every invocation's outcome, from the tail records, folded into counts each minute: a 2-hour
    // soak makes too many to read in one go at the end. Each read has a budget, and the watcher is
    // interrupted when the workload ends, wherever it is.
    const tails = new TailTally(startedAt);
    const tailWatcher = every(
      "60 seconds",
      Effect.gen(function* () {
        const read = yield* readTailsWithin(tails, WATCHER_READ_BUDGET, forEachInvocationRecord);
        // The cursor only moves past pages that were read, so the next read picks up the rest.
        if (read._tag === "Failed") ledger.note("tail read", read.message.slice(0, 200));
        if (read._tag === "OutOfTime")
          ledger.note("tail read", `cut off after ${WATCHER_READ_BUDGET} (${tails.progress()})`);
      }),
    );

    // Pull the stage's spans as they arrive, so the final pull is short: a busy stage exports
    // faster than one pull at the end could read (D132). One pull at a time (the puller's lock);
    // a pull cut off resumes after the last export it stored; the report says how far they got.
    const spanPuller = yield* makeSpanPuller(
      pullMeasurementSpans(options.minutes + 10),
      spansPulledThroughMs,
    );
    const tracePuller = every(
      SPAN_PULL_INTERVAL,
      Effect.gen(function* () {
        const pulled = yield* spanPuller.pullWithin(WATCHER_READ_BUDGET);
        if (pulled._tag === "Failed") ledger.note("trace pull", pulled.message.slice(0, 200));
        else if (pulled._tag === "OutOfTime")
          ledger.note(
            "trace pull",
            `cut off after ${WATCHER_READ_BUDGET}; pulled through ${spanPuller.progress.toReport().spansPulledThrough ?? "nothing yet"}`,
          );
      }),
    );

    const redeployer = Option.match(options.redeployAtMinute, {
      onNone: () => Effect.void,
      onSome: (minute) =>
        Effect.gen(function* () {
          // A redeploy planned after the workload's end is skipped, like a late split.
          if (minute >= options.minutes) return;
          yield* Effect.sleep(`${minute * 60_000} millis`);
          const code = yield* redeploy(
            stack.previewStage.pipe(Option.getOrElse(() => "local")),
            options.traceSampleRate,
          );
          ledger.checked("redeploy");
          if (code !== 0) ledger.failures.push(`redeploy exited ${code}`);
        }),
    });

    // The report as known so far: written to `--report-file` during the run, printed at the end.
    let phase: SoakPhase = "workload";
    let endedAt = Option.none<number>();
    const storage: Array<{
      readonly databaseId: DatabaseId;
      readonly liveSsts: number;
      readonly liveRunBytes: number;
      readonly stored: { objects: number; bytes: number; older: number; olderBytes: number };
    }> = [];
    let spansChecked = 0;
    const errorsByException = new Map<string, number>();
    let health = Option.none<Effect.Success<ReturnType<typeof measureHealth>>>();
    const report = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const unexpectedErrors = [...errorsByException].filter(
        ([type]) => !EXPECTED_EXCEPTIONS.has(type),
      );
      return {
        passed: soakPassed({
          phase,
          mismatches: ledger.mismatches,
          failures: ledger.failures,
          unexpectedErrors,
          healthIssues: Option.map(health, (found) => found.issues),
        }),
        phase,
        updatedAt: new Date(now).toISOString(),
        minutes: (Option.getOrElse(endedAt, () => now) - startedAt) / 60_000,
        databases: models.map((model) => model.databaseId),
        group,
        mismatches: ledger.mismatches,
        failures: ledger.failures,
        checks: Object.fromEntries(ledger.checks),
        retried: Object.fromEntries(ledger.retried),
        events: ledger.events,
        counters: models.map((model) => ({
          databaseId: model.databaseId,
          committed: model.counterCommitted,
          uncertain: model.counterUncertain,
        })),
        latencies: Object.fromEntries(
          [...ledger.latencies].map(([kind, values]) => [
            kind,
            {
              count: values.length,
              p50: percentile(values, 0.5),
              p99: percentile(values, 0.99),
              max: Math.max(...values),
            },
          ]),
        ),
        storage,
        spansChecked,
        ...spanPuller.progress.toReport(),
        invocationsChecked: tails.recordsRead,
        newestTailRecordAt: Option.getOrNull(
          Option.map(tails.newestRecordAt, (at) => new Date(at).toISOString()),
        ),
        invocationOutcomes: Object.fromEntries(
          [...tails.invocationOutcomes].map(([outcome, count]) => [outcome, Math.round(count)]),
        ),
        errorsByException: Object.fromEntries(errorsByException),
        unexpectedErrors: Object.fromEntries(unexpectedErrors),
        health: Option.getOrNull(health),
      };
    });
    let reportFileFailed = false;
    /** Replaces the report file with the report as known now; a failed write is a soak failure. */
    const saveReport = Option.match(options.reportFile, {
      onNone: () => Effect.void,
      onSome: (path) =>
        Effect.gen(function* () {
          const written = yield* Effect.result(writeReportFile(path, yield* report));
          if (written._tag === "Failure" && !reportFileFailed) {
            reportFileFailed = true;
            ledger.failures.push(`report file ${path}: ${written.failure.message.slice(0, 200)}`);
          }
        }),
    });
    /** Moves to `next` and records it in the report file. */
    const enter = (next: SoakPhase) =>
      Effect.gen(function* () {
        phase = next;
        yield* saveReport;
      });

    yield* saveReport;
    // The watchers run beside the workload and are interrupted when it ends (D65).
    yield* whileWorkloadRuns(
      Effect.all(
        [
          ...models.flatMap((model) => [
            writer(model),
            reader(model),
            scanner(model),
            pager(model),
            transactor(model),
            snapshotter(model),
          ]),
          remover,
          groupWriter,
          splitter,
          redeployer,
          splitWriter,
          mergeWriter,
          burster,
          idleReader,
          forkWorkload,
        ],
        { concurrency: "unbounded", discard: true },
      ),
      Effect.all(
        [
          tracePuller,
          tailWatcher,
          every("60 seconds", saveReport),
          // The change-feed followers run for the whole workload and stop with it; each then
          // reads to the end of its feed in the final checks (D106).
          ...models.map((model, index) => {
            const follower = followers[index];
            return follower === undefined ? Effect.void : feedFollower(model, follower);
          }),
          splitFollower,
        ],
        { concurrency: "unbounded", discard: true },
      ),
    );

    // Final checks: every key at Latest, then (after a flush) at Flushed; counters; the group.
    /**
     * Reads `databaseId`'s feed to its end (an empty page, within 200 pages), then checks that every
     * acknowledged batch was delivered once and every delivered batch was sent (D106).
     */
    const finishFeed = (databaseId: DatabaseId, follower: FeedFollower, of: ChangeLedger) =>
      Effect.gen(function* () {
        let ended = false;
        for (let page = 0; page < 200 && !ended; page++) {
          const read = yield* followOnce(databaseId, follower);
          if (Option.isNone(read)) break;
          ended = read.value === 0;
        }
        if (!ended) ledger.mismatch(`${databaseId} feed: did not reach its end in 200 pages`);
        checkFeed(databaseId, follower, of);
        for (const problem of follower.unaccounted(of))
          ledger.mismatch(`${databaseId} feed: ${problem}`);
        ledger.checked("feed.final");
      });
    const finalChecks = Effect.gen(function* () {
      for (const [index, model] of models.entries()) {
        const follower = followers[index];
        if (follower !== undefined) yield* finishFeed(model.databaseId, follower, model.ledger);
      }
      if (!splitDone) {
        ledger.checked("feedsplit.not_reached");
        yield* finishFeed(splitSource.databaseId, splitFollowers.source, splitSource.ledger);
      } else {
        // The source to its end, then each successor from the source's last sequence number.
        yield* finishFeed(splitSource.databaseId, splitFollowers.source, splitSource.ledger);
        for (const [successor, follower] of [
          [splitLeft, splitFollowers.left],
          [splitRight, splitFollowers.right],
        ] as const) {
          yield* finishFeed(
            successor.databaseId,
            Option.getOrElse(follower, () => new FeedFollower(splitFromSeq)),
            successor.ledger,
          );
        }
        ledger.checked("feedsplit.final");
      }
      if (forkReady) {
        yield* readFork(forkKeys, "final.fork");
        ledger.checked("final.fork");
      }
      for (const model of models) {
        for (const consistency of [{ _tag: "Latest" } as const, { _tag: "Flushed" } as const]) {
          if (consistency._tag === "Flushed") {
            yield* untilSuccess(
              ledger,
              "flush",
              api.databases.flush({ params: { databaseId: model.databaseId } }),
            );
          }
          const seen = new Map<string, string>();
          let start: Uint8Array = new Uint8Array(0);
          for (;;) {
            const page = yield* untilSuccess(
              ledger,
              "final.scan",
              api.databases.scan({
                params: { databaseId: model.databaseId },
                payload: {
                  range: { start, end: Option.none() },
                  consistency,
                  snapshot: Option.none(),
                  limit: 1000,
                },
              }),
            );
            if (Option.isNone(page)) break;
            for (const row of page.value.rows) {
              if (row.value._tag === "Value")
                seen.set(stringOf(row.key), stringOf(row.value.bytes));
            }
            if (Option.isNone(page.value.resumeFrom)) break;
            start = page.value.resumeFrom.value;
          }
          let compared = 0;
          for (const [key] of model.keys) {
            const expected = Option.flatten(model.current(key));
            compared += 1;
            if ((seen.get(key) ?? null) !== Option.getOrNull(expected)) {
              ledger.mismatch(
                `${model.databaseId} ${key} final ${consistency._tag}: expected ${Option.getOrNull(expected)?.slice(0, 12)}, read ${seen.get(key)?.slice(0, 12)}`,
              );
            }
          }
          for (const [key, value] of seen) {
            const aborted = model.maybe.get(key);
            if (aborted !== undefined) {
              ledger.checked("final.aborted_key");
              if (value !== aborted)
                ledger.mismatch(
                  `${model.databaseId} ${key} aborted write read ${value.slice(0, 12)}`,
                );
              continue;
            }
            if (key !== COUNTER && !model.keys.has(key))
              ledger.mismatch(`${model.databaseId} ${key} was never written`);
          }
          ledger.checked(`final.${consistency._tag}`);
          ledger.time(`final.keys.${consistency._tag}`, compared);
          const counter = Number(seen.get(COUNTER) ?? "0");
          if (
            counter < model.counterCommitted ||
            counter > model.counterCommitted + model.counterUncertain
          ) {
            ledger.mismatch(
              `${model.databaseId} counter ${counter}, committed ${model.counterCommitted} (+${model.counterUncertain} uncertain)`,
            );
          }
        }
      }
      yield* Effect.forEach(
        [...groupModel.keys.keys()],
        (key) =>
          Effect.gen(function* () {
            const got = yield* untilSuccess(
              ledger,
              "final.shards.get",
              shards.groups.get({ params: { group }, payload: { key: bytesOf(key) } }),
            );
            if (Option.isNone(got)) return;
            const expected = Option.flatten(groupModel.current(key));
            if (
              Option.getOrNull(Option.map(got.value.value, stringOf)) !== Option.getOrNull(expected)
            ) {
              ledger.mismatch(`group ${key} final: expected ${Option.getOrNull(expected)}`);
            }
          }),
        { concurrency: 16, discard: true },
      );
      ledger.checked("final.group");
    });
    // The data checks come first: telemetry is read only after them, so it can never hold them up.
    yield* enter("final-reads");
    yield* finalChecks;

    // Storage: after collecting, no SST older than GC's age limit is stored unless the manifest uses it.
    yield* enter("storage");
    const oldMs = 3_600_000 + 300_000 + 600_000;
    for (const model of models) {
      yield* untilSuccess(
        ledger,
        "gc.collect",
        gcApi.gc.collect({ params: { databaseId: model.databaseId } }),
      );
      yield* untilSuccess(
        ledger,
        "gc.collect",
        gcApi.gc.collect({ params: { databaseId: model.databaseId } }),
      );
      const status = yield* untilSuccess(
        ledger,
        "compaction",
        api.databases.compaction({ params: { databaseId: model.databaseId } }),
      );
      const described = yield* http
        .execute(
          HttpClientRequest.get(`${stack.probe}/v1/probe/storage`).pipe(
            HttpClientRequest.setUrlParams({
              prefix: `db/${model.databaseId}/`,
              olderThanMs: String(oldMs),
            }),
          ),
        )
        .pipe(
          Effect.flatMap((response) => response.text),
          Effect.flatMap(decodeStorageReport),
          Effect.timeout(REQUEST_TIMEOUT),
          Effect.option,
        );
      if (Option.isNone(status) || Option.isNone(described)) continue;
      const liveSsts =
        status.value.l0Ssts + status.value.sortedRuns.reduce((sum, run) => sum + run.ssts, 0);
      const liveBytes = status.value.sortedRuns.reduce((sum, run) => sum + run.sizeBytes, 0);
      const ssts = described.value.kinds["Sst"] ?? {
        objects: 0,
        bytes: 0,
        older: 0,
        olderBytes: 0,
      };
      storage.push({
        databaseId: model.databaseId,
        liveSsts,
        liveRunBytes: liveBytes,
        stored: ssts,
      });
      ledger.checked("storage");
      if (ssts.older > liveSsts) {
        ledger.mismatch(
          `${model.databaseId} stores ${ssts.older} SSTs older than ${oldMs} ms but uses ${liveSsts}`,
        );
      }
    }

    endedAt = Option.some(yield* Clock.currentTimeMillis);
    // The watcher's last pull has been interrupted and awaited with the workload: this is how far
    // the pulls had got when the workload ended (D132).
    spanPuller.progress.markWorkloadEnd(Option.getOrElse(endedAt, () => startedAt));
    yield* enter("telemetry");
    // Telemetry last, each read within its budget: one that runs out is a failure saying how far
    // it got, and the next check still runs.
    const budget = Duration.minutes(options.telemetryBudgetMinutes);
    const budgetText = `${options.telemetryBudgetMinutes} min`;
    // The last tail records, then the health report over the soak's last minutes (its tail-record
    // part over the whole soak is `invocationOutcomes`).
    yield* Effect.sleep("20 seconds");
    const tailsLeft = yield* catchUpTails(tails, budget, forEachInvocationRecord);
    if (Option.isSome(tailsLeft)) ledger.failures.push(tailsLeft.value);
    if (tails.recordsRead === 0) {
      ledger.failures.push("no tail records from the soak's window were read");
    }
    for (const hit of tails.limitHits) ledger.failures.push(`invocation: ${hit}`);
    yield* saveReport;
    // The spans left since the watcher's last pull, before the health report: its own pull of
    // the last 15 minutes then has nothing left to fetch, and it cannot move the store's cursor
    // past spans the error check below still needs (D132).
    const caughtUp = yield* spanPuller.pullWithin(budget);
    if (caughtUp._tag === "Failed") ledger.failures.push(`trace pull: ${caughtUp.message}`);
    else if (caughtUp._tag === "OutOfTime")
      ledger.failures.push(
        `trace pull: not finished within ${budgetText}; pulled through ${spanPuller.progress.toReport().spansPulledThrough ?? "nothing"}`,
      );
    yield* saveReport;
    const healthRead = yield* Effect.result(readWithin(budget, measureHealth(15)));
    if (healthRead._tag === "Failure") {
      ledger.failures.push(`health report: ${healthRead.failure.message}`);
    } else if (healthRead.success._tag === "OutOfTime") {
      ledger.failures.push(`health report: not finished within ${budgetText}`);
    } else {
      health = Option.some(healthRead.success.value);
    }
    yield* saveReport;
    // Searched even when the catch-up ran out (that is already a failure): what it holds is read.
    const store = Option.some(yield* measurementStorePath);
    /** The spans matching `args` in the store, or `None` (recorded as a failure) when unreadable. */
    const searchSpans = (args: ReadonlyArray<string>) =>
      Effect.gen(function* () {
        if (Option.isNone(store))
          return Option.none<Effect.Success<ReturnType<typeof spansSince>>>();
        const found = yield* Effect.result(
          readWithin(budget, spansSince(store.value, startedAt, args)),
        );
        if (found._tag === "Failure") {
          ledger.failures.push(`trace grep: ${found.failure.message}`);
          return Option.none();
        }
        if (found.success._tag === "OutOfTime") {
          ledger.failures.push(`trace grep ${args.join(" ")}: not finished within ${budgetText}`);
          return Option.none();
        }
        return Option.some(found.success.value);
      });
    for (const span of Option.getOrElse(yield* searchSpans(["--error"]), () => [])) {
      const type = exceptionType(span);
      errorsByException.set(type, (errorsByException.get(type) ?? 0) + 1);
    }
    // A soak whose spans were not read cannot claim no unexpected errors.
    spansChecked = Option.match(yield* searchSpans(["--name", "DbWriter.write"]), {
      onNone: () => 0,
      onSome: (spans) => spans.length,
    });
    if (spansChecked === 0) ledger.failures.push("no spans from the soak's window were read");
    // The file at `done` holds exactly the report printed. A report-file failure found here is
    // recorded first, so the printed report and its verdict include it.
    phase = "done";
    yield* saveReport;
    const finalReport = yield* report;
    yield* Option.match(options.reportFile, {
      onNone: () => Effect.void,
      onSome: (path) => Effect.ignore(writeReportFile(path, finalReport)),
    });
    return finalReport;
  });
