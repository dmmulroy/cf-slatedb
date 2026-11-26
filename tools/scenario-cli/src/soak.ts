import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { DatabaseId, SeqNum } from "@cf-slatedb/lsm-core";
import { DbHttpApi, GcHttpApi } from "@cf-slatedb/lsm-cloudflare";
import { ShardedHttpApi, ShardGroup } from "@cf-slatedb/sharding";
import { Clock, Effect, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { exceptionType, measureHealth, pullMeasurementSpans, spansSince } from "./measurements.ts";
import { ScenarioStack, uniqueId } from "./scenario.ts";

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
}

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
class DatabaseModel {
  readonly keys = new Map<string, KeyHistory>();
  readonly pending = new Map<string, Option.Option<string>>();
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
    ("_tag" in error && (error._tag === "HttpClientError" || error._tag === "SchemaError")));

const tagOf = (error: unknown) =>
  typeof error === "object" && error !== null && "_tag" in error && typeof error._tag === "string"
    ? error._tag
    : "unknown";

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
      const result = yield* Effect.result(effect);
      if (result._tag === "Success") return Option.some(result.success);
      if (!isRetryable(result.failure) || attempt >= attempts) {
        ledger.failures.push(`${kind}: ${tagOf(result.failure)}`);
        return Option.none<A>();
      }
      ledger.retry(kind);
      yield* Effect.sleep("1 second");
    }
  });

/** Redeploys the stage with production settings without blocking the soak's fibers. */
const redeploy = (stage: string) =>
  Effect.callback<number>((resume) => {
    const child = spawn(
      process.execPath,
      [
        fileURLToPath(new URL("../../preview-cli/src/preview-cli.ts", import.meta.url)),
        "deploy",
        stage,
      ],
      { cwd: REPO_ROOT, stdio: ["ignore", "ignore", "ignore"] },
    );
    child.on("exit", (code) => resume(Effect.succeed(code ?? -1)));
  });

/**
 * Exceptions the soak's workload provokes on purpose, so their error spans are expected:
 * - `DatabaseSealedError`: a router's stale map sends a key to a shard a split just sealed; it waits
 *   for the new map and retries (D59).
 * - `DatabaseNotFoundError`: the remover's check that a removed database no longer answers.
 * - `TransactionConflictError`: concurrent increments of one counter; the loser retries.
 * - `WriterFencedError`: the redeploy replaces writers; a request on the old one is retried.
 * - `HttpClientError`: the calling side of one of the above.
 * Any other exception in the soak's window fails it.
 */
const EXPECTED_EXCEPTIONS = new Set([
  "DatabaseSealedError",
  "DatabaseNotFoundError",
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
    const prefix = uniqueId("soak");
    const models = Array.from(
      { length: options.databases },
      (_, index) => new DatabaseModel(DatabaseId.make(`${prefix}-${index}`)),
    );
    const keyOf = (index: number) => `k${index.toString().padStart(6, "0")}`;
    const COUNTER = "counter";

    for (const model of models) {
      yield* untilSuccess(
        ledger,
        "create",
        api.databases.createDatabase({
          payload: {
            databaseId: model.databaseId,
            keyRange: Option.none(),
            mergeOperator: Option.none(),
            changelogRetentionMs: Option.none(),
            budgetGroup: Option.none(),
          },
        }),
      );
    }

    /** Writes batches to one database: puts and deletes on its keys, each acknowledged write recorded. */
    const writer = (model: DatabaseModel) =>
      Effect.gen(function* () {
        let counter = 0;
        while (yield* running) {
          const size = 20 + Math.floor(Math.random() * 120);
          const chosen = new Set<number>();
          while (chosen.size < size)
            chosen.add(Math.floor(Math.random() * options.keysPerDatabase));
          const operations = [...chosen].map((index) => {
            const key = keyOf(index);
            counter += 1;
            const value =
              Math.random() < 0.1
                ? Option.none<string>()
                : Option.some(`v${counter}-${"x".repeat(64 + (counter % 128))}`);
            model.pending.set(key, value);
            return { key, value };
          });
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
            ).pipe(Effect.tapError(() => Effect.sync(() => (replayed = true)))),
          );
          if (Option.isSome(receipt)) {
            for (const { key, value } of operations)
              model.record(key, receipt.value.seq, value, replayed);
            model.lastAckedSeq = Math.max(model.lastAckedSeq, receipt.value.seq);
          }
          for (const { key } of operations) model.pending.delete(key);
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
          const commit = yield* Effect.result(
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
          );
          if (commit._tag === "Success") {
            model.counterCommitted += 1;
            ledger.checked("transaction.commit");
          } else if (tagOf(commit.failure) === "TransactionConflictPublicError") {
            ledger.checked("transaction.conflict");
          } else if (isRetryable(commit.failure)) {
            model.counterUncertain += 1;
            ledger.retry("transaction.commit");
          } else {
            ledger.failures.push(`transaction.commit: ${tagOf(commit.failure)}`);
          }
          yield* Effect.sleep("2 seconds");
        }
      });

    /** Pins a snapshot every few minutes and reads sampled keys at it later. */
    const snapshotter = (model: DatabaseModel) =>
      Effect.gen(function* () {
        yield* Effect.sleep("3 minutes");
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
            yield* Effect.sleep("4 minutes");
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
          yield* Effect.sleep("6 minutes");
        }
      });

    /** Creates, uses, and removes short-lived databases. */
    const remover = Effect.gen(function* () {
      let round = 0;
      while (yield* running) {
        yield* Effect.sleep("9 minutes");
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

    const redeployer = Option.match(options.redeployAtMinute, {
      onNone: () => Effect.void,
      onSome: (minute) =>
        Effect.gen(function* () {
          yield* Effect.sleep(`${minute * 60_000} millis`);
          const code = yield* redeploy(stack.previewStage.pipe(Option.getOrElse(() => "local")));
          ledger.checked("redeploy");
          if (code !== 0) ledger.failures.push(`redeploy exited ${code}`);
        }),
    });

    yield* Effect.all(
      [
        ...models.flatMap((model) => [
          writer(model),
          reader(model),
          scanner(model),
          transactor(model),
          snapshotter(model),
        ]),
        remover,
        groupWriter,
        splitter,
        redeployer,
      ],
      { concurrency: "unbounded", discard: true },
    );

    // Final checks: every key at Latest, then (after a flush) at Flushed; counters; the group.
    const finalChecks = Effect.gen(function* () {
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
          for (const key of seen.keys()) {
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
      for (const [key] of groupModel.keys) {
        const got = yield* untilSuccess(
          ledger,
          "final.shards.get",
          shards.groups.get({ params: { group }, payload: { key: bytesOf(key) } }),
        );
        if (Option.isNone(got)) continue;
        const expected = Option.flatten(groupModel.current(key));
        if (
          Option.getOrNull(Option.map(got.value.value, stringOf)) !== Option.getOrNull(expected)
        ) {
          ledger.mismatch(`group ${key} final: expected ${Option.getOrNull(expected)}`);
        }
      }
      ledger.checked("final.group");
    });
    yield* finalChecks;

    // Storage: after collecting, no SST older than GC's age limit is stored unless the manifest uses it.
    const storage = [];
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

    const endedAt = yield* Clock.currentTimeMillis;
    const health = yield* measureHealth(Math.ceil((endedAt - startedAt) / 60_000) + 5).pipe(
      Effect.option,
    );
    const store = yield* pullMeasurementSpans(Math.ceil((endedAt - startedAt) / 60_000) + 5);
    const errorSpans = yield* spansSince(store, startedAt, ["--error"]).pipe(
      Effect.orElseSucceed(() => []),
    );
    const errorsByException = new Map<string, number>();
    for (const span of errorSpans) {
      const type = exceptionType(span);
      errorsByException.set(type, (errorsByException.get(type) ?? 0) + 1);
    }
    const unexpectedErrors = [...errorsByException].filter(
      ([type]) => !EXPECTED_EXCEPTIONS.has(type),
    );
    const latencies = Object.fromEntries(
      [...ledger.latencies].map(([kind, values]) => [
        kind,
        {
          count: values.length,
          p50: percentile(values, 0.5),
          p99: percentile(values, 0.99),
          max: Math.max(...values),
        },
      ]),
    );
    const passed =
      ledger.mismatches.length === 0 &&
      ledger.failures.length === 0 &&
      unexpectedErrors.length === 0 &&
      Option.exists(health, (report) => report.issues.length === 0);
    return {
      passed,
      minutes: (endedAt - startedAt) / 60_000,
      databases: models.map((model) => model.databaseId),
      group,
      mismatches: ledger.mismatches,
      failures: ledger.failures,
      checks: Object.fromEntries(ledger.checks),
      retried: Object.fromEntries(ledger.retried),
      counters: models.map((model) => ({
        databaseId: model.databaseId,
        committed: model.counterCommitted,
        uncertain: model.counterUncertain,
      })),
      latencies,
      storage,
      errorsByException: Object.fromEntries(errorsByException),
      unexpectedErrors: Object.fromEntries(unexpectedErrors),
      health: Option.getOrNull(health),
    };
  });
