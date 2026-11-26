/**
 * Flush benchmark: the time one flush of `--megabytes` of 1,000-byte rows takes in process (the
 * memtable read from SQLite, the SST encoding, and the multipart upload into the in-memory object
 * store), to find where a flush spends its CPU.
 *
 * Usage: `node tools/writer-bench/src/flush-bench.ts [--megabytes 60] [--rounds 3]`. Node's
 * `node:sqlite` stands in for Durable Object SQLite and nothing crosses a network, so the result is
 * the flush's CPU, not its wall time on Cloudflare.
 */
import { performance } from "node:perf_hooks";

import { DatabaseId, DEFAULT_DATABASE_OPTIONS, type WriteBatch } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
} from "@shaledb/lsm-cloudflare";
import { Effect, Option } from "effect";

const argument = (name: string, fallback: number) => {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : Number(process.argv[index + 1]);
};
const MEGABYTES = argument("--megabytes", 60);
const ROUNDS = argument("--rounds", 3);
const BATCH_ROWS = 2000;
const VALUE_BYTES = 1000;

const batchOf = (round: number, sequence: number): WriteBatch => ({
  operations: Array.from({ length: BATCH_ROWS }, (_, row) => ({
    _tag: "Put" as const,
    key: new TextEncoder().encode(
      `k${round.toString().padStart(3, "0")}${sequence.toString().padStart(6, "0")}${row.toString().padStart(6, "0")}`,
    ),
    value: new Uint8Array(VALUE_BYTES).fill((row * 13) % 251),
    ttlMillis: Option.none(),
  })),
});

const flushOnce = (round: number) =>
  Effect.gen(function* () {
    const databaseId = DatabaseId.make(`flush-bench-${round}`);
    yield* (yield* DatabaseLifecycle).create(databaseId, {
      keyRange: Option.none(),
      databaseOptions: DEFAULT_DATABASE_OPTIONS,
    });
    const client = yield* DbClient;
    const batches = Math.ceil((MEGABYTES * 1_048_576) / (BATCH_ROWS * VALUE_BYTES));
    const writesStarted = performance.now();
    for (let sequence = 0; sequence < batches; sequence++) {
      yield* client.write(databaseId, batchOf(round, sequence));
    }
    const writeMs = performance.now() - writesStarted;
    const flushStarted = performance.now();
    yield* client.flush(databaseId);
    return {
      rows: batches * BATCH_ROWS,
      writeMs: Math.round(writeMs),
      flushMs: Math.round(performance.now() - flushStarted),
    };
  });

const results = await Effect.runPromise(
  Effect.forEach(
    Array.from({ length: ROUNDS }, (_, round) => round),
    flushOnce,
  ).pipe(
    Effect.scoped,
    Effect.provide(
      dbServicesTestLayer({
        l0SstSize: 1024 * 1024 * 1024,
        maxUnflushedBytes: 2 * 1024 * 1024 * 1024,
      }),
    ),
    Effect.provide(lsmTestRuntimeContextLayer),
  ),
);
console.log(JSON.stringify({ megabytes: MEGABYTES, results }));
