/**
 * Writer benchmark: splits one write batch's CPU on the writer side into decoding the JSON body,
 * collapsing the batch, and the rest of the in-process write path (the writer's HTTP handling and
 * the SQLite commit), to decide whether a binary batch body is worth it (D61).
 *
 * Usage: `node tools/writer-bench/src/writer-bench.ts [--rounds 300]`. Node's `node:sqlite` stands in
 * for Durable Object SQLite, so absolute numbers differ from workerd; the shares are what matter.
 */
import { performance } from "node:perf_hooks";

import {
  collapseWriteBatch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  WriteBatch,
} from "@cf-slatedb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
} from "@cf-slatedb/lsm-cloudflare";
import { Effect, Option, Schema } from "effect";

const roundsArgument = process.argv.indexOf("--rounds");
const ROUNDS = roundsArgument === -1 ? 300 : Number(process.argv[roundsArgument + 1]);

const encodeBatchJson = Schema.encodeSync(Schema.fromJsonString(WriteBatch));
const decodeBatchJson = Schema.decodeUnknownSync(Schema.fromJsonString(WriteBatch));

/** A batch of `rows` puts with 16-byte keys and 100-byte values, keys distinct per `round`. */
const batchOf = (rows: number, round: number): WriteBatch => ({
  operations: Array.from({ length: rows }, (_, row) => ({
    _tag: "Put" as const,
    key: new TextEncoder().encode(
      `k${round.toString().padStart(7, "0")}${row.toString().padStart(8, "0")}`,
    ),
    value: new Uint8Array(100).fill(row % 251),
    ttlMillis: Option.none(),
  })),
});

/** Mean microseconds per call of `run` over `ROUNDS` calls (after a warm-up). */
const timeSync = (run: (round: number) => void) => {
  for (let round = 0; round < 20; round++) run(round);
  const started = performance.now();
  for (let round = 0; round < ROUNDS; round++) run(round);
  return ((performance.now() - started) / ROUNDS) * 1000;
};

const benchmark = (rows: number) =>
  Effect.gen(function* () {
    const json = encodeBatchJson(batchOf(rows, 0));
    const decodeUs = timeSync(() => {
      decodeBatchJson(json);
    });
    const decoded = batchOf(rows, 0);
    const collapseUs = timeSync(() => {
      collapseWriteBatch(decoded, {
        databaseId: DatabaseId.make("bench"),
        keyRange: Option.none(),
        mergeOperator: Option.none(),
      });
    });
    const databaseId = DatabaseId.make(`bench-${rows}`);
    yield* (yield* DatabaseLifecycle).create(databaseId, {
      keyRange: Option.none(),
      databaseOptions: DEFAULT_DATABASE_OPTIONS,
    });
    const client = yield* DbClient;
    for (let round = 0; round < 20; round++) yield* client.write(databaseId, batchOf(rows, round));
    const started = performance.now();
    for (let round = 0; round < ROUNDS; round++) {
      yield* client.write(databaseId, batchOf(rows, 1_000 + round));
    }
    const writeUs = ((performance.now() - started) / ROUNDS) * 1000;
    return {
      rows,
      bodyBytes: json.length,
      decodeUs: Math.round(decodeUs),
      collapseUs: Math.round(collapseUs),
      writePathUs: Math.round(writeUs),
      decodeShareOfWritePath: Number((decodeUs / writeUs).toFixed(3)),
    };
  });

const results = await Effect.runPromise(
  Effect.forEach([100, 1000], benchmark).pipe(
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
console.log(JSON.stringify({ rounds: ROUNDS, results }, null, 2));
