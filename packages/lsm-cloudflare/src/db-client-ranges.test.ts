import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type KeyRange,
  SortedRunId,
  type WriteBatch,
} from "@shaledb/lsm-core";
import { Arbitrary, Bytes, Clock, Effect, Layer, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { runCompactionJob } from "./compaction-job.ts";
import { directStepRunner } from "./compaction-launcher.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/**
 * Scans against an oracle over the shapes the other scan properties never produce (read audit):
 * keys that are prefixes of each other (and the empty key), bounded, empty and reversed ranges,
 * values that expire, reads after the expiry, compactions, and pages followed by their resume key.
 */

const DB = DatabaseId.make("orders");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** Keys that are prefixes of each other, so every range bound lands between or on them. */
const KEYS = ["", "a", "aa", "ab", "b", "ba"].map((text) => new TextEncoder().encode(text));
/** Range bounds: every key, and a byte past one ("a\0"), between "a" and "aa". */
const BOUNDS = [...KEYS, Uint8Array.of(0x61, 0)];

type Step =
  | { readonly _tag: "Put"; readonly key: number; readonly value: number; readonly ttl: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Merge"; readonly key: number; readonly value: number }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Compact" }
  | { readonly _tag: "Wait"; readonly millis: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 11),
  key: smallInt(0, KEYS.length - 1),
  value: smallInt(1, 255),
  ttl: smallInt(0, 3),
}).pipe(
  Arbitrary.map(({ kind, key, value, ttl }): Step => {
    if (kind <= 3) return { _tag: "Put", key, value, ttl: ttl * 1_000 };
    if (kind === 4) return { _tag: "Delete", key };
    if (kind <= 6) return { _tag: "Merge", key, value };
    if (kind <= 8) return { _tag: "Flush" };
    if (kind === 9) return { _tag: "Compact" };
    return { _tag: "Wait", millis: ttl * 1_000 + 500 };
  }),
);

/** One version the oracle keeps: what was written, and when it expires (ms; none: never). */
interface Version {
  readonly kind: "Put" | "Delete" | "Merge";
  readonly value: number;
  readonly expireAt: Option.Option<number>;
}

/** A key's value at `now`, as D81 states it: walk newest first to the first value or delete. */
const resolve = (versions: ReadonlyArray<Version>, now: number): Option.Option<Uint8Array> => {
  const operands: Array<number> = [];
  for (let index = versions.length - 1; index >= 0; index--) {
    const version = versions[index];
    if (version === undefined) break;
    if (version.kind === "Merge") {
      operands.unshift(version.value);
      continue;
    }
    const live =
      version.kind === "Put" &&
      Option.match(version.expireAt, {
        onNone: () => true,
        onSome: (at) => at > now,
      });
    const base = live ? [version.value] : [];
    return base.length + operands.length === 0
      ? Option.none()
      : Option.some(Uint8Array.from([...base, ...operands]));
  }
  return operands.length === 0 ? Option.none() : Option.some(Uint8Array.from(operands));
};

const contains = (range: KeyRange, key: Uint8Array) =>
  Bytes.Order(key, range.start) >= 0 &&
  Option.match(range.end, { onNone: () => true, onSome: (end) => Bytes.Order(key, end) < 0 });

const testLayer = Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 64 })),
);

const compactAll = Effect.gen(function* () {
  const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
  if (manifest.l0.length === 0 && manifest.sortedRuns.length < 2) return;
  const lowest = manifest.sortedRuns.reduce(
    (min: Option.Option<SortedRunId>, run) =>
      Option.isNone(min) || run.id < min.value ? Option.some(run.id) : min,
    Option.none<SortedRunId>(),
  );
  const destination = Option.getOrElse(lowest, () => SortedRunId.make(0));
  yield* runCompactionJob(
    {
      databaseId: DB,
      compactorEpoch: manifest.compactorEpoch,
      jobId: `job-${manifest.l0.length}-${manifest.sortedRuns.length}-${Math.random()}`,
      job: {
        sources: [
          ...[...manifest.l0].reverse().map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
          ...manifest.sortedRuns.map((run) => ({ _tag: "SortedRun" as const, id: run.id })),
        ],
        destination,
        isBottom: true,
      },
    },
    directStepRunner,
  );
});

describe("scans over prefix keys, bounded ranges, expiry, compaction, and resume keys", () => {
  it("return exactly the oracle's visible rows of the range, page after page", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.all({
          steps: Arbitrary.array(stepArbitrary, { minLength: 10, maxLength: 40 }),
          start: smallInt(0, BOUNDS.length - 1),
          end: smallInt(0, BOUNDS.length),
          limit: smallInt(1, 4),
          consistency: smallInt(0, 1),
        }),
        ({ steps, start, end, limit, consistency }) =>
          Effect.gen(function* () {
            yield* (yield* ManifestStore).initialize(
              initialManifest(DB, {
                keyRange: Option.none(),
                databaseOptions: {
                  ...DEFAULT_DATABASE_OPTIONS,
                  mergeOperator: Option.some("Concat"),
                },
              }),
            );
            const client = yield* DbClient;
            const oracle = new Map<number, Array<Version>>();
            for (const step of steps) {
              if (step._tag === "Put" || step._tag === "Delete" || step._tag === "Merge") {
                const batch: WriteBatch = {
                  operations: [
                    step._tag === "Put"
                      ? {
                          _tag: "Put",
                          key: KEYS[step.key] ?? new Uint8Array(),
                          value: Uint8Array.of(step.value),
                          ttlMillis: step.ttl === 0 ? Option.none() : Option.some(step.ttl),
                        }
                      : step._tag === "Merge"
                        ? {
                            _tag: "Merge",
                            key: KEYS[step.key] ?? new Uint8Array(),
                            operand: Uint8Array.of(step.value),
                            ttlMillis: Option.none(),
                          }
                        : { _tag: "Delete", key: KEYS[step.key] ?? new Uint8Array() },
                  ],
                };
                const receipt = yield* client.write(DB, batch);
                const versions = oracle.get(step.key) ?? [];
                versions.push({
                  kind: step._tag,
                  value: step._tag === "Delete" ? 0 : step.value,
                  expireAt:
                    step._tag === "Put" && step.ttl > 0
                      ? Option.some(receipt.createTs + step.ttl)
                      : Option.none(),
                });
                oracle.set(step.key, versions);
              } else if (step._tag === "Flush") yield* client.flush(DB);
              else if (step._tag === "Compact") {
                yield* client.flush(DB);
                yield* compactAll;
              } else yield* TestClock.adjust(step.millis);
            }
            // Past every refresh interval, so a Flushed read sees the last flush; then flush all.
            yield* client.flush(DB);
            yield* TestClock.adjust(2_000);
            const now = yield* Clock.currentTimeMillis;
            const range: KeyRange = {
              start: BOUNDS[start] ?? new Uint8Array(),
              end: end === BOUNDS.length ? Option.none() : Option.fromUndefinedOr(BOUNDS[end]),
            };
            const expected = KEYS.flatMap((key, index) => {
              if (!contains(range, key)) return [];
              return Option.toArray(
                Option.map(resolve(oracle.get(index) ?? [], now), (value) => [
                  [...key].join(","),
                  [...value].join(","),
                ]),
              );
            }).sort((left, right) => (left[0] ?? "").localeCompare(right[0] ?? ""));
            // Page by page, each asking for one row past its limit to find where the next starts.
            const seen: Array<ReadonlyArray<string>> = [];
            let from = range.start;
            for (let page = 0; page < 20; page++) {
              const rows = yield* Stream.runCollect(
                client.scan(
                  DB,
                  { start: from, end: range.end },
                  {
                    consistency: consistency === 0 ? { _tag: "Latest" } : { _tag: "Flushed" },
                    snapshot: Option.none(),
                    refuseSealed: false,
                    limit: Option.some(limit + 1),
                  },
                ),
              );
              for (const row of rows.slice(0, limit)) {
                seen.push([
                  [...row.key].join(","),
                  row.value._tag === "Value" ? [...row.value.bytes].join(",") : "?",
                ]);
              }
              const next = rows[limit];
              if (next === undefined) break;
              from = next.key;
            }
            const byKey = (rows: ReadonlyArray<ReadonlyArray<string>>) =>
              [...rows].sort((left, right) =>
                Bytes.Order(
                  Uint8Array.from(
                    (left[0] ?? "")
                      .split(",")
                      .filter((part) => part !== "")
                      .map(Number),
                  ),
                  Uint8Array.from(
                    (right[0] ?? "")
                      .split(",")
                      .filter((part) => part !== "")
                      .map(Number),
                  ),
                ),
              );
            return JSON.stringify(byKey(seen)) === JSON.stringify(byKey(expected));
          }).pipe(
            Effect.scoped,
            Effect.provide(testLayer),
            Effect.provide(lsmTestRuntimeContextLayer),
            Effect.provide(TestClock.layer()),
            Effect.orDie,
          ),
        { runs: 80 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    expect(failure).toBeUndefined();
  });
});
