import { Arbitrary, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseId, SeqNum, SortedRunId, SstId, Timestamp } from "./ids.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";
import { DEFAULT_LSM_SETTINGS, LsmSettings } from "./lsm-settings.ts";
import {
  commitCompaction,
  commitL0Flush,
  type CompactionJob,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  Manifest,
  type SortedRun,
  type SstHandle,
} from "./manifest.ts";
import {
  DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS,
  planCompactions,
  planCompactionsAround,
  type SizeTieredCompactionOptions,
} from "./size-tiered-compaction-scheduler.ts";

const DB = DatabaseId.make("orders");
let nextHandle = 0;
let nextJobId = 0;
const handle = (sizeBytes: number): SstHandle => ({
  id: SstId.make(`01JD${String(++nextHandle).padStart(22, "0")}`),
  firstKey: Uint8Array.of(0),
  lastKey: Uint8Array.of(255),
  sizeBytes,
  rowCount: 1,
  metaOffset: 0,
  metaLength: 1,
  owner: Option.none(),
  view: Option.none(),
});

const empty = initialManifest(DB, {
  keyRange: Option.none(),
  databaseOptions: DEFAULT_DATABASE_OPTIONS,
});

const withLsm = (l0: ReadonlyArray<SstHandle>, sortedRuns: ReadonlyArray<SortedRun>): Manifest =>
  new Manifest({
    formatVersion: 1,
    databaseId: empty.databaseId,
    writerEpoch: empty.writerEpoch,
    compactorEpoch: empty.compactorEpoch,
    lastL0Seq: empty.lastL0Seq,
    lastL0ClockTick: empty.lastL0ClockTick,
    l0,
    sortedRuns,
    checkpoints: [],
    keyRange: Option.none(),
    state: { _tag: "Open" },
    externalRefs: [],
    changelog: [],
    options: DEFAULT_DATABASE_OPTIONS,
    gcSstBoundary: Timestamp.make(0),
    droppedSsts: [],
    changelogAfterSeq: Option.none(),
    recentCompactionJobIds: [],
  });

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** A random manifest: 0–8 L0 SSTs and 0–12 sorted runs whose sizes cluster, so groups form. */
const manifestArbitrary = Arbitrary.all({
  l0Count: smallInt(0, 8),
  runSizes: Arbitrary.array(smallInt(1, 24), { maxLength: 12 }),
}).pipe(
  Arbitrary.map(({ l0Count, runSizes }) =>
    withLsm(
      Array.from({ length: l0Count }, () => handle(1000)),
      runSizes.map((size, index) => ({
        id: SortedRunId.make(runSizes.length - 1 - index),
        ssts: [handle(size * 1000)],
      })),
    ),
  ),
);

const optionsArbitrary: Arbitrary.Arbitrary<SizeTieredCompactionOptions> = Arbitrary.all({
  minCompactionSources: smallInt(2, 4),
  extra: smallInt(0, 4),
  includeSizeThreshold: smallInt(2, 4),
  maxConcurrentCompactions: smallInt(1, 4),
}).pipe(
  Arbitrary.map(
    ({ minCompactionSources, extra, includeSizeThreshold, maxConcurrentCompactions }) => ({
      minCompactionSources,
      maxCompactionSources: minCompactionSources + extra,
      includeSizeThreshold,
      maxConcurrentCompactions,
    }),
  ),
);

const decodeLsmSettings = Schema.decodeUnknownOption(LsmSettings);

/**
 * Settings an operator may configure (any that `LsmSettings` accepts), with the scheduler's and the
 * write path's L0 limits drawn across their whole small range.
 */
const validSettingsArbitrary = Arbitrary.all({
  l0MaxSsts: smallInt(1, 20),
  minCompactionSources: smallInt(2, 10),
  extra: smallInt(0, 6),
}).pipe(
  Arbitrary.map(({ l0MaxSsts, minCompactionSources, extra }) =>
    decodeLsmSettings({
      ...DEFAULT_LSM_SETTINGS,
      l0MaxSsts,
      minCompactionSources,
      maxCompactionSources: minCompactionSources + extra,
    }),
  ),
);

const sourceKey = (source: CompactionJob["sources"][number]) =>
  source._tag === "L0" ? `l0:${source.sstId}` : `sr:${source.id}`;

const runSize = (manifest: Manifest, id: number) =>
  manifest.sortedRuns
    .find((run) => run.id === id)
    ?.ssts.reduce((sum, sst) => sum + sst.sizeBytes, 0) ?? 0;

/** Whether one job satisfies the scheduler's invariants against `manifest`. */
const jobIsValid = (
  manifest: Manifest,
  job: CompactionJob,
  options: SizeTieredCompactionOptions,
): boolean => {
  const l0Order = manifest.l0.map((sst) => sst.id);
  const runOrder = manifest.sortedRuns.map((run) => run.id);
  const positions = job.sources.map((source) =>
    source._tag === "L0" ? l0Order.indexOf(source.sstId) : runOrder.indexOf(source.id),
  );
  const consecutive = positions.every(
    (position, index) =>
      position >= 0 && (index === 0 || position === (positions[index - 1] ?? 0) + 1),
  );
  const allL0 = job.sources.every((source) => source._tag === "L0");
  const allRuns = job.sources.every((source) => source._tag === "SortedRun");
  const runIds = job.sources.flatMap((source) => (source._tag === "SortedRun" ? [source.id] : []));
  const sizes = runIds.map((id) => runSize(manifest, id));
  const withinThreshold = sizes.every((size, index) => {
    // Every run is within the threshold of the smallest run before it in the group (SlateDB's rule).
    const smallestBefore = Math.min(...sizes.slice(0, index + 1));
    return index === 0 || size <= Math.max(1, smallestBefore) * options.includeSizeThreshold;
  });
  const destinationOk = allL0
    ? job.destination > Math.max(-1, ...runOrder) &&
      positions[positions.length - 1] === l0Order.length - 1
    : job.destination === Math.min(...runIds);
  const isBottomOk = allL0
    ? job.isBottom === (runOrder.length === 0)
    : job.isBottom === (job.destination === Math.min(...runOrder));
  return (
    (allL0 || allRuns) &&
    consecutive &&
    withinThreshold &&
    destinationOk &&
    isBottomOk &&
    job.sources.length >= options.minCompactionSources &&
    job.sources.length <= options.maxCompactionSources
  );
};

describe("planCompactions", () => {
  it("plans a job that shrinks L0 whenever writes are refused for L0 backlog and nothing runs (D87)", () => {
    assertProperty(
      Arbitrary.all({ settings: validSettingsArbitrary, base: manifestArbitrary }),
      ({ settings, base }) => {
        // Settings the schema refuses never reach a writer or a compactor.
        if (Option.isNone(settings)) return true;
        const { l0MaxSsts } = settings.value;
        // L0 exactly at the limit at which the writer refuses writes; any sorted runs.
        const manifest = withLsm(
          Array.from({ length: l0MaxSsts }, () => handle(1000)),
          base.sortedRuns,
        );
        const planned = planCompactions(manifest, [], settings.value);
        return planned.some((job) => job.sources.some((source) => source._tag === "L0"));
      },
      { runs: 300 },
    );
  });

  it("plans valid jobs that never overlap each other or a job in flight", () => {
    assertProperty(
      Arbitrary.all({ manifest: manifestArbitrary, options: optionsArbitrary }),
      ({ manifest, options }) => {
        const first = planCompactions(manifest, [], options);
        // Plan again with the first jobs in flight: new jobs must avoid their sources and destinations.
        const second = planCompactions(manifest, first, {
          ...options,
          maxConcurrentCompactions: options.maxConcurrentCompactions * 2,
        });
        const busy = new Set(
          first.flatMap((job) => [...job.sources.map(sourceKey), `sr:${job.destination}`]),
        );
        const seen = new Set<string>();
        const disjoint = first.every((job) =>
          job.sources.every((source) => {
            const key = sourceKey(source);
            const fresh = !seen.has(key);
            seen.add(key);
            return fresh;
          }),
        );
        return (
          first.length <= options.maxConcurrentCompactions &&
          first.length + second.length <= options.maxConcurrentCompactions * 2 &&
          disjoint &&
          [...first, ...second].every((job) => jobIsValid(manifest, job, options)) &&
          second.every(
            (job) =>
              job.sources.every((source) => !busy.has(sourceKey(source))) &&
              !busy.has(`sr:${job.destination}`),
          )
        );
      },
      { runs: 400 },
    );
  });

  it("keeps the number of sorted runs logarithmic in the data over a long run of flushes (Workflow audit)", () => {
    // 2,000 flushes of one L0 SST each, every planned job committed at once, with the default
    // options: size-tiered merging keeps fewer than minCompactionSources runs per size tier, and
    // tiers grow at least includeSizeThreshold-fold, so runs stay within that many per tier.
    const options = DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS;
    let current = withLsm([], []);
    let mostRuns = 0;
    let flushes = 0;
    const sizeOf = (manifest: Manifest) =>
      manifest.sortedRuns.reduce(
        (sum, run) => sum + run.ssts.reduce((inner, sst) => inner + sst.sizeBytes, 0),
        0,
      ) + manifest.l0.reduce((sum, sst) => sum + sst.sizeBytes, 0);
    for (; flushes < 2_000; flushes++) {
      current = withLsm([handle(1000), ...current.l0], current.sortedRuns);
      for (;;) {
        const [job] = planCompactions(current, [], options);
        if (job === undefined) break;
        const inputs = job.sources.flatMap((source) =>
          source._tag === "L0"
            ? current.l0.filter((sst) => sst.id === source.sstId)
            : (current.sortedRuns.find((run) => run.id === source.id)?.ssts ?? []),
        );
        const output = handle(inputs.reduce((sum, sst) => sum + sst.sizeBytes, 0));
        current = Result.getOrThrow(
          commitCompaction(current, {
            compactorEpoch: current.compactorEpoch,
            jobId: `job-${++nextJobId}`,
            job,
            outputs: [output],
          }),
        );
      }
      mostRuns = Math.max(mostRuns, current.sortedRuns.length);
    }
    const tiers =
      Math.ceil(Math.log(sizeOf(current) / 1000) / Math.log(options.includeSizeThreshold)) + 1;
    expect(current.l0.length).toBeLessThan(options.minCompactionSources);
    expect(mostRuns).toBeLessThanOrEqual(options.minCompactionSources * tiers);
  });

  it("converges: committing planned jobs until none remain terminates with fewer sources than the minimum per group", () => {
    assertProperty(
      Arbitrary.all({ manifest: manifestArbitrary, options: optionsArbitrary }),
      ({ manifest, options }) => {
        let current = manifest;
        let rounds = 0;
        for (;;) {
          const jobs = planCompactions(current, [], options);
          if (jobs.length === 0) break;
          for (const job of jobs) {
            const inputs = job.sources.flatMap((source) =>
              source._tag === "L0"
                ? current.l0.filter((sst) => sst.id === source.sstId)
                : (current.sortedRuns.find((run) => run.id === source.id)?.ssts ?? []),
            );
            const output = handle(inputs.reduce((sum, sst) => sum + sst.sizeBytes, 0));
            current = Result.getOrThrow(
              commitCompaction(current, {
                compactorEpoch: current.compactorEpoch,
                jobId: `job-${++nextJobId}`,
                job,
                outputs: [output],
              }),
            );
          }
          rounds++;
          // Each job removes at least minCompactionSources - 1 sources, so this bound cannot be hit.
          if (rounds > 40) return false;
        }
        return current.l0.length < options.minCompactionSources;
      },
      { runs: 300 },
    );
  });

  it("rewrites a fork's external SSTs with one full compaction once nothing else is planned", () => {
    const external = { ...handle(10), owner: Option.some(DatabaseId.make("source")) };
    const own = handle(10);
    const fork = withLsm([own], [{ id: SortedRunId.make(3), ssts: [external] }]);
    const [job, more] = planCompactions(fork, [], DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS);
    expect(more).toBeUndefined();
    expect(job).toEqual({
      sources: [
        { _tag: "L0", sstId: own.id },
        { _tag: "SortedRun", id: 3 },
      ],
      destination: 3,
      isBottom: true,
    });
    // Not while another job runs, and never for a database reading only its own SSTs.
    expect(
      planCompactions(fork, job === undefined ? [] : [job], DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS),
    ).toEqual([]);
    expect(planCompactions(withLsm([own], []), [], DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS)).toEqual(
      [],
    );
  });

  it("compacts all L0 SSTs (up to the maximum) into a new, newer sorted run", () => {
    let manifest = empty;
    for (const n of [1, 2, 3, 4, 5]) {
      manifest = Result.getOrThrow(
        commitL0Flush(manifest, {
          writerEpoch: manifest.writerEpoch,
          sst: handle(1000),
          lastSeq: SeqNum.make(n),
          clockTick: Timestamp.make(n),
          changelog: Option.none(),
        }),
      );
    }
    const [job] = planCompactions(manifest, [], DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS);
    expect(job?.sources.map((source) => (source._tag === "L0" ? source.sstId : ""))).toEqual(
      manifest.l0.map((sst) => sst.id),
    );
    expect([job?.destination, job?.isBottom]).toEqual([0, true]);
  });

  it("never plans a job over a blocked (corrupted) source, and plans no fork rewrite while one is blocked", () => {
    const runs = (ids: ReadonlyArray<number>): ReadonlyArray<SortedRun> =>
      ids.map((id) => ({ id: SortedRunId.make(id), ssts: [handle(1000)] }));
    const manifest = withLsm([], runs([7, 6, 5, 4, 3, 2, 1, 0]));
    const options = { ...DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS, maxConcurrentCompactions: 4 };
    const blocked = [{ _tag: "SortedRun" as const, id: SortedRunId.make(5) }];
    const planned = planCompactionsAround(manifest, [], blocked, options);
    const sources = planned.flatMap((job) => job.sources);
    expect(sources.some((source) => source._tag === "SortedRun" && source.id === 5)).toBe(false);
    // Without the block, the same manifest compacts run 5.
    expect(
      planCompactions(manifest, [], options)
        .flatMap((job) => job.sources)
        .some((source) => source._tag === "SortedRun" && source.id === 5),
    ).toBe(true);
  });
});
