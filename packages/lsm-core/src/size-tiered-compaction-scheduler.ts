import { SortedRunId, type SstId } from "./ids.ts";
import type { CompactionJob, CompactionSource, Manifest, SortedRun } from "./manifest.ts";

/** Options of the size-tiered scheduler (SlateDB's defaults: 4, 8, 4.0). */
export interface SizeTieredCompactionOptions {
  /** Fewest sources a compaction merges. */
  readonly minCompactionSources: number;
  /** Most sources a compaction merges. */
  readonly maxCompactionSources: number;
  /** A sorted run joins a group while its size is at most this multiple of the group's smallest. */
  readonly includeSizeThreshold: number;
  /** Most compactions running at once for one database. */
  readonly maxConcurrentCompactions: number;
}

/** SlateDB's default size-tiered options, with one concurrent compaction per database. */
export const DEFAULT_SIZE_TIERED_COMPACTION_OPTIONS: SizeTieredCompactionOptions = {
  minCompactionSources: 4,
  maxCompactionSources: 8,
  includeSizeThreshold: 4,
  maxConcurrentCompactions: 1,
};

const sortedRunSize = (run: SortedRun): number =>
  run.ssts.reduce((sum, sst) => sum + sst.sizeBytes, 0);

/**
 * Plans compactions for a manifest, given the jobs already running (a pure function).
 *
 * **Details**
 *
 * SlateDB's size-tiered policy:
 *
 * 1. When at least `minCompactionSources` L0 SSTs are not being compacted, merge the oldest of them
 *    (at most `maxCompactionSources`) into a new sorted run, newer than every existing one.
 * 2. Otherwise, find the newest group of consecutive sorted runs, none being compacted, where each
 *    run is at most `includeSizeThreshold` times the group's smallest; with at least
 *    `minCompactionSources` runs, merge the oldest `maxCompactionSources` of them into the group's
 *    oldest (lowest) id.
 *
 * Planning repeats until `maxConcurrentCompactions` jobs are running or planned. A job is
 * `isBottom` when no data older than its sources exists. Every planned job's sources are
 * consecutive and disjoint from every running job's sources and destination.
 */
export const planCompactions = (
  manifest: Manifest,
  running: ReadonlyArray<CompactionJob>,
  options: SizeTieredCompactionOptions,
): ReadonlyArray<CompactionJob> => {
  const busyL0 = new Set<SstId>();
  const busyRuns = new Set<SortedRunId>();
  let highestRunId = manifest.sortedRuns.reduce((max, run) => Math.max(max, run.id), -1);
  const reserve = (job: CompactionJob) => {
    for (const source of job.sources) {
      if (source._tag === "L0") busyL0.add(source.sstId);
      else busyRuns.add(source.id);
    }
    busyRuns.add(job.destination);
    highestRunId = Math.max(highestRunId, job.destination);
  };
  running.forEach(reserve);

  const lowestRunId = manifest.sortedRuns.reduce(
    (min, run) => Math.min(min, run.id),
    Number.POSITIVE_INFINITY,
  );
  const planned: Array<CompactionJob> = [];

  const planOne = (): CompactionJob | undefined => {
    // L0: only the oldest contiguous free L0 SSTs, so newer L0 SSTs stay newer than the output.
    const freeOldestL0: Array<SstId> = [];
    for (let index = manifest.l0.length - 1; index >= 0; index--) {
      const sst = manifest.l0[index];
      if (sst === undefined || busyL0.has(sst.id)) break;
      freeOldestL0.push(sst.id);
    }
    if (freeOldestL0.length >= options.minCompactionSources && busyL0.size === 0) {
      const chosen = freeOldestL0.slice(0, options.maxCompactionSources).reverse();
      return {
        sources: chosen.map((sstId): CompactionSource => ({ _tag: "L0", sstId })),
        destination: SortedRunId.make(highestRunId + 1),
        isBottom: manifest.sortedRuns.length === 0,
      };
    }
    // Sorted runs, newest first.
    const runs = manifest.sortedRuns;
    for (let start = 0; start < runs.length; start++) {
      const group: Array<SortedRun> = [];
      let smallest = Number.POSITIVE_INFINITY;
      for (let index = start; index < runs.length; index++) {
        const run = runs[index];
        if (run === undefined || busyRuns.has(run.id)) break;
        const size = sortedRunSize(run);
        if (group.length > 0 && size > Math.max(1, smallest) * options.includeSizeThreshold) break;
        smallest = Math.min(smallest, size);
        group.push(run);
      }
      if (group.length >= options.minCompactionSources) {
        const chosen = group.slice(-options.maxCompactionSources);
        const destination = chosen.reduce(
          (min, run) => Math.min(min, run.id),
          Number.POSITIVE_INFINITY,
        );
        return {
          sources: chosen.map((run): CompactionSource => ({ _tag: "SortedRun", id: run.id })),
          destination: SortedRunId.make(destination),
          isBottom: destination === lowestRunId,
        };
      }
    }
    return undefined;
  };

  while (running.length + planned.length < options.maxConcurrentCompactions) {
    const job = planOne();
    if (job === undefined) break;
    reserve(job);
    planned.push(job);
  }
  return planned;
};
