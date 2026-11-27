import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import type { InvocationRecord } from "./measurements.ts";
import {
  catchUpTails,
  every,
  makeSpanPuller,
  pullCursorTimeMs,
  readTailsWithin,
  readWithin,
  TailTally,
  type TailPageSource,
  whileWorkloadRuns,
} from "./soak-telemetry.ts";

const record = (at: number, outcome = "ok"): InvocationRecord => ({
  scriptName: Option.some("shaledb-dbapiworker-preview-1"),
  durableObjectId: Option.none(),
  eventKind: "fetch",
  method: Option.some("POST"),
  path: Option.some("/v1/databases/a/batches"),
  status: Option.some(200),
  outcome,
  eventTimestamp: Option.some(at),
  cpuTimeMs: 3,
  wallTimeMs: 20,
  sampleRate: 1,
});

/** A collector on a busy stage: a page of 10 records every millisecond, never caught up. */
const neverEndingSource: TailPageSource<never, never> = (after, onPage) =>
  Effect.gen(function* () {
    let cursor = Number(after.split("/")[1]);
    for (;;) {
      yield* Effect.sleep("1 millis");
      const page = Array.from({ length: 10 }, (_, index) => record(cursor + index + 1));
      cursor += 10;
      onPage(page, `tails/${cursor.toString().padStart(15, "0")}`);
    }
  });

/** A collector holding `pages` pages of 10 records after the cursor, then no more. */
const finiteSource =
  (pages: number): TailPageSource<never, never> =>
  (after, onPage) =>
    Effect.gen(function* () {
      let cursor = after;
      for (let page = 0; page < pages; page++) {
        yield* Effect.sleep("1 millis");
        cursor = `${after}-${page}`;
        onPage(
          Array.from({ length: 10 }, (_, index) => record(1_000 + page * 10 + index)),
          cursor,
        );
      }
      return cursor;
    });

describe("bounded telemetry reads", () => {
  it("cuts a read that never ends off at its budget", async () => {
    const started = Date.now();
    const read = await Effect.runPromise(readWithin("50 millis", Effect.never));
    expect(read).toEqual({ _tag: "OutOfTime" });
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it("returns a read that finishes within its budget", async () => {
    const read = await Effect.runPromise(readWithin("1 second", Effect.succeed(7)));
    expect(read).toEqual({ _tag: "Finished", value: 7 });
  });

  it("cuts a never-ending tail source off and keeps the pages read before the budget ran out", async () => {
    const tally = new TailTally(1);
    const read = await Effect.runPromise(readTailsWithin(tally, "100 millis", neverEndingSource));
    expect(read).toEqual({ _tag: "OutOfTime" });
    expect(tally.recordsRead).toBeGreaterThan(0);
    expect(tally.recordsRead % 10).toBe(0);
    // The cursor sits after the last page counted, so the next read neither skips nor recounts.
    expect(tally.cursor).toBe(`tails/${tally.recordsRead.toString().padStart(15, "0")}`);
    expect(tally.newestRecordAt).toEqual(Option.some(tally.recordsRead));
  });

  it("records a final catch-up that runs out of time as a failure saying how far it got", async () => {
    const tally = new TailTally(1);
    const failure = await Effect.runPromise(catchUpTails(tally, "100 millis", neverEndingSource));
    expect(Option.isSome(failure)).toBe(true);
    const message = Option.getOrThrow(failure);
    expect(message).toContain("not all read within");
    expect(message).toContain(`${tally.recordsRead} records read`);
    expect(message).toContain(
      `the newest from ${new Date(Option.getOrThrow(tally.newestRecordAt)).toISOString()}`,
    );
  });

  it("reports nothing when the final catch-up finishes", async () => {
    const tally = new TailTally(1);
    const failure = await Effect.runPromise(catchUpTails(tally, "5 seconds", finiteSource(3)));
    expect(failure).toEqual(Option.none());
    expect(tally.recordsRead).toBe(30);
    expect(tally.invocationOutcomes.get("dbapiworker ok")).toBe(30);
  });

  it("records a catch-up that keeps failing, with how far it got", async () => {
    const tally = new TailTally(1);
    const failing: TailPageSource<{ readonly message: string }, never> = () =>
      Effect.fail({ message: "collector answered 503" });
    const failure = await Effect.runPromise(
      catchUpTails(tally, "5 seconds", failing, 2, "1 millis"),
    );
    expect(Option.getOrThrow(failure)).toContain("could not be read after 2 tries");
    expect(Option.getOrThrow(failure)).toContain("collector answered 503");
    expect(Option.getOrThrow(failure)).toContain("0 records read");
  });
});

describe("watchers", () => {
  it("are interrupted when the workload ends, in the middle of a read that never ends", async () => {
    const tally = new TailTally(1);
    let watcherStopped = false;
    const watcher = every(
      "1 millis",
      Effect.asVoid(readTailsWithin(tally, "1 hour", neverEndingSource)),
    ).pipe(Effect.onInterrupt(() => Effect.sync(() => (watcherStopped = true))));
    const started = Date.now();
    const result = await Effect.runPromise(
      whileWorkloadRuns(Effect.as(Effect.sleep("100 millis"), "workload done"), watcher),
    );
    expect(result).toBe("workload done");
    expect(watcherStopped).toBe(true);
    expect(Date.now() - started).toBeLessThan(2_000);
    // It was reading when it was stopped, and stops counting once stopped.
    expect(tally.recordsRead).toBeGreaterThan(0);
    const counted = tally.recordsRead;
    await Effect.runPromise(Effect.sleep("30 millis"));
    expect(tally.recordsRead).toBe(counted);
  });

  it("are interrupted when the workload fails", async () => {
    let watcherStopped = false;
    const watcher = Effect.never.pipe(
      Effect.onInterrupt(() => Effect.sync(() => (watcherStopped = true))),
    );
    const exit = await Effect.runPromiseExit(
      whileWorkloadRuns(
        Effect.andThen(Effect.sleep("20 millis"), Effect.fail("workload failed")),
        watcher,
      ),
    );
    expect(exit._tag).toBe("Failure");
    expect(watcherStopped).toBe(true);
  });
});

/**
 * A stage's collector as the span pull sees it: each pull stores exports one at a time (the
 * cursor moves after each), `exportsPerPull` of them if it is not cut off first.
 */
const spanCollector = (exportsPerPull: number, exportMs = 10) => {
  let cursorMs = 1_000;
  let running = 0;
  let mostAtOnce = 0;
  const pull = Effect.gen(function* () {
    running += 1;
    mostAtOnce = Math.max(mostAtOnce, running);
    yield* Effect.gen(function* () {
      for (let stored = 0; stored < exportsPerPull; stored++) {
        yield* Effect.sleep(`${exportMs} millis`);
        cursorMs += 1_000;
      }
    }).pipe(Effect.ensuring(Effect.sync(() => (running -= 1))));
  });
  return {
    pull,
    pulledThrough: Effect.sync(() => Option.some(cursorMs)),
    mostAtOnce: () => mostAtOnce,
  };
};

describe("the soak's span pulls (D132)", () => {
  it("reads how far a pull got from its cursor", () => {
    expect(pullCursorTimeMs("traces/001795754765700-d354cbb3.json")).toEqual(
      Option.some(1_795_754_765_700),
    );
    expect(pullCursorTimeMs("tails/001795754765700")).toEqual(Option.none());
  });

  it("cuts a pull that cannot keep up off at its budget, keeping how far it got, and the next resumes", async () => {
    const collector = spanCollector(1_000_000);
    const result = await Effect.gen(function* () {
      const puller = yield* makeSpanPuller(collector.pull, collector.pulledThrough);
      const first = yield* puller.pullWithin("50 millis");
      const afterFirst = Option.getOrThrow(puller.progress.pulledThroughMs);
      const second = yield* puller.pullWithin("50 millis");
      return { first, second, afterFirst, progress: puller.progress };
    }).pipe(Effect.runPromise);
    expect(result.first._tag).toBe("OutOfTime");
    expect(result.second._tag).toBe("OutOfTime");
    // Some exports were stored before the cut, and the second pull went on from there.
    expect(result.afterFirst).toBeGreaterThan(1_000);
    expect(Option.getOrThrow(result.progress.pulledThroughMs)).toBeGreaterThan(result.afterFirst);
    expect(result.progress.toReport().spanPulls).toEqual({ finished: 0, cutOff: 2, failed: 0 });
  });

  it("runs one pull at a time: the final catch-up waits for the watcher's pull", async () => {
    const collector = spanCollector(5);
    const mostAtOnce = await Effect.gen(function* () {
      const puller = yield* makeSpanPuller(collector.pull, collector.pulledThrough);
      yield* Effect.all([puller.pullWithin("10 seconds"), puller.pullWithin("10 seconds")], {
        concurrency: "unbounded",
      });
      return collector.mostAtOnce();
    }).pipe(Effect.runPromise);
    expect(mostAtOnce).toBe(1);
  });

  it("records a failed pull and the lag at the workload's end", async () => {
    const report = await Effect.gen(function* () {
      const puller = yield* makeSpanPuller(
        Effect.fail({ message: "collector answered 503" }),
        Effect.succeed(Option.some(60_000)),
      );
      const read = yield* puller.pullWithin("1 second");
      puller.progress.markWorkloadEnd(150_000);
      return { read, report: puller.progress.toReport() };
    }).pipe(Effect.runPromise);
    expect(report.read).toEqual({ _tag: "Failed", message: "collector answered 503" });
    expect(report.report).toEqual({
      spansPulledThrough: new Date(60_000).toISOString(),
      spanPullLagAtWorkloadEndSeconds: 90,
      spanPulls: { finished: 0, cutOff: 0, failed: 1 },
    });
  });

  it("stops the watcher's pulls when the workload ends, wherever a pull is", async () => {
    const collector = spanCollector(1_000_000);
    const progress = await Effect.gen(function* () {
      const puller = yield* makeSpanPuller(collector.pull, collector.pulledThrough);
      yield* whileWorkloadRuns(
        Effect.sleep("200 millis"),
        every("10 millis", puller.pullWithin("5 minutes")),
      );
      return { puller, running: collector.mostAtOnce() };
    }).pipe(Effect.runPromise);
    // The pull was interrupted with the workload and released its lock: the next pull can start.
    const next = await Effect.runPromise(progress.puller.pullWithin("30 millis"));
    expect(next._tag).toBe("OutOfTime");
    expect(progress.running).toBe(1);
  });
});
