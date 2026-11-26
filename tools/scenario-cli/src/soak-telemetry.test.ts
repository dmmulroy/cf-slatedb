import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import type { InvocationRecord } from "./measurements.ts";
import {
  catchUpTails,
  every,
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
