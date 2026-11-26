import { Effect, Fiber, Result } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { hedgedBlockRead } from "./block-source.ts";

const run = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(Effect.provide(TestClock.layer()), Effect.runPromise);

/** A read that answers `value` after `millis`, recording when it starts and if it is interrupted. */
const readAfter = (millis: number, value: string, log: Array<string>) =>
  Effect.sync(() => log.push(`${value} started`)).pipe(
    Effect.andThen(Effect.sleep(millis)),
    Effect.as(value),
    Effect.onInterrupt(() => Effect.sync(() => log.push(`${value} interrupted`))),
  );

describe("hedgedBlockRead", () => {
  it("answers from the BlockServer when it is fast, and never reads the bucket", async () => {
    const log: Array<string> = [];
    const answer = await run(
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          hedgedBlockRead(readAfter(10, "server", log), readAfter(40, "bucket", log), 50),
        );
        yield* TestClock.adjust("10 millis");
        return yield* Fiber.join(read);
      }),
    );
    expect(answer).toBe("server");
    expect(log).toEqual(["server started"]);
  });

  it("reads the bucket once the BlockServer has taken the hedge delay, and takes the first answer", async () => {
    const log: Array<string> = [];
    const outcome = await run(
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          hedgedBlockRead(readAfter(2000, "server", log), readAfter(40, "bucket", log), 50),
        );
        yield* TestClock.adjust("49 millis");
        const beforeDelay = [...log];
        yield* TestClock.adjust("1 millis");
        const atDelay = [...log];
        yield* TestClock.adjust("40 millis");
        return { beforeDelay, atDelay, answer: yield* Fiber.join(read) };
      }),
    );
    expect(outcome.beforeDelay).toEqual(["server started"]);
    expect(outcome.atDelay).toEqual(["server started", "bucket started"]);
    expect(outcome.answer).toBe("bucket");
    expect(log).toEqual(["server started", "bucket started", "server interrupted"]);
  });

  it("takes the BlockServer's answer when it arrives while the bucket read is in flight", async () => {
    const log: Array<string> = [];
    const answer = await run(
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          hedgedBlockRead(readAfter(60, "server", log), readAfter(40, "bucket", log), 50),
        );
        yield* TestClock.adjust("60 millis");
        return yield* Fiber.join(read);
      }),
    );
    expect(answer).toBe("server");
    expect(log).toEqual(["server started", "bucket started", "bucket interrupted"]);
  });

  it("a failed bucket read leaves the answer to the BlockServer", async () => {
    const log: Array<string> = [];
    const answer = await run(
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          hedgedBlockRead(readAfter(200, "server", log), Effect.fail("bucket unavailable"), 50),
        );
        yield* TestClock.adjust("200 millis");
        return yield* Fiber.join(read);
      }),
    );
    expect(answer).toBe("server");
  });

  it("a BlockServer failure is the read's failure, before or after the hedge started", async () => {
    const log: Array<string> = [];
    const outcome = await run(
      Effect.gen(function* () {
        const failAfter = (millis: number) =>
          Effect.sleep(millis).pipe(Effect.andThen(Effect.fail(`server failed at ${millis}`)));
        const early = yield* Effect.forkChild(
          Effect.result(hedgedBlockRead(failAfter(10), readAfter(40, "bucket", log), 50)),
        );
        yield* TestClock.adjust("10 millis");
        const beforeHedge = yield* Fiber.join(early);
        const startedBeforeHedge = log.length;
        const late = yield* Effect.forkChild(
          Effect.result(hedgedBlockRead(failAfter(70), readAfter(500, "bucket", log), 50)),
        );
        yield* TestClock.adjust("70 millis");
        return { beforeHedge, startedBeforeHedge, afterHedge: yield* Fiber.join(late) };
      }),
    );
    expect(outcome.beforeHedge).toEqual(Result.fail("server failed at 10"));
    expect(outcome.startedBeforeHedge).toBe(0);
    expect(outcome.afterHedge).toEqual(Result.fail("server failed at 70"));
    expect(log).toEqual(["bucket started", "bucket interrupted"]);
  });

  it("never reads the bucket when the hedge delay is 0", async () => {
    const log: Array<string> = [];
    const answer = await run(
      Effect.gen(function* () {
        const read = yield* Effect.forkChild(
          hedgedBlockRead(readAfter(5000, "server", log), readAfter(1, "bucket", log), 0),
        );
        yield* TestClock.adjust("5 seconds");
        return yield* Fiber.join(read);
      }),
    );
    expect(answer).toBe("server");
    expect(log).toEqual(["server started"]);
  });
});
