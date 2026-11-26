import { DatabaseId, SstId } from "@shaledb/lsm-core";
import { ConfigProvider, Effect, Fiber, Layer, Option, Result } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { BlockSource, hedgeBlockSource, hedgedBlockRead } from "./block-source.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { LSM_JURISDICTION_SETTING } from "./lsm-jurisdiction.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectReader } from "./object-reader.ts";
import { ObjectStore, objectStoreTestLayer } from "./object-store.ts";

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

/** A fork's reader asks for a block of an SST its source database owns. */
const SOURCE = DatabaseId.make("orders-source");
const SST = SstId.make("01JD0000000000000000000AAA");
const SST_BYTES = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);

/**
 * Reads `offset`/`length` of the source's SST as `block` and as `meta` through
 * `hedgeBlockSource` over a `BlockServer` that never answers, with `jurisdiction` bound and
 * `hedgeAfterMs` configured, advancing the test clock by `waitMs`.
 *
 * @returns What each read answered within `waitMs` (none: still waiting), and how many range
 *   reads reached the bucket.
 */
const readThroughUnansweredBlockServer = (options: {
  readonly jurisdiction: string;
  readonly hedgeAfterMs: number;
  readonly waitMs: number;
}) =>
  Effect.gen(function* () {
    const store = yield* ObjectStore;
    yield* store.put(sstKey(SOURCE, SST), SST_BYTES);
    let bucketReads = 0;
    const countingReader = ObjectReader.of({
      get: store.get,
      // Counted when the read runs, not when the hedge builds it.
      getRange: (key, range) =>
        Effect.suspend(() => {
          bucketReads++;
          return store.getRange(key, range);
        }),
    });
    const unanswered = BlockSource.of({
      block: () => Effect.never,
      meta: () => Effect.never,
      purge: () => Effect.succeed(true),
    });
    const source = yield* hedgeBlockSource(unanswered).pipe(
      Effect.provideService(ObjectReader, countingReader),
      Effect.provide(lsmConfigurationTestLayer({ blockReadHedgeAfterMs: options.hedgeAfterMs })),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({ [LSM_JURISDICTION_SETTING]: options.jurisdiction }),
      ),
    );
    const block = yield* Effect.forkChild(Effect.scoped(source.block(SOURCE, SST, 2, 4)));
    const meta = yield* Effect.forkChild(Effect.scoped(source.meta(SOURCE, SST, 0, 8)));
    yield* TestClock.adjust(options.waitMs);
    const answered = (fiber: Fiber.Fiber<Uint8Array, unknown>) =>
      Effect.sync(() => {
        const exit = fiber.pollUnsafe();
        return exit !== undefined && exit._tag === "Success"
          ? Option.some([...exit.value])
          : Option.none<ReadonlyArray<number>>();
      });
    return {
      block: yield* answered(block),
      meta: yield* answered(meta),
      bucketReads,
    };
  }).pipe(
    Effect.provide(Layer.merge(objectStoreTestLayer, lsmTestRuntimeContextLayer)),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

describe("hedgeBlockSource", () => {
  it("answers block and metadata reads from the source's SST in the bucket once the hedge delay has passed", async () => {
    const early = await readThroughUnansweredBlockServer({
      jurisdiction: "none",
      hedgeAfterMs: 50,
      waitMs: 49,
    });
    expect(early).toEqual({ block: Option.none(), meta: Option.none(), bucketReads: 0 });
    const late = await readThroughUnansweredBlockServer({
      jurisdiction: "none",
      hedgeAfterMs: 50,
      waitMs: 50,
    });
    expect(late).toEqual({
      block: Option.some([3, 4, 5, 6]),
      meta: Option.some([...SST_BYTES]),
      bucketReads: 2,
    });
  });

  it("never reads the bucket with a hedge delay of 0", async () => {
    const outcome = await readThroughUnansweredBlockServer({
      jurisdiction: "none",
      hedgeAfterMs: 0,
      waitMs: 60_000,
    });
    expect(outcome).toEqual({ block: Option.none(), meta: Option.none(), bucketReads: 0 });
  });

  it("never reads the bucket in a Stack restricted to a jurisdiction, however slow the BlockServer is", async () => {
    for (const jurisdiction of ["eu", "fedramp"]) {
      const outcome = await readThroughUnansweredBlockServer({
        jurisdiction,
        hedgeAfterMs: 50,
        waitMs: 60_000,
      });
      expect(outcome).toEqual({ block: Option.none(), meta: Option.none(), bucketReads: 0 });
    }
  });
});
