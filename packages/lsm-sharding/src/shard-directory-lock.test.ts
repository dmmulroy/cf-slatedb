import { DatabaseId } from "@shaledb/lsm-core";
import { inMemoryCell, lsmTestRuntimeContextLayer } from "@shaledb/lsm-cloudflare";
import { Deferred, Effect, Fiber, Option, Ref } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { makeShardDirectoryOver } from "./shard-directory.ts";
import { type ShardMap, ShardGroup } from "./shard-map.ts";

const GROUP = ShardGroup.make("orders");
const SHARD = DatabaseId.make("orders-0");

describe("ShardDirectory edits under its lock (D116)", () => {
  it("two splits of one shard begun at once share one pending split", async () => {
    const outcome = await Effect.gen(function* () {
      // The group's cell, whose next read is held after it read: in production the read is a
      // Durable Object storage await.
      const inner = yield* inMemoryCell<ShardMap>();
      const gate = yield* Ref.make(
        Option.none<{ loaded: Deferred.Deferred<void>; resume: Deferred.Deferred<void> }>(),
      );
      const cell = {
        ...inner,
        get: Effect.gen(function* () {
          const current = yield* inner.get;
          const held = yield* Ref.getAndSet(gate, Option.none());
          if (Option.isSome(held)) {
            yield* Deferred.succeed(held.value.loaded, undefined);
            yield* Deferred.await(held.value.resume);
          }
          return current;
        }),
      };
      const directory = yield* makeShardDirectoryOver(() => cell);
      yield* directory.create(GROUP, SHARD);
      const loaded = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      yield* Ref.set(gate, Option.some({ loaded, resume }));
      // The first split's edit read the map (no split pending) and is held; a second request to
      // split the same shard at the same key arrives meanwhile.
      const atKey = Uint8Array.of(0x80);
      const first = yield* Effect.forkChild(directory.beginSplit(GROUP, SHARD, atKey));
      yield* Deferred.await(loaded);
      const second = yield* Effect.forkChild(directory.beginSplit(GROUP, SHARD, atKey));
      for (let step = 0; step < 100; step++) yield* Effect.yieldNow;
      yield* Deferred.succeed(resume, undefined);
      const a = yield* Fiber.join(first);
      const b = yield* Fiber.join(second);
      const recorded = Option.getOrThrow((yield* directory.map(GROUP)).pendingSplit);
      return {
        same: a.left === b.left && a.right === b.right,
        firstIsRecorded: recorded.left === a.left && recorded.right === a.right,
      };
    }).pipe(Effect.provide(lsmTestRuntimeContextLayer), Effect.runPromise);
    // Both split processes seal and fork with the successors the directory publishes.
    expect(outcome).toEqual({ same: true, firstIsRecorded: true });
  });
});
