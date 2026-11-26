import {
  DatabaseId,
  DEFAULT_LSM_SETTINGS,
  type Row,
  RowValue,
  SeqNum,
  SstBuilder,
  SstId,
  sstHandleFromInfo,
  type SstHandle,
} from "@cf-slatedb/lsm-core";
import { Bytes, Effect, Fiber, Layer, Option, Ref } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { BlockCache, blockCacheLayerWithoutDependencies } from "./block-cache.ts";
import {
  BlockSource,
  type BlockSourceServices,
  BlockSourceUnavailableError,
} from "./block-source.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";

const OWNER = DatabaseId.make("owner");

/** An SST of `blocks` data blocks of about 1 KiB each. */
const buildSst = (id: string, blocks: number) => {
  const builder = new SstBuilder({
    kind: "Compacted",
    blockSize: 1024,
    filterBitsPerKey: 10,
    minFilterKeys: 0,
  });
  const parts: Array<Uint8Array> = [];
  for (let index = 0; index < blocks * 8; index++) {
    const row: Row = {
      key: new TextEncoder().encode(`key-${index.toString().padStart(6, "0")}`),
      seq: SeqNum.make(index + 1),
      value: RowValue.Value({ bytes: new Uint8Array(120).fill(index % 251) }),
      createTs: Option.none(),
      expireTs: Option.none(),
    };
    parts.push(...builder.add(row));
  }
  const { tail, info } = builder.finish();
  parts.push(tail);
  const sstId = SstId.make(id);
  return { bytes: Bytes.concat(parts), handle: sstHandleFromInfo(sstId, info) };
};

/** A block source over in-memory SSTs that counts reads per range and can fail on demand. */
const recordingSource = (ssts: ReadonlyArray<{ bytes: Uint8Array; handle: SstHandle }>) =>
  Effect.gen(function* () {
    const reads = yield* Ref.make(new Map<string, number>());
    const failing = yield* Ref.make(false);
    const hanging = yield* Ref.make(false);
    const read = (sstId: string, offset: number, length: number) =>
      Effect.gen(function* () {
        // A read that never finishes, as one whose request the runtime cancelled.
        if (yield* Ref.get(hanging)) return yield* Effect.never;
        yield* Ref.update(reads, (current) => {
          const key = `${sstId}/${offset}`;
          return new Map(current).set(key, (current.get(key) ?? 0) + 1);
        });
        // Let concurrent readers of one range meet while this read is in flight.
        yield* Effect.yieldNow;
        if (yield* Ref.get(failing)) {
          return yield* new BlockSourceUnavailableError({
            owner: OWNER,
            sstId: Option.none(),
            message: "unavailable",
          });
        }
        const sst = ssts.find((candidate) => candidate.handle.id === sstId);
        return (sst?.bytes ?? new Uint8Array()).subarray(offset, offset + length);
      });
    const source = BlockSource.of({
      block: (_owner, sstId, offset, length) => read(sstId, offset, length),
      meta: (_owner, sstId, offset, length) => read(sstId, offset, length),
      purge: () => Effect.succeed(true),
    });
    return { source, reads, failing, hanging };
  });

const run = <A, E>(
  ssts: ReadonlyArray<{ bytes: Uint8Array; handle: SstHandle }>,
  budget: { readonly data: number; readonly share: number },
  body: (
    recording: Effect.Success<ReturnType<typeof recordingSource>>,
  ) => Effect.Effect<A, E, BlockCache | BlockSourceServices>,
) =>
  Effect.gen(function* () {
    const recording = yield* recordingSource(ssts);
    return yield* body(recording).pipe(
      Effect.provide(
        blockCacheLayerWithoutDependencies.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(BlockSource, recording.source),
              lsmConfigurationTestLayer({
                ...DEFAULT_LSM_SETTINGS,
                l1DataCacheBytes: budget.data,
                l1GroupCacheShare: budget.share,
              }),
            ),
          ),
        ),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(lsmTestRuntimeContextLayer), Effect.runPromise);

const readAllBlocks = (sst: { handle: SstHandle }, group: string) =>
  Effect.gen(function* () {
    const cache = yield* BlockCache;
    const meta = yield* cache.meta(OWNER, sst.handle, group);
    for (const entry of meta.index) yield* cache.block(OWNER, sst.handle, entry, group);
    return meta.index;
  });

describe("BlockCache", () => {
  it("keeps a block once read, so later reads of it make no request", async () => {
    const sst = buildSst("01JD00000000000000000000A1", 4);
    const reads = await run([sst], { data: 1 << 20, share: 1 }, ({ reads }) =>
      Effect.gen(function* () {
        const cache = yield* BlockCache;
        const meta = yield* cache.meta(OWNER, sst.handle, "g");
        const entry = meta.index[0];
        if (entry === undefined) return yield* Effect.die("no blocks");
        for (let read = 0; read < 8; read++) yield* cache.block(OWNER, sst.handle, entry, "g");
        return (yield* Ref.get(reads)).get(`${sst.handle.id}/${entry.offset}`);
      }),
    );
    expect(reads).toBe(1);
  });

  it("never makes a miss wait on another invocation's read, even one that never finishes (D66)", async () => {
    const sst = buildSst("01JD00000000000000000000A2", 4);
    const outcome = await run([sst], { data: 1 << 20, share: 1 }, ({ hanging }) =>
      Effect.gen(function* () {
        const cache = yield* BlockCache;
        const meta = yield* cache.meta(OWNER, sst.handle, "g");
        const entry = meta.index[0];
        if (entry === undefined) return yield* Effect.die("no blocks");
        // Another invocation's read of the same block, stuck.
        yield* Ref.set(hanging, true);
        const stuck = yield* Effect.forkChild(cache.block(OWNER, sst.handle, entry, "g"));
        yield* Effect.yieldNow;
        yield* Ref.set(hanging, false);
        const block = yield* cache
          .block(OWNER, sst.handle, entry, "g")
          .pipe(Effect.timeoutOption("1 second"));
        yield* Fiber.interrupt(stuck);
        return Option.isSome(block);
      }),
    );
    expect(outcome).toBe(true);
  });

  it("never keeps a failed read", async () => {
    const sst = buildSst("01JD00000000000000000000A2", 2);
    const reads = await run([sst], { data: 1 << 20, share: 1 }, ({ reads, failing }) =>
      Effect.gen(function* () {
        const cache = yield* BlockCache;
        yield* Ref.set(failing, true);
        const failed = yield* Effect.flip(cache.meta(OWNER, sst.handle, "g"));
        yield* Ref.set(failing, false);
        yield* cache.meta(OWNER, sst.handle, "g");
        return {
          failed: failed._tag,
          count: (yield* Ref.get(reads)).get(`${sst.handle.id}/${sst.handle.metaOffset}`),
        };
      }),
    );
    expect(reads).toEqual({ failed: "BlockSourceUnavailableError", count: 2 });
  });

  it("bounds each budget group by bytes, so one group's scan never evicts another group's blocks", async () => {
    const small = buildSst("01JD00000000000000000000A3", 4);
    const large = buildSst("01JD00000000000000000000A4", 64);
    // 32 KiB of data blocks in total, half per group: the small SST fits; the large one does not.
    const result = await run([small, large], { data: 32 * 1024, share: 0.5 }, ({ reads }) =>
      Effect.gen(function* () {
        const quiet = yield* readAllBlocks(small, "quiet");
        const before = new Map(yield* Ref.get(reads));
        yield* readAllBlocks(large, "busy");
        yield* readAllBlocks(large, "busy");
        yield* readAllBlocks(small, "quiet");
        const after = yield* Ref.get(reads);
        return {
          quietRereads: quiet.filter(
            (entry) =>
              after.get(`${small.handle.id}/${entry.offset}`) !==
              before.get(`${small.handle.id}/${entry.offset}`),
          ).length,
          busyRereads: [...after.entries()].filter(
            ([key, count]) => key.startsWith(large.handle.id) && count > 1,
          ).length,
        };
      }),
    );
    // The busy group cycled through more than its share and re-read its own blocks...
    expect(result.busyRereads).toBeGreaterThan(0);
    // ...while the quiet group's blocks all stayed cached.
    expect(result.quietRereads).toBe(0);
  });

  it("holds one group to its share even when the isolate's total budget has room", async () => {
    const sst = buildSst("01JD00000000000000000000A5", 24);
    // 24 KiB of blocks: within the 64 KiB total, above one group's 25% share (16 KiB).
    const rereads = await run([sst], { data: 64 * 1024, share: 0.25 }, ({ reads }) =>
      Effect.gen(function* () {
        yield* readAllBlocks(sst, "only");
        yield* readAllBlocks(sst, "only");
        return [...(yield* Ref.get(reads)).values()].filter((count) => count > 1).length;
      }),
    );
    expect(rereads).toBeGreaterThan(0);
  });
});
