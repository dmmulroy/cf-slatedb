import {
  type DatabaseId,
  decodeDataBlock,
  decodeSstMeta,
  type Manifest,
  manifestSstHandles,
  mergeRowStreams,
  type Row,
  sstHandleOwner,
} from "@shaledb/lsm-core";
import { Effect, Option, Result, Stream } from "effect";

import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";

/**
 * Reads every row version a manifest references, merged in LSM order, straight from the object
 * store: L0 newest first, then sorted runs. For tests that check what a manifest makes durable,
 * independently of the reader.
 */
export const manifestRows = (manifest: Manifest) =>
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const inputs: Array<Stream.Stream<Row>> = [];
    const handles = manifestSstHandles(manifest);
    for (const handle of handles) {
      const owner: DatabaseId = sstHandleOwner(handle, manifest.databaseId);
      const bytes = Option.getOrThrow(yield* objects.get(sstKey(owner, handle.id)));
      const meta = Result.getOrThrow(decodeSstMeta(handle.id, bytes.subarray(handle.metaOffset)));
      const rows: Array<Row> = [];
      for (const entry of meta.index) {
        const block = Result.getOrThrow(
          decodeDataBlock(bytes.subarray(entry.offset, entry.offset + entry.length), {
            sstId: handle.id,
            offset: entry.offset,
          }),
        );
        rows.push(...block.rows());
      }
      inputs.push(Stream.fromIterable(rows));
    }
    return yield* Stream.runCollect(mergeRowStreams(inputs));
  });
