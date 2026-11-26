import {
  type DatabaseId,
  generateSstId,
  type Row,
  SstBuilder,
  type SstHandle,
  sstHandleFromInfo,
  type SstId,
  type SstInfo,
  type SstKind,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Metric, Option, Stream } from "effect";

import {
  LsmConfiguration,
  lsmConfigurationLayer,
  lsmConfigurationTestLayer,
} from "./lsm-configuration.ts";
import {
  ObjectStore,
  objectStoreLayer,
  objectStoreTestLayer,
  type ObjectStoreUnavailableError,
} from "./object-store.ts";

/** An SST that was written: its object key, manifest handle, and builder info. */
export interface WrittenSst {
  readonly key: string;
  readonly handle: SstHandle;
  readonly info: SstInfo;
}

/** Bytes written to SSTs (flushes and compactions), for SlateDB RFC-0021 parity. */
export const sstBytesWritten = Metric.counter("lsm_sst_bytes_written", {
  description: "Bytes written to SST objects by flushes and compactions",
});

/** Writes SSTs to object storage. */
export interface ISstStore {
  /**
   * Streams `rows` through the SST builder into one object, uploading closed blocks as multipart
   * parts so the SST is never fully buffered.
   *
   * **Details**
   *
   * `rows` must already be in the order `kind` requires. The SST id is a new ULID and `objectKey`
   * names the object from it.
   *
   * @returns The written SST, or none when `rows` was empty (nothing is left in storage).
   */
  readonly write: <E, R>(options: {
    readonly owner: DatabaseId;
    readonly kind: SstKind;
    readonly objectKey: (id: SstId) => string;
    readonly rows: Stream.Stream<Row, E, R>;
  }) => Effect.Effect<
    Option.Option<WrittenSst>,
    E | ObjectStoreUnavailableError,
    R | RuntimeContext
  >;
}

/** The contextual SST store. */
export class SstStore extends Context.Service<SstStore, ISstStore>()("@shaledb/SstStore") {}

/** Constructs the SST store over the contextual object store and settings. */
export const makeSstStore = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const settings = yield* LsmConfiguration;

  const write = <E, R>(options: {
    readonly owner: DatabaseId;
    readonly kind: SstKind;
    readonly objectKey: (id: SstId) => string;
    readonly rows: Stream.Stream<Row, E, R>;
  }) =>
    Effect.gen(function* () {
      const id = yield* generateSstId;
      const key = options.objectKey(id);
      yield* Effect.annotateCurrentSpan({
        "database.id": options.owner,
        "sst.id": id,
        "sst.kind": options.kind,
      });
      const builder = new SstBuilder({
        kind: options.kind,
        blockSize: settings.blockSize,
        filterBitsPerKey: settings.filterBitsPerKey,
        minFilterKeys: settings.minFilterKeys,
      });
      let info = Option.none<SstInfo>();
      const body = options.rows.pipe(
        // A chunk of rows at a time: one stream per row costs more than encoding the row.
        Stream.mapArray((rows) => [rows.flatMap((row: Row) => builder.add(row))] as const),
        Stream.flattenIterable,
        Stream.concat(
          Stream.suspend(() => {
            if (builder.count === 0) return Stream.empty;
            const finished = builder.finish();
            info = Option.some(finished.info);
            return Stream.make(finished.tail);
          }),
        ),
      );
      const size = yield* objects.putMultipart(key, body, settings.multipartPartSize);
      if (Option.isNone(info)) {
        // An empty input still created an empty object; remove it.
        yield* objects.delete([key]);
        return Option.none<WrittenSst>();
      }
      yield* Metric.update(sstBytesWritten, size);
      yield* Effect.annotateCurrentSpan({
        "sst.size": size,
        "sst.rows": info.value.footer.rowCount,
      });
      return Option.some({ key, handle: sstHandleFromInfo(id, info.value), info: info.value });
    }).pipe(Effect.withSpan("SstStore.write"));

  return SstStore.of({ write });
});

/** Provides the SST store, leaving the object store and settings visible. */
export const sstStoreLayerWithoutDependencies = Layer.effect(SstStore, makeSstStore);

/** Provides the SST store over the R2-backed object store and configured settings. */
export const sstStoreLayer = sstStoreLayerWithoutDependencies.pipe(
  Layer.provide([objectStoreLayer, lsmConfigurationLayer]),
);

/** Provides the SST store over a fresh in-memory object store and default settings. */
export const sstStoreTestLayer = sstStoreLayerWithoutDependencies.pipe(
  Layer.provide([objectStoreTestLayer, lsmConfigurationTestLayer()]),
);
