import {
  type DatabaseId,
  keyRangeOfPrefix,
  type KeyRange,
  type LsmSettings,
  ManifestId,
  type ReadConsistency,
  type Row,
  SeqNum,
  type WriteBatch,
  type WriteReceipt,
} from "@cf-slatedb/lsm-core";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";

import { blockCacheLayerWithoutDependencies } from "./block-cache.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import {
  DbReader,
  dbReaderLayer,
  dbReaderLayerWithoutDependencies,
  type DbReaderServices,
  type DbReadError,
  type ReadView,
} from "./db-reader.ts";
import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { manifestCacheLayerWithoutDependencies } from "./manifest-cache.ts";
import { manifestStoreTestLayer } from "./manifest-store.ts";
import type { MemTableCursor } from "./mem-table.ts";
import {
  WriterClient,
  type WriterClientError,
  writerClientLayer,
  writerClientTestLayer,
} from "./writer-client.ts";

/**
 * Schema for a snapshot: a read view pinned to one manifest, so repeated reads see the same data.
 * Its `snapshotSeq` is the manifest's `lastL0Seq`; flush first to include the newest writes.
 */
export const DbSnapshot = Schema.Struct({
  databaseId: Schema.String,
  manifestId: ManifestId,
  snapshotSeq: SeqNum,
});

/** A snapshot. */
export type DbSnapshot = typeof DbSnapshot.Type;

/** Options of a read. */
export interface ReadOptions {
  readonly consistency: ReadConsistency;
  /** Read at this snapshot instead (consistency is ignored). */
  readonly snapshot: Option.Option<DbSnapshot>;
}

/** Options of a scan. */
export interface ScanReadOptions extends ReadOptions {
  /** Most rows to return; none: all. */
  readonly limit: Option.Option<number>;
}

/** Failures a write can report. */
export type DbWriteError = WriterClientError;

/** Failures a read can report: SST read failures, and writer failures for `Latest` reads. */
export type DbClientReadError = DbReadError | WriterClientError;

/** Rows per memtable page a `Latest` scan reads from the writer. */
const MEMTABLE_PAGE_ROWS = 1000;

/** The public database API, used in-process by Workers (design §8). */
export interface IDbClient {
  /** Sets `key` to `value`; `ttlMillis` makes it expire. Durable when it returns. */
  readonly put: (
    databaseId: DatabaseId,
    key: Uint8Array,
    value: Uint8Array,
    options?: { readonly ttlMillis: Option.Option<number> },
  ) => Effect.Effect<WriteReceipt, DbWriteError, DbReaderServices>;
  /** Deletes `key`. Durable when it returns. */
  readonly delete: (
    databaseId: DatabaseId,
    key: Uint8Array,
  ) => Effect.Effect<WriteReceipt, DbWriteError, DbReaderServices>;
  /** Adds a merge operand for `key`, combined by the database's merge operator. */
  readonly merge: (
    databaseId: DatabaseId,
    key: Uint8Array,
    operand: Uint8Array,
    options?: { readonly ttlMillis: Option.Option<number> },
  ) => Effect.Effect<WriteReceipt, DbWriteError, DbReaderServices>;
  /** Commits a batch atomically with one sequence number. */
  readonly write: (
    databaseId: DatabaseId,
    batch: WriteBatch,
  ) => Effect.Effect<WriteReceipt, DbWriteError, DbReaderServices>;
  /** The key's visible value, or none. */
  readonly get: (
    databaseId: DatabaseId,
    key: Uint8Array,
    options: ReadOptions,
  ) => Effect.Effect<Option.Option<Uint8Array>, DbClientReadError, DbReaderServices>;
  /** The key's visible row (with `seq`, `createTs`, `expireTs`), or none. */
  readonly getRow: (
    databaseId: DatabaseId,
    key: Uint8Array,
    options: ReadOptions,
  ) => Effect.Effect<Option.Option<Row>, DbClientReadError, DbReaderServices>;
  /** Visible rows in `range`, in key order. */
  readonly scan: (
    databaseId: DatabaseId,
    range: KeyRange,
    options: ScanReadOptions,
  ) => Stream.Stream<Row, DbClientReadError, DbReaderServices>;
  /** Visible rows whose keys start with `prefix`, in key order. */
  readonly scanPrefix: (
    databaseId: DatabaseId,
    prefix: Uint8Array,
    options: ScanReadOptions,
  ) => Stream.Stream<Row, DbClientReadError, DbReaderServices>;
  /** Flushes the memtable to L0 now. @returns The new `flushedSeq`. */
  readonly flush: (databaseId: DatabaseId) => Effect.Effect<SeqNum, DbWriteError, DbReaderServices>;
  /** Pins a snapshot at the current flushed manifest. */
  readonly snapshot: (
    databaseId: DatabaseId,
  ) => Effect.Effect<DbSnapshot, DbClientReadError, DbReaderServices>;
}

/** The contextual database client. */
export class DbClient extends Context.Service<DbClient, IDbClient>()("@cf-slatedb/DbClient") {}

const FLUSHED_VIEW: ReadView = {
  minLastL0Seq: SeqNum.make(0),
  snapshotSeq: Option.none(),
  memtable: [],
  manifestId: Option.none(),
};

/** Constructs the client over the contextual writer client and reader. */
export const makeDbClient = Effect.gen(function* () {
  const writer = yield* WriterClient;
  const reader = yield* DbReader;

  const snapshotView = (snapshot: DbSnapshot): ReadView => ({
    minLastL0Seq: SeqNum.make(0),
    snapshotSeq: Option.some(snapshot.snapshotSeq),
    memtable: [],
    manifestId: Option.some(snapshot.manifestId),
  });

  /** Whether an `AtLeast` read can skip the writer: the cached manifest already covers `seq`. */
  const coveredByManifest = (databaseId: DatabaseId, consistency: ReadConsistency) =>
    consistency._tag === "AtLeast"
      ? Effect.map(
          reader.currentManifest(databaseId),
          (current) => current.manifest.lastL0Seq >= consistency.seq,
        )
      : Effect.succeed(consistency._tag === "Flushed");

  const pointView = (databaseId: DatabaseId, key: Uint8Array, options: ReadOptions) =>
    Effect.gen(function* () {
      if (Option.isSome(options.snapshot)) return snapshotView(options.snapshot.value);
      if (yield* coveredByManifest(databaseId, options.consistency)) {
        return options.consistency._tag === "AtLeast"
          ? { ...FLUSHED_VIEW, minLastL0Seq: options.consistency.seq }
          : FLUSHED_VIEW;
      }
      const probe = yield* writer.probe(databaseId, key);
      return {
        minLastL0Seq: probe.flushedSeq,
        snapshotSeq: Option.some(probe.snapshotSeq),
        memtable: probe.versions,
        manifestId: Option.none(),
      } satisfies ReadView;
    });

  /** The memtable rows of a `Latest` scan: every page at one snapshot, and the largest `flushedSeq`. */
  const scanView = (databaseId: DatabaseId, range: KeyRange, options: ReadOptions) =>
    Effect.gen(function* () {
      if (Option.isSome(options.snapshot)) return snapshotView(options.snapshot.value);
      if (yield* coveredByManifest(databaseId, options.consistency)) {
        return options.consistency._tag === "AtLeast"
          ? { ...FLUSHED_VIEW, minLastL0Seq: options.consistency.seq }
          : FLUSHED_VIEW;
      }
      const rows: Array<Row> = [];
      let cursor = Option.none<MemTableCursor>();
      let snapshotSeq = Option.none<SeqNum>();
      let flushedSeq = SeqNum.make(0);
      for (;;) {
        const page = yield* writer.scan(databaseId, {
          range,
          snapshotSeq,
          cursor,
          limit: MEMTABLE_PAGE_ROWS,
        });
        rows.push(...page.rows);
        snapshotSeq = Option.some(page.snapshotSeq);
        if (page.flushedSeq > flushedSeq) flushedSeq = page.flushedSeq;
        if (Option.isNone(page.nextCursor)) break;
        cursor = page.nextCursor;
      }
      return {
        minLastL0Seq: flushedSeq,
        snapshotSeq,
        memtable: rows,
        manifestId: Option.none(),
      } satisfies ReadView;
    });

  const getRow = Effect.fn("DbClient.getRow")(function* (
    databaseId: DatabaseId,
    key: Uint8Array,
    options: ReadOptions,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "read.consistency": options.consistency._tag,
    });
    const view = yield* pointView(databaseId, key, options);
    return yield* reader.getRow(databaseId, key, view);
  });

  const scan = (databaseId: DatabaseId, range: KeyRange, options: ScanReadOptions) =>
    Stream.unwrap(
      Effect.map(scanView(databaseId, range, options), (view) =>
        reader.scan(databaseId, range, view, { limit: options.limit }),
      ),
    ).pipe(
      Stream.withSpan("DbClient.scan", {
        attributes: { "database.id": databaseId, "read.consistency": options.consistency._tag },
      }),
    );

  const noTtl = { ttlMillis: Option.none<number>() };

  return DbClient.of({
    put: (databaseId, key, value, options = noTtl) =>
      writer.write(databaseId, {
        operations: [{ _tag: "Put", key, value, ttlMillis: options.ttlMillis }],
      }),
    delete: (databaseId, key) =>
      writer.write(databaseId, { operations: [{ _tag: "Delete", key }] }),
    merge: (databaseId, key, operand, options = noTtl) =>
      writer.write(databaseId, {
        operations: [{ _tag: "Merge", key, operand, ttlMillis: options.ttlMillis }],
      }),
    write: (databaseId, batch) => writer.write(databaseId, batch),
    get: (databaseId, key, options) =>
      getRow(databaseId, key, options).pipe(
        Effect.map(
          Option.flatMap((row) =>
            row.value._tag === "Value" ? Option.some(row.value.bytes) : Option.none(),
          ),
        ),
      ),
    getRow,
    scan,
    scanPrefix: (databaseId, prefix, options) =>
      scan(databaseId, keyRangeOfPrefix(prefix), options),
    flush: (databaseId) => writer.flush(databaseId),
    snapshot: Effect.fn("DbClient.snapshot")(function* (databaseId: DatabaseId) {
      const current = yield* reader.currentManifest(databaseId);
      return { databaseId, manifestId: current.id, snapshotSeq: current.manifest.lastL0Seq };
    }),
  });
});

/** Provides the client, leaving the writer client and reader visible. */
export const dbClientLayerWithoutDependencies = Layer.effect(DbClient, makeDbClient);

/**
 * Provides the client with its production dependencies: execution-scoped writer clients and the
 * reader over R2 manifests and the `BlockServer` block cache. Build it in a Worker's init.
 */
export const dbClientLayer = dbClientLayerWithoutDependencies.pipe(
  Layer.provide([writerClientLayer, dbReaderLayer]),
);

/**
 * Provides a client over one in-memory object store shared by in-process writers (crossing the real
 * writer HTTP API) and the reader (blocks read straight from the store). The Layer also outputs the
 * manifest store, manifest reader, object store, and a recording compaction trigger so tests can
 * inspect them.
 */
export const dbClientTestLayer = (overrides: Partial<LsmSettings> = {}) =>
  dbClientLayerWithoutDependencies.pipe(
    Layer.provideMerge(writerClientTestLayer),
    Layer.provideMerge(
      dbReaderLayerWithoutDependencies.pipe(
        Layer.provide(manifestCacheLayerWithoutDependencies),
        Layer.provide(blockCacheLayerWithoutDependencies.pipe(Layer.provide(blockSourceTestLayer))),
      ),
    ),
    Layer.provideMerge(manifestStoreTestLayer),
    Layer.provideMerge(
      Layer.mergeAll(lsmConfigurationTestLayer(overrides), compactionTriggerTestLayer),
    ),
  );
