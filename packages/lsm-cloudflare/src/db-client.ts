import {
  type DatabaseId,
  keyRangeContains,
  keyRangeOfPrefix,
  type KeyRange,
  type LsmSettings,
  MAX_BATCH_OPERATIONS,
  ManifestId,
  MERGE_OPERATORS,
  type ReadConsistency,
  type Row,
  RowValue,
  SeqNum,
  type Timestamp,
  type TransactionCommit,
  TransactionConflictError,
  type TransactionIsolation,
  type WriteBatch,
  type WriteReceipt,
} from "@cf-slatedb/lsm-core";
import { Bytes, Context, Effect, Layer, Option, Schema, Stream } from "effect";

import { blockCacheLayerWithoutDependencies } from "./block-cache.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import {
  DbReader,
  dbReaderLayer,
  dbReaderLayerWithoutDependencies,
  type DbReaderServices,
  type DbReadError,
  type ChangelogUnavailableError,
  type ReaderUnavailableError,
  type ReadView,
} from "./db-reader.ts";
import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { manifestCacheLayerWithoutDependencies } from "./manifest-cache.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { ObjectStore, objectStoreTestLayer } from "./object-store.ts";
import { objectReaderFromObjectStoreLayer } from "./object-reader.ts";
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
  /**
   * Runs `body` as an optimistic transaction (SlateDB RFC 0011): its reads see the database at one
   * sequence number plus its own writes, its writes are buffered, and they commit atomically only if
   * no other commit since conflicts (`TransactionConflictError`, retryable; nothing was written).
   * With `SerializableSnapshot` every key it read and range it scanned is checked too.
   *
   * @returns The body's result, and the receipt when it wrote anything.
   */
  readonly transaction: <A, E, R>(
    databaseId: DatabaseId,
    isolation: TransactionIsolation,
    body: (transaction: DbTransaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<
    { readonly value: A; readonly receipt: Option.Option<WriteReceipt> },
    E | DbTransactionError,
    R | DbReaderServices
  >;
  /**
   * The change feed (design §3, CDC): committed changes after `afterSeq` in sequence order, at most
   * `limit` rows (never splitting a batch): flushed changes from the changelog SSTs, then unflushed
   * ones from the writer.
   */
  readonly changes: (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<
    ChangeFeedPage,
    DbClientReadError | ChangelogUnavailableError,
    DbReaderServices
  >;
  /** A transaction's read sequence number: everything committed now. */
  readonly beginTransaction: (
    databaseId: DatabaseId,
  ) => Effect.Effect<SeqNum, DbClientReadError, DbReaderServices>;
  /** The key's visible row at `readSeq` (a stateless transaction read). */
  readonly getAt: (
    databaseId: DatabaseId,
    key: Uint8Array,
    readSeq: SeqNum,
  ) => Effect.Effect<Option.Option<Row>, DbTransactionError, DbReaderServices>;
  /** Up to `limit` visible rows in `range` at `readSeq` (a stateless transaction read). */
  readonly scanAt: (
    databaseId: DatabaseId,
    range: KeyRange,
    readSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<Row>, DbTransactionError, DbReaderServices>;
  /** Commits a transaction's writes if nothing since its `readSeq` conflicts. */
  readonly commitTransaction: (
    databaseId: DatabaseId,
    commit: TransactionCommit,
  ) => Effect.Effect<WriteReceipt, DbWriteError, DbReaderServices>;
}

/** One page of the change feed: rows in sequence order, and where the next page starts. */
export interface ChangeFeedPage {
  readonly rows: ReadonlyArray<Row>;
  /** Pass as `afterSeq` for the next page (the last sequence number returned, or `afterSeq`). */
  readonly nextAfterSeq: SeqNum;
}

/**
 * Cuts rows in sequence order to at most `limit`, only between sequence numbers, so a page never
 * splits a batch (a consumer resuming after the page's last sequence number would skip the rest).
 * A single batch larger than `limit` is returned whole.
 */
export const cutAtSequenceBoundary = (
  rows: ReadonlyArray<Row>,
  limit: number,
): ReadonlyArray<Row> => {
  if (rows.length <= limit) return rows;
  const boundary = rows[limit]?.seq;
  const kept = rows.slice(0, limit).filter((row) => row.seq !== boundary);
  if (kept.length > 0) return kept;
  return rows.filter((row) => row.seq === boundary);
};

/** A transaction in progress (see `IDbClient.transaction`). */
export interface DbTransaction {
  readonly databaseId: DatabaseId;
  /** The sequence number its reads see. */
  readonly readSeq: SeqNum;
  readonly isolation: TransactionIsolation;
  /** The key's value at `readSeq`, with this transaction's own writes applied. */
  readonly get: (
    key: Uint8Array,
  ) => Effect.Effect<Option.Option<Uint8Array>, DbTransactionError, DbReaderServices>;
  /** Up to `limit` visible rows in `range` at `readSeq`, with this transaction's writes applied. */
  readonly scan: (
    range: KeyRange,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<Row>, DbTransactionError, DbReaderServices>;
  readonly put: (key: Uint8Array, value: Uint8Array) => Effect.Effect<void>;
  readonly delete: (key: Uint8Array) => Effect.Effect<void>;
  readonly merge: (key: Uint8Array, operand: Uint8Array) => Effect.Effect<void>;
}

/** Every failure a transaction's reads and commit can report. */
export type DbTransactionError = DbClientReadError | TransactionConflictError;

/** The contextual database client. */
export class DbClient extends Context.Service<DbClient, IDbClient>()("@cf-slatedb/DbClient") {}

const FLUSHED_VIEW: ReadView = {
  minLastL0Seq: SeqNum.make(0),
  snapshotSeq: Option.none(),
  memtable: [],
  manifestId: Option.none(),
  maxLastL0Seq: Option.none(),
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
    maxLastL0Seq: Option.none(),
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
        maxLastL0Seq: Option.none(),
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
        maxLastL0Seq: Option.none(),
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

  /** A read at a transaction's `readSeq` that found newer data flushed is a conflict. */
  const tooOld = (databaseId: DatabaseId, readSeq: SeqNum) =>
    new TransactionConflictError({
      databaseId,
      reason: "SnapshotTooOld",
      message: `Database ${databaseId} has flushed data newer than the transaction's reads (sequence number ${readSeq}). Begin a new transaction and run it again; nothing was written.`,
    });
  const expiredIsTooOld = (databaseId: DatabaseId, readSeq: SeqNum) => ({
    ReaderUnavailableError: (error: ReaderUnavailableError) =>
      error.reason === "SnapshotExpired"
        ? Effect.fail(tooOld(databaseId, readSeq))
        : Effect.fail(error),
  });

  const beginTransaction = Effect.fn("DbClient.beginTransaction")(function* (
    databaseId: DatabaseId,
  ) {
    const status = yield* writer.status(databaseId);
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "transaction.read_seq": status.lastSeq,
    });
    return status.lastSeq;
  });

  const getAt = Effect.fn("DbClient.getAt")(function* (
    databaseId: DatabaseId,
    key: Uint8Array,
    readSeq: SeqNum,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "transaction.read_seq": readSeq,
    });
    const probe = yield* writer.probe(databaseId, key);
    if (probe.flushedSeq > readSeq) return yield* tooOld(databaseId, readSeq);
    const view: ReadView = {
      minLastL0Seq: probe.flushedSeq,
      snapshotSeq: Option.some(readSeq),
      memtable: probe.versions.filter((row) => row.seq <= readSeq),
      manifestId: Option.none(),
      maxLastL0Seq: Option.some(readSeq),
    };
    return yield* reader
      .getRow(databaseId, key, view)
      .pipe(Effect.catchTags(expiredIsTooOld(databaseId, readSeq)));
  });

  const scanAt = Effect.fn("DbClient.scanAt")(function* (
    databaseId: DatabaseId,
    range: KeyRange,
    readSeq: SeqNum,
    limit: number,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "transaction.read_seq": readSeq,
    });
    const rows: Array<Row> = [];
    let cursor = Option.none<MemTableCursor>();
    for (;;) {
      const page = yield* writer.scan(databaseId, {
        range,
        snapshotSeq: Option.some(readSeq),
        cursor,
        limit: MEMTABLE_PAGE_ROWS,
      });
      if (page.flushedSeq > readSeq) return yield* tooOld(databaseId, readSeq);
      rows.push(...page.rows);
      if (Option.isNone(page.nextCursor)) break;
      cursor = page.nextCursor;
    }
    const view: ReadView = {
      minLastL0Seq: SeqNum.make(0),
      snapshotSeq: Option.some(readSeq),
      memtable: rows,
      manifestId: Option.none(),
      maxLastL0Seq: Option.some(readSeq),
    };
    return yield* Stream.runCollect(
      reader.scan(databaseId, range, view, { limit: Option.some(limit) }),
    ).pipe(Effect.catchTags(expiredIsTooOld(databaseId, readSeq)));
  });

  const changes = Effect.fn("DbClient.changes")(function* (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    limit: number,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, seq: afterSeq });
    /** Changes after `afterSeq`, at least `count` rows when that many exist, in sequence order. */
    const fetch = (count: number) =>
      Effect.gen(function* () {
        // The writer's unflushed rows (> flushedSeq) and its flushedSeq, read together.
        const unflushed = yield* writer.changes(databaseId, afterSeq, count);
        const flushed =
          afterSeq < unflushed.flushedSeq
            ? yield* reader.changelogRows(databaseId, afterSeq, unflushed.flushedSeq, count)
            : [];
        // The memtable holds exactly the rows above flushedSeq (truncate and flushed_seq commit
        // together), so the two parts neither overlap nor leave a gap.
        return [...flushed, ...unflushed.rows];
      });
    // One row past the limit shows whether the page's last batch continues.
    let fetched = yield* fetch(limit + 1);
    let rows = cutAtSequenceBoundary(fetched, limit);
    const boundary = fetched[limit];
    if (boundary !== undefined && rows.every((row) => row.seq === boundary.seq)) {
      // One batch larger than the page: return the whole batch.
      fetched = yield* fetch(MAX_BATCH_OPERATIONS);
      rows = fetched.filter((row) => row.seq === boundary.seq);
    }
    const last = rows[rows.length - 1];
    yield* Effect.annotateCurrentSpan({ "changes.rows": rows.length });
    return { rows, nextAfterSeq: last === undefined ? afterSeq : last.seq };
  });

  const commitTransaction = (databaseId: DatabaseId, commit: TransactionCommit) =>
    writer.commitTransaction(databaseId, commit);

  const transaction = <A, E, R>(
    databaseId: DatabaseId,
    isolation: TransactionIsolation,
    body: (transaction: DbTransaction) => Effect.Effect<A, E, R>,
  ) =>
    Effect.gen(function* () {
      const readSeq = yield* beginTransaction(databaseId);
      const mergeOperator = Option.flatMap(
        Option.some(yield* reader.currentManifest(databaseId)),
        (current) =>
          Option.map(current.manifest.options.mergeOperator, (name) => MERGE_OPERATORS[name]),
      );
      // Buffered writes in order, so the batch replays them as the body issued them.
      const operations: Array<WriteBatch["operations"][number]> = [];
      const readKeys: Array<Uint8Array> = [];
      const readRanges: Array<KeyRange> = [];
      const buffered = (key: Uint8Array) =>
        operations.filter((operation) => Bytes.Equivalence(operation.key, key));
      /** The committed value at `readSeq` with this transaction's writes to `key` applied. */
      const overlay = (key: Uint8Array, base: Option.Option<Uint8Array>) => {
        let value = base;
        for (const operation of buffered(key)) {
          if (operation._tag === "Put") value = Option.some(operation.value);
          else if (operation._tag === "Delete") value = Option.none();
          else
            value = Option.some(
              Option.match(mergeOperator, {
                onNone: () => operation.operand,
                onSome: (operator) => operator.merge(key, value, [operation.operand]),
              }),
            );
        }
        return value;
      };
      const handle: DbTransaction = {
        databaseId,
        readSeq,
        isolation,
        get: (key) =>
          Effect.gen(function* () {
            readKeys.push(key);
            // A buffered put or delete settles the value (later merges fold onto it): no read.
            const base = buffered(key).some((operation) => operation._tag !== "Merge")
              ? Option.none<Uint8Array>()
              : yield* Effect.map(getAt(databaseId, key, readSeq), (row) =>
                  Option.flatMap(row, (visible) =>
                    visible.value._tag === "Value"
                      ? Option.some(visible.value.bytes)
                      : Option.none(),
                  ),
                );
            return overlay(key, base);
          }),
        scan: (range, limit) =>
          Effect.gen(function* () {
            readRanges.push(range);
            const touched = operations.filter((operation) =>
              keyRangeContains(range, operation.key),
            );
            const committed = yield* scanAt(databaseId, range, readSeq, limit + touched.length);
            const values = new Map<
              string,
              { key: Uint8Array; value: Option.Option<Uint8Array>; row: Option.Option<Row> }
            >();
            const hex = (key: Uint8Array) =>
              Array.from(key, (byte) => byte.toString(16).padStart(2, "0")).join("");
            for (const row of committed) {
              values.set(hex(row.key), {
                key: row.key,
                value: row.value._tag === "Value" ? Option.some(row.value.bytes) : Option.none(),
                row: Option.some(row),
              });
            }
            for (const operation of touched) {
              const current = values.get(hex(operation.key));
              values.set(hex(operation.key), {
                key: operation.key,
                value: overlay(
                  operation.key,
                  current === undefined ? Option.none() : current.value,
                ),
                row: Option.none(),
              });
            }
            const rows: Array<Row> = [];
            for (const entry of values.values()) {
              if (Option.isNone(entry.value)) continue;
              const value = RowValue.Value({ bytes: entry.value.value });
              // A committed row keeps its seq and timestamps; a buffered write has none yet.
              rows.push(
                Option.match(entry.row, {
                  onSome: (row) => ({ ...row, value }),
                  onNone: () => ({
                    key: entry.key,
                    seq: readSeq,
                    value,
                    createTs: Option.none<Timestamp>(),
                    expireTs: Option.none<Timestamp>(),
                  }),
                }),
              );
            }
            return rows.sort((left, right) => Bytes.Order(left.key, right.key)).slice(0, limit);
          }),
        put: (key, value) =>
          Effect.sync(() => {
            operations.push({ _tag: "Put", key, value, ttlMillis: Option.none() });
          }),
        delete: (key) =>
          Effect.sync(() => {
            operations.push({ _tag: "Delete", key });
          }),
        merge: (key, operand) =>
          Effect.sync(() => {
            operations.push({ _tag: "Merge", key, operand, ttlMillis: Option.none() });
          }),
      };
      const value = yield* body(handle);
      if (operations.length === 0) {
        // A read-only transaction commits nothing; under SerializableSnapshot its reads were all
        // taken at one sequence number, which is a serializable point.
        return { value, receipt: Option.none<WriteReceipt>() };
      }
      const receipt = yield* commitTransaction(databaseId, {
        readSeq,
        isolation,
        batch: { operations },
        readKeys,
        readRanges,
      });
      return { value, receipt: Option.some(receipt) };
    }).pipe(Effect.withSpan("DbClient.transaction", { attributes: { "database.id": databaseId } }));

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
    transaction,
    changes,
    beginTransaction,
    getAt,
    scanAt,
    commitTransaction,
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
 * inspect them. `objectStore` replaces the in-memory bucket (a fault-injecting one, for example).
 */
export const dbClientTestLayer = (
  overrides: Partial<LsmSettings> = {},
  objectStore: Layer.Layer<ObjectStore> = objectStoreTestLayer,
) =>
  dbClientLayerWithoutDependencies.pipe(
    Layer.provideMerge(writerClientTestLayer),
    Layer.provideMerge(
      dbReaderLayerWithoutDependencies.pipe(
        Layer.provide(objectReaderFromObjectStoreLayer),
        Layer.provide(manifestCacheLayerWithoutDependencies),
        Layer.provide(blockCacheLayerWithoutDependencies.pipe(Layer.provide(blockSourceTestLayer))),
      ),
    ),
    Layer.provideMerge(
      manifestStoreLayerWithoutDependencies.pipe(
        Layer.provideMerge(manifestReaderLayerWithoutDependencies),
        Layer.provideMerge(objectStore),
      ),
    ),
    Layer.provideMerge(
      Layer.mergeAll(lsmConfigurationTestLayer(overrides), compactionTriggerTestLayer),
    ),
  );
