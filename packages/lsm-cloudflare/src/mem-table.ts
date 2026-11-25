import {
  type CollapsedWrite,
  DatabaseId,
  type KeyRange,
  type Row,
  RowValue,
  SeqNum,
  Timestamp,
  WriterEpoch,
} from "@cf-slatedb/lsm-core";
import { Context, Data, Effect, Layer, Option, Schema, Stream } from "effect";

import {
  type SqlValue,
  WriterSqlStorage,
  writerSqlStorageTestLayer,
  type WriterSqlError,
} from "./writer-sql-storage.ts";

/** Bytes a memtable row counts toward `unflushedBytes`: key, payload, and fixed overhead. */
const ROW_OVERHEAD_BYTES = 32;

/** Rows per page when streaming the memtable out for a flush. */
const FLUSH_PAGE_ROWS = 1024;

const ROW_KIND = { Value: 0, Merge: 1, Tombstone: 2 } as const;

/** The writer's durable bookkeeping, stored in the same SQLite database as the rows. */
export interface WriterMeta {
  /** Database this writer serves; set by the first request. */
  readonly databaseId: Option.Option<DatabaseId>;
  /** Epoch this writer fenced in with; none before the first fence. */
  readonly writerEpoch: Option.Option<WriterEpoch>;
  /** Newest assigned sequence number (the sequence oracle). */
  readonly lastSeq: SeqNum;
  /** Newest sequence number committed to the manifest by a flush. */
  readonly flushedSeq: SeqNum;
  /** Newest commit time handed out (the monotonic clock). */
  readonly clockTick: Timestamp;
}

/** Size and age of the unflushed rows. */
export interface MemTableStats {
  readonly unflushedBytes: number;
  readonly rowCount: number;
  /** Commit time of the oldest unflushed row. */
  readonly oldestCreateTs: Option.Option<Timestamp>;
}

/** A scan position: the last returned row's key and sequence number. */
export interface MemTableCursor {
  readonly key: Uint8Array;
  readonly seq: SeqNum;
}

/**
 * The writer's stored data was invalid or its storage failed.
 *
 * **Details**
 *
 * `StorageUnavailable` is retryable (the transaction rolled back). `StoredDataInvalid` means a row
 * did not decode; it needs an operator.
 */
export class MemTableError extends Data.TaggedError("MemTableError")<{
  readonly operation: string;
  readonly reason: "StorageUnavailable" | "StoredDataInvalid";
  readonly message: string;
}> {}

/**
 * The memtable, which is also the write-ahead log: one SQLite table in the writer Durable Object,
 * indexed on `(key, seq DESC)`, made durable before a write is acknowledged.
 */
export interface IMemTable {
  /** Reads the writer's bookkeeping. */
  readonly meta: Effect.Effect<WriterMeta, MemTableError>;
  /** Records the database this writer serves and the epoch it fenced in with. */
  readonly setIdentity: (
    databaseId: DatabaseId,
    writerEpoch: WriterEpoch,
  ) => Effect.Effect<void, MemTableError>;
  /** Raises `lastSeq`, `flushedSeq`, and `clockTick` to at least the given values (writer start). */
  readonly seed: (floor: {
    readonly seq: SeqNum;
    readonly clockTick: Timestamp;
  }) => Effect.Effect<void, MemTableError>;
  /**
   * Commits one batch atomically: assigns the next sequence number (the sequence oracle), inserts
   * every row with it, and records `createTs` as the clock tick, in one SQLite transaction.
   *
   * @returns The batch's sequence number.
   */
  readonly commit: (
    writes: ReadonlyArray<CollapsedWrite>,
    createTs: Timestamp,
  ) => Effect.Effect<SeqNum, MemTableError>;
  /** Every version of `key` at or below `snapshotSeq`, newest first. */
  readonly versions: (
    key: Uint8Array,
    snapshotSeq: SeqNum,
  ) => Effect.Effect<ReadonlyArray<Row>, MemTableError>;
  /**
   * Up to `limit` rows in `range` at or below `snapshotSeq`, in key-ascending, sequence-descending
   * order, after `cursor`.
   */
  readonly scan: (
    range: KeyRange,
    snapshotSeq: SeqNum,
    cursor: Option.Option<MemTableCursor>,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<Row>, MemTableError>;
  /** Every row at or below `cut`, in key-ascending, sequence-descending order, paged from SQLite. */
  readonly rowsUpTo: (cut: SeqNum) => Stream.Stream<Row, MemTableError>;
  /** Up to `limit` rows with `afterSeq < seq <= upToSeq`, in sequence order (changelog and CDC). */
  readonly rowsBySeq: (
    afterSeq: SeqNum,
    upToSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<Row>, MemTableError>;
  /**
   * Deletes every row at or below `cut` and records it as `flushedSeq`. Call only after the
   * manifest commit that made those rows durable in R2.
   */
  readonly truncate: (cut: SeqNum) => Effect.Effect<void, MemTableError>;
  /** Unflushed bytes, row count, and the oldest unflushed commit time. */
  readonly stats: Effect.Effect<MemTableStats, MemTableError>;
}

/** The contextual memtable. */
export class MemTable extends Context.Service<MemTable, IMemTable>()("@cf-slatedb/MemTable") {}

const StoredRow = Schema.Struct({
  key: Schema.Uint8Array,
  seq: SeqNum,
  kind: Schema.Literals([ROW_KIND.Value, ROW_KIND.Merge, ROW_KIND.Tombstone]),
  payload: Schema.Uint8Array,
  create_ts: Schema.NullOr(Timestamp),
  expire_ts: Schema.NullOr(Timestamp),
});
const decodeStoredRows = Schema.decodeUnknownEffect(Schema.Array(StoredRow));

const StoredMeta = Schema.Array(
  Schema.Struct({ name: Schema.String, value: Schema.Union([Schema.Number, Schema.String]) }),
);
const decodeStoredMeta = Schema.decodeUnknownEffect(StoredMeta);
const decodeDatabaseId = Schema.decodeUnknownEffect(DatabaseId);

const MIGRATIONS = [
  `CREATE TABLE IF NOT EXISTS memtable_rows (
     key BLOB NOT NULL,
     seq INTEGER NOT NULL,
     kind INTEGER NOT NULL,
     payload BLOB NOT NULL,
     create_ts INTEGER,
     expire_ts INTEGER,
     PRIMARY KEY (key, seq DESC)
   ) WITHOUT ROWID`,
  "CREATE INDEX IF NOT EXISTS memtable_rows_by_seq ON memtable_rows (seq)",
  "CREATE TABLE IF NOT EXISTS writer_meta (name TEXT PRIMARY KEY, value) WITHOUT ROWID",
];

const ROW_COLUMNS = "key, seq, kind, payload, create_ts, expire_ts";

const toRow = (stored: typeof StoredRow.Type): Row => ({
  key: stored.key,
  seq: stored.seq,
  value:
    stored.kind === ROW_KIND.Value
      ? RowValue.Value({ bytes: stored.payload })
      : stored.kind === ROW_KIND.Merge
        ? RowValue.Merge({ operand: stored.payload })
        : RowValue.Tombstone(),
  createTs: Option.fromNullOr(stored.create_ts),
  expireTs: Option.fromNullOr(stored.expire_ts),
});

/** Constructs the memtable over the contextual SQLite storage, creating its tables when absent. */
export const makeMemTable = Effect.gen(function* () {
  const sql = yield* WriterSqlStorage;

  const failed = (operation: string) => (error: WriterSqlError) =>
    new MemTableError({ operation, reason: "StorageUnavailable", message: error.message });
  const invalid = (operation: string) => () =>
    new MemTableError({
      operation,
      reason: "StoredDataInvalid",
      message: `The writer's stored rows are invalid (${operation}). An operator must inspect the writer's storage.`,
    });

  const exec = (operation: string, query: string, ...bindings: ReadonlyArray<SqlValue>) =>
    sql.exec(query, ...bindings).pipe(Effect.mapError(failed(operation)));
  const selectRows = (operation: string, query: string, ...bindings: ReadonlyArray<SqlValue>) =>
    exec(operation, query, ...bindings).pipe(
      Effect.flatMap((rows) => decodeStoredRows(rows).pipe(Effect.mapError(invalid(operation)))),
      Effect.map((rows) => rows.map(toRow)),
    );

  for (const statement of MIGRATIONS) yield* exec("migrate", statement);

  const setMeta = (operation: string, name: string, value: number | string) =>
    exec(
      operation,
      "INSERT INTO writer_meta (name, value) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
      name,
      value,
    );

  const meta = Effect.gen(function* () {
    const rows = yield* exec("meta", "SELECT name, value FROM writer_meta").pipe(
      Effect.flatMap((raw) => decodeStoredMeta(raw).pipe(Effect.mapError(invalid("meta")))),
    );
    const values = new Map(rows.map((row) => [row.name, row.value]));
    const numeric = (name: string) => {
      const value = values.get(name);
      return typeof value === "number" ? value : 0;
    };
    const databaseIdText = values.get("database_id");
    const databaseId =
      typeof databaseIdText === "string"
        ? Option.some(
            yield* decodeDatabaseId(databaseIdText).pipe(Effect.mapError(invalid("meta"))),
          )
        : Option.none<DatabaseId>();
    return {
      databaseId,
      writerEpoch: values.has("writer_epoch")
        ? Option.some(WriterEpoch.make(numeric("writer_epoch")))
        : Option.none(),
      lastSeq: SeqNum.make(numeric("last_seq")),
      flushedSeq: SeqNum.make(numeric("flushed_seq")),
      clockTick: Timestamp.make(numeric("clock_tick")),
    } satisfies WriterMeta;
  });

  const stats = Effect.gen(function* () {
    const [totals] = yield* exec(
      "stats",
      `SELECT COUNT(*) AS row_count, COALESCE(SUM(LENGTH(key) + LENGTH(payload) + ${ROW_OVERHEAD_BYTES}), 0) AS bytes FROM memtable_rows`,
    );
    const [oldest] = yield* exec(
      "stats",
      "SELECT create_ts FROM memtable_rows ORDER BY seq LIMIT 1",
    );
    const rowCount = totals?.["row_count"];
    const bytes = totals?.["bytes"];
    const createTs = oldest?.["create_ts"];
    return {
      unflushedBytes: typeof bytes === "number" ? bytes : 0,
      rowCount: typeof rowCount === "number" ? rowCount : 0,
      oldestCreateTs:
        typeof createTs === "number" ? Option.some(Timestamp.make(createTs)) : Option.none(),
    };
  });

  return MemTable.of({
    meta,
    setIdentity: (databaseId, writerEpoch) =>
      sql
        .transaction(
          Effect.all([
            sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('database_id', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
              databaseId,
            ),
            sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('writer_epoch', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
              writerEpoch,
            ),
          ]),
        )
        .pipe(Effect.asVoid, Effect.mapError(failed("setIdentity"))),
    seed: (floor) =>
      Effect.gen(function* () {
        const current = yield* meta;
        if (floor.seq > current.lastSeq) yield* setMeta("seed", "last_seq", floor.seq);
        if (floor.seq > current.flushedSeq) yield* setMeta("seed", "flushed_seq", floor.seq);
        if (floor.clockTick > current.clockTick)
          yield* setMeta("seed", "clock_tick", floor.clockTick);
      }),
    commit: Effect.fn("MemTable.commit")(function* (
      writes: ReadonlyArray<CollapsedWrite>,
      createTs: Timestamp,
    ) {
      const committed = yield* sql
        .transaction(
          Effect.gen(function* () {
            const [last] = yield* sql.exec("SELECT value FROM writer_meta WHERE name = 'last_seq'");
            const lastValue = last?.["value"];
            const seq = SeqNum.make((typeof lastValue === "number" ? lastValue : 0) + 1);
            for (const write of writes) {
              const kind = ROW_KIND[write.value._tag];
              const payload =
                write.value._tag === "Value"
                  ? write.value.bytes
                  : write.value._tag === "Merge"
                    ? write.value.operand
                    : new Uint8Array(0);
              const expireTs = Option.match(write.ttlMillis, {
                onNone: () => null,
                onSome: (ttl) => createTs + ttl,
              });
              yield* sql.exec(
                `INSERT INTO memtable_rows (${ROW_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?)`,
                write.key,
                seq,
                kind,
                payload,
                createTs,
                expireTs,
              );
            }
            yield* sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('last_seq', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
              seq,
            );
            yield* sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('clock_tick', ?) ON CONFLICT (name) DO UPDATE SET value = MAX(value, excluded.value)",
              createTs,
            );
            return seq;
          }),
        )
        .pipe(Effect.mapError(failed("commit")));
      yield* Effect.annotateCurrentSpan({ seq: committed, "batch.rows": writes.length });
      return committed;
    }),
    versions: (key, snapshotSeq) =>
      selectRows(
        "versions",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE key = ? AND seq <= ? ORDER BY seq DESC`,
        key,
        snapshotSeq,
      ),
    scan: (range, snapshotSeq, cursor, limit) => {
      const conditions = ["key >= ?", "seq <= ?"];
      const bindings: Array<SqlValue> = [range.start, snapshotSeq];
      if (Option.isSome(range.end)) {
        conditions.push("key < ?");
        bindings.push(range.end.value);
      }
      if (Option.isSome(cursor)) {
        conditions.push("(key > ? OR (key = ? AND seq < ?))");
        bindings.push(cursor.value.key, cursor.value.key, cursor.value.seq);
      }
      return selectRows(
        "scan",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE ${conditions.join(" AND ")} ORDER BY key ASC, seq DESC LIMIT ?`,
        ...bindings,
        limit,
      );
    },
    rowsUpTo: (cut) =>
      Stream.paginate(Option.none<MemTableCursor>(), (cursor) =>
        Effect.gen(function* () {
          const page = Option.isNone(cursor)
            ? yield* selectRows(
                "rowsUpTo",
                `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE seq <= ? ORDER BY key ASC, seq DESC LIMIT ?`,
                cut,
                FLUSH_PAGE_ROWS,
              )
            : yield* selectRows(
                "rowsUpTo",
                `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE seq <= ? AND (key > ? OR (key = ? AND seq < ?)) ORDER BY key ASC, seq DESC LIMIT ?`,
                cut,
                cursor.value.key,
                cursor.value.key,
                cursor.value.seq,
                FLUSH_PAGE_ROWS,
              );
          const last = page[page.length - 1];
          return [
            page,
            page.length === FLUSH_PAGE_ROWS && last !== undefined
              ? Option.some(Option.some({ key: last.key, seq: last.seq }))
              : Option.none(),
          ] as const;
        }),
      ),
    rowsBySeq: (afterSeq, upToSeq, limit) =>
      selectRows(
        "rowsBySeq",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE seq > ? AND seq <= ? ORDER BY seq ASC, key ASC LIMIT ?`,
        afterSeq,
        upToSeq,
        limit,
      ),
    truncate: Effect.fn("MemTable.truncate")(function* (cut: SeqNum) {
      yield* Effect.annotateCurrentSpan({ seq: cut });
      yield* sql
        .transaction(
          Effect.all([
            sql.exec("DELETE FROM memtable_rows WHERE seq <= ?", cut),
            sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('flushed_seq', ?) ON CONFLICT (name) DO UPDATE SET value = MAX(value, excluded.value)",
              cut,
            ),
          ]),
        )
        .pipe(Effect.asVoid, Effect.mapError(failed("truncate")));
    }),
    stats,
  });
});

/** Provides the memtable, leaving the SQLite storage visible. */
export const memTableLayerWithoutDependencies = Layer.effect(MemTable, makeMemTable);

/**
 * Provides the memtable; the writer Durable Object supplies its storage through
 * `writerSqlStorageLayer` at construction.
 */
export const memTableLayer = memTableLayerWithoutDependencies;

/** Provides a memtable on a fresh in-memory `node:sqlite` database. */
export const memTableTestLayer = memTableLayerWithoutDependencies.pipe(
  Layer.provide(writerSqlStorageTestLayer),
);
