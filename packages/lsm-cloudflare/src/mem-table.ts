import {
  type CollapsedWrite,
  DatabaseId,
  type KeyRange,
  ManifestId,
  type Row,
  RowValue,
  SeqNum,
  Timestamp,
  WriterEpoch,
} from "@shaledb/lsm-core";
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

/**
 * Most key and payload bytes one page of memtable rows holds (D93): a scan page, a flush page, and
 * a change feed page. A value may be 1 MiB, so a page of 1,000 rows could otherwise be 1 GiB in a
 * 128 MB isolate. At least one row (one batch, for pages cut between sequence numbers) is always
 * returned; a batch holds at most 8 MiB (`MAX_BATCH_BYTES`), so a batch always fits.
 */
export const MAX_MEMTABLE_PAGE_BYTES = 8 * 1024 * 1024;

/** One page of rows, and whether it holds every row its query asked for. */
export interface MemTablePage {
  readonly rows: ReadonlyArray<Row>;
  /** False when the page stopped at its row limit or byte bound: more rows may follow it. */
  readonly complete: boolean;
}

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
  /**
   * Every row above it is still in SQLite (`retainedFromSeq <= flushedSeq`): flushed rows stay until
   * a trim, so a transaction can still check them (D58). Writers from before D58 deleted rows on
   * flush, so for them it equals `flushedSeq`.
   */
  readonly retainedFromSeq: SeqNum;
  /**
   * The manifest whose commit made `flushedSeq` durable (its `lastL0Seq` is `flushedSeq`), or none
   * for writer state from before D58 (a transaction then reads the newest manifest instead).
   */
  readonly flushedManifestId: Option.Option<ManifestId>;
  /** Newest commit time handed out (the monotonic clock). */
  readonly clockTick: Timestamp;
}

/**
 * How long flushed rows stay for transactions to check: rows flushed within `retentionMs` stay,
 * bounded by `maxRetainedBytes`, and the newest flush's rows always stay.
 */
export interface MemTableRetention {
  readonly nowMs: number;
  readonly retentionMs: number;
  readonly maxRetainedBytes: number;
}

/** Size and age of the unflushed rows (above `flushedSeq`), and the size of the retained ones. */
export interface MemTableStats {
  /** Bytes of the unflushed rows, each counted as {@link memTableWriteBytes} counts it. */
  readonly unflushedBytes: number;
  readonly rowCount: number;
  /** Commit time of the oldest unflushed row. */
  readonly oldestCreateTs: Option.Option<Timestamp>;
}

/**
 * The bytes one write adds to the memtable's unflushed size: its key, its payload, and a fixed
 * overhead per row. The same figure `MemTable.stats` sums in SQLite, so a counter kept by adding
 * this for each committed write equals what a scan would report (D74).
 */
export const memTableWriteBytes = (write: CollapsedWrite): number =>
  write.key.length +
  (write.value._tag === "Value"
    ? write.value.bytes.length
    : write.value._tag === "Merge"
      ? write.value.operand.length
      : 0) +
  ROW_OVERHEAD_BYTES;

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
  /**
   * Runs `read` with the current bookkeeping, both in one SQLite transaction, so no flush can
   * truncate rows between reading `flushedSeq` and reading the rows (a `Latest` read relies on
   * every row above `flushedSeq` still being in the memtable).
   */
  readonly consistently: <A>(
    read: (meta: WriterMeta) => Effect.Effect<A, MemTableError>,
  ) => Effect.Effect<A, MemTableError>;
  /** Every version of `key` with `afterSeq < seq <= snapshotSeq`, newest first. */
  readonly versions: (
    key: Uint8Array,
    snapshotSeq: SeqNum,
    afterSeq: SeqNum,
  ) => Effect.Effect<ReadonlyArray<Row>, MemTableError>;
  /**
   * Up to `limit` rows in `range` with `afterSeq < seq <= snapshotSeq`, in key-ascending,
   * sequence-descending order, after `cursor`.
   */
  readonly scan: (
    range: KeyRange,
    snapshotSeq: SeqNum,
    afterSeq: SeqNum,
    cursor: Option.Option<MemTableCursor>,
    limit: number,
  ) => Effect.Effect<MemTablePage, MemTableError>;
  /**
   * Every row with `afterSeq < seq <= cut`, in key-ascending, sequence-descending order, paged from
   * SQLite (a flush streams the unflushed rows: `afterSeq` is `flushedSeq`).
   */
  readonly rowsUpTo: (afterSeq: SeqNum, cut: SeqNum) => Stream.Stream<Row, MemTableError>;
  /**
   * Up to `limit` rows with `afterSeq < seq <= upToSeq`, in sequence order (changelog and CDC),
   * within {@link MAX_MEMTABLE_PAGE_BYTES} and cut only between sequence numbers.
   */
  readonly rowsBySeq: (
    afterSeq: SeqNum,
    upToSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<MemTablePage, MemTableError>;
  /**
   * Records `cut` as `flushedSeq` (it never moves back) and, when given, `manifestId` as the manifest
   * that made it durable, with the flush's time for retention. Deletes nothing: flushed rows stay
   * until {@link IMemTable.trim}. Call only after the manifest commit that made the rows durable.
   */
  readonly markFlushed: (
    cut: SeqNum,
    manifestId: Option.Option<ManifestId>,
    flushedAtMs: number,
  ) => Effect.Effect<void, MemTableError>;
  /**
   * Deletes flushed rows that `retention` no longer keeps and raises `retainedFromSeq` past them,
   * in one SQLite transaction. Never deletes an unflushed row or the newest flush's rows.
   *
   * @returns The new `retainedFromSeq`.
   */
  readonly trim: (retention: MemTableRetention) => Effect.Effect<SeqNum, MemTableError>;
  /**
   * Unflushed bytes, row count, and the oldest unflushed commit time, read by scanning every
   * unflushed row: for a writer's start and the end of a flush, never per request (D74).
   */
  readonly stats: Effect.Effect<MemTableStats, MemTableError>;
  /** Deletes every row and the writer's identity and sequence state (removal of the database). */
  readonly clear: Effect.Effect<void, MemTableError>;
  /**
   * Whether any of `keys`, or any key in `ranges`, has a row with a sequence number above
   * `afterSeq` (a transaction's conflict check), retained or unflushed. Uses the `(key, seq)`
   * primary key. Exact only while `afterSeq >= retainedFromSeq`.
   */
  readonly writtenAfter: (
    keys: ReadonlyArray<Uint8Array>,
    ranges: ReadonlyArray<KeyRange>,
    afterSeq: SeqNum,
  ) => Effect.Effect<boolean, MemTableError>;
}

/** The contextual memtable. */
export class MemTable extends Context.Service<MemTable, IMemTable>()("@shaledb/MemTable") {}

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
  // One row per flush: its cut and when it committed, so a trim knows how old flushed rows are.
  "CREATE TABLE IF NOT EXISTS flush_marks (cut INTEGER PRIMARY KEY, flushed_at INTEGER NOT NULL) WITHOUT ROWID",
];

const ROW_COLUMNS = "key, seq, kind, payload, create_ts, expire_ts";

/**
 * Query shape for key-ordered reads (D61): sequence bounds are written `+seq`, so SQLite cannot pick
 * the `seq` index for them (it would then sort every matching row, payloads included, on every page;
 * a 60 MiB flush exceeded the 30 s CPU limit that way), and a page resumes with `key >= ?` so the
 * primary key `(key, seq DESC)` seeks to it instead of scanning from the first key.
 */
const resumeAfterCursor = "key >= ? AND (key > ? OR seq < ?)";

/**
 * The flush's page queries (internal; exported for the query-plan test): the first page of rows in
 * `(afterSeq, cut]` in key order, and the next page after a cursor.
 */
export const MEMTABLE_FLUSH_PAGE_QUERIES = {
  first: `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE +seq > ? AND +seq <= ? ORDER BY key ASC, seq DESC LIMIT ?`,
  next: `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE ${resumeAfterCursor} AND +seq > ? AND +seq <= ? ORDER BY key ASC, seq DESC LIMIT ?`,
} as const;

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

  /**
   * Runs `query` (a `SELECT ${ROW_COLUMNS} …` whose last binding is its `LIMIT`) for at most
   * `limit` rows and {@link MAX_MEMTABLE_PAGE_BYTES} of keys and payloads (D93). It first reads
   * only each row's sequence number and size, with the same conditions and order, then the rows
   * that fit. With `bySeq` (which reads one batch's rows) it cuts only between sequence numbers,
   * so a page never splits a batch. Run it
   * inside one transaction (or over rows nothing else changes) so both reads see the same rows.
   */
  const selectPage = (
    operation: string,
    query: string,
    bindings: ReadonlyArray<SqlValue>,
    limit: number,
    bySeq: Option.Option<(seq: number) => Effect.Effect<ReadonlyArray<Row>, MemTableError>>,
  ) =>
    Effect.gen(function* () {
      const sizes = yield* exec(
        operation,
        query.replace(`SELECT ${ROW_COLUMNS}`, "SELECT seq, LENGTH(key) + LENGTH(payload) AS size"),
        ...bindings,
        limit,
      );
      const seqAt = (index: number) => sizes[index]?.["seq"];
      let count = 0;
      let bytes = 0;
      for (const entry of sizes) {
        const size = typeof entry["size"] === "number" ? entry["size"] : 0;
        if (count > 0 && bytes + size > MAX_MEMTABLE_PAGE_BYTES) break;
        bytes += size;
        count++;
      }
      // A page that may stop inside a batch (at its byte bound, or at its row limit) backs off to
      // the start of that batch; a page that is all one batch returns the whole batch instead.
      const cutSeq =
        count < sizes.length
          ? seqAt(count)
          : sizes.length === limit && count > 0
            ? seqAt(count - 1)
            : undefined;
      if (Option.isSome(bySeq) && cutSeq !== undefined) {
        let start = count < sizes.length ? count : count - 1;
        while (start > 0 && seqAt(start - 1) === cutSeq) start--;
        if (start > 0) {
          const rows = yield* selectRows(operation, query, ...bindings, start);
          return { rows, complete: false };
        }
        if (typeof cutSeq === "number") {
          return { rows: yield* bySeq.value(cutSeq), complete: false };
        }
      }
      const rows = count === 0 ? [] : yield* selectRows(operation, query, ...bindings, count);
      return { rows, complete: sizes.length < limit && count === sizes.length };
    });

  for (const statement of MIGRATIONS) yield* exec("migrate", statement);

  const setMeta = (operation: string, name: string, value: number | string) =>
    exec(
      operation,
      "INSERT INTO writer_meta (name, value) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
      name,
      value,
    );

  const hasMeta = (name: string) =>
    Effect.map(
      sql.exec("SELECT 1 AS present FROM writer_meta WHERE name = ?", name),
      (rows) => rows.length > 0,
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
      retainedFromSeq: SeqNum.make(
        values.has("retained_from_seq")
          ? Math.min(numeric("retained_from_seq"), numeric("flushed_seq"))
          : numeric("flushed_seq"),
      ),
      flushedManifestId: values.has("flushed_manifest_id")
        ? Option.some(ManifestId.make(numeric("flushed_manifest_id")))
        : Option.none(),
      clockTick: Timestamp.make(numeric("clock_tick")),
    } satisfies WriterMeta;
  });

  const rowBytes = `COALESCE(SUM(LENGTH(key) + LENGTH(payload) + ${ROW_OVERHEAD_BYTES}), 0)`;
  const stats = Effect.gen(function* () {
    const { flushedSeq } = yield* meta;
    const [totals] = yield* exec(
      "stats",
      `SELECT COUNT(*) AS row_count, ${rowBytes} AS bytes FROM memtable_rows WHERE seq > ?`,
      flushedSeq,
    );
    const [oldest] = yield* exec(
      "stats",
      "SELECT create_ts FROM memtable_rows WHERE seq > ? ORDER BY seq LIMIT 1",
      flushedSeq,
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
    consistently: (read) =>
      sql.transaction(Effect.flatMap(meta, read)).pipe(
        Effect.catchTags({
          WriterSqlError: (error) => Effect.fail(failed("consistently")(error)),
        }),
      ),
    versions: (key, snapshotSeq, afterSeq) =>
      selectRows(
        "versions",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE key = ? AND +seq <= ? AND +seq > ? ORDER BY seq DESC`,
        key,
        snapshotSeq,
        afterSeq,
      ),
    scan: (range, snapshotSeq, afterSeq, cursor, limit) => {
      const conditions = ["key >= ?", "+seq <= ?", "+seq > ?"];
      const bindings: Array<SqlValue> = [range.start, snapshotSeq, afterSeq];
      if (Option.isSome(range.end)) {
        conditions.push("key < ?");
        bindings.push(range.end.value);
      }
      if (Option.isSome(cursor)) {
        conditions.push(resumeAfterCursor);
        bindings.push(cursor.value.key, cursor.value.key, cursor.value.seq);
      }
      return selectPage(
        "scan",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE ${conditions.join(" AND ")} ORDER BY key ASC, seq DESC LIMIT ?`,
        bindings,
        limit,
        Option.none(),
      );
    },
    rowsUpTo: (afterSeq, cut) =>
      Stream.paginate(Option.none<MemTableCursor>(), (cursor) =>
        Effect.gen(function* () {
          const { rows: page, complete } = Option.isNone(cursor)
            ? yield* selectPage(
                "rowsUpTo",
                MEMTABLE_FLUSH_PAGE_QUERIES.first,
                [afterSeq, cut],
                FLUSH_PAGE_ROWS,
                Option.none(),
              )
            : yield* selectPage(
                "rowsUpTo",
                MEMTABLE_FLUSH_PAGE_QUERIES.next,
                [cursor.value.key, cursor.value.key, cursor.value.seq, afterSeq, cut],
                FLUSH_PAGE_ROWS,
                Option.none(),
              );
          const last = page[page.length - 1];
          return [
            page,
            !complete && last !== undefined
              ? Option.some(Option.some({ key: last.key, seq: last.seq }))
              : Option.none(),
          ] as const;
        }),
      ),
    rowsBySeq: (afterSeq, upToSeq, limit) =>
      selectPage(
        "rowsBySeq",
        `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE seq > ? AND seq <= ? ORDER BY seq ASC, key ASC LIMIT ?`,
        [afterSeq, upToSeq],
        limit,
        Option.some((seq: number) =>
          selectRows(
            "rowsBySeq",
            `SELECT ${ROW_COLUMNS} FROM memtable_rows WHERE seq = ? ORDER BY key ASC`,
            seq,
          ),
        ),
      ),
    markFlushed: Effect.fn("MemTable.markFlushed")(function* (
      cut: SeqNum,
      manifestId: Option.Option<ManifestId>,
      flushedAtMs: number,
    ) {
      yield* Effect.annotateCurrentSpan({ seq: cut });
      yield* sql
        .transaction(
          Effect.gen(function* () {
            const current = yield* meta.pipe(Effect.orDie);
            if (cut < current.flushedSeq) return;
            yield* sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('flushed_seq', ?) ON CONFLICT (name) DO UPDATE SET value = MAX(value, excluded.value)",
              cut,
            );
            // Writer state from before D58 deleted every flushed row: its horizon is the old cut.
            if (!(yield* hasMeta("retained_from_seq"))) {
              yield* sql.exec(
                "INSERT INTO writer_meta (name, value) VALUES ('retained_from_seq', ?)",
                current.flushedSeq,
              );
            }
            if (Option.isSome(manifestId)) {
              yield* sql.exec(
                "INSERT INTO writer_meta (name, value) VALUES ('flushed_manifest_id', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
                manifestId.value,
              );
            }
            if (cut > current.flushedSeq) {
              yield* sql.exec(
                "INSERT INTO flush_marks (cut, flushed_at) VALUES (?, ?) ON CONFLICT (cut) DO NOTHING",
                cut,
                flushedAtMs,
              );
            }
          }),
        )
        .pipe(Effect.asVoid, Effect.mapError(failed("markFlushed")));
    }),
    trim: Effect.fn("MemTable.trim")(function* (retention: MemTableRetention) {
      const horizon = yield* sql
        .transaction(
          Effect.gen(function* () {
            const current = yield* meta.pipe(Effect.orDie);
            const marks = yield* sql.exec(
              "SELECT cut, flushed_at FROM flush_marks WHERE cut > ? AND cut <= ? ORDER BY cut ASC",
              current.retainedFromSeq,
              current.flushedSeq,
            );
            const flushes = marks.flatMap((mark) => {
              const cut = mark["cut"];
              const at = mark["flushed_at"];
              return typeof cut === "number" && typeof at === "number" ? [{ cut, at }] : [];
            });
            // The newest flush always stays; older ones go once old enough or over the byte bound.
            let horizon: number = current.retainedFromSeq;
            const older = flushes.slice(0, -1);
            const [kept] = yield* sql.exec(
              `SELECT ${rowBytes} AS bytes FROM memtable_rows WHERE seq > ? AND seq <= ?`,
              current.retainedFromSeq,
              current.flushedSeq,
            );
            let retainedBytes = typeof kept?.["bytes"] === "number" ? kept["bytes"] : 0;
            for (const flush of older) {
              const expired = flush.at <= retention.nowMs - retention.retentionMs;
              if (!expired && retainedBytes <= retention.maxRetainedBytes) break;
              const [segment] = yield* sql.exec(
                `SELECT ${rowBytes} AS bytes FROM memtable_rows WHERE seq > ? AND seq <= ?`,
                horizon,
                flush.cut,
              );
              retainedBytes -= typeof segment?.["bytes"] === "number" ? segment["bytes"] : 0;
              horizon = flush.cut;
            }
            if (horizon > current.retainedFromSeq) {
              yield* sql.exec("DELETE FROM memtable_rows WHERE seq <= ?", horizon);
              yield* sql.exec("DELETE FROM flush_marks WHERE cut <= ?", horizon);
            }
            yield* sql.exec(
              "INSERT INTO writer_meta (name, value) VALUES ('retained_from_seq', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value",
              horizon,
            );
            return SeqNum.make(horizon);
          }),
        )
        .pipe(Effect.mapError(failed("trim")));
      yield* Effect.annotateCurrentSpan({ "memtable.retained_from_seq": horizon });
      return horizon;
    }),
    stats,
    writtenAfter: Effect.fn("MemTable.writtenAfter")(function* (
      keys: ReadonlyArray<Uint8Array>,
      ranges: ReadonlyArray<KeyRange>,
      afterSeq: SeqNum,
    ) {
      for (const key of keys) {
        const hits = yield* exec(
          "writtenAfter",
          "SELECT 1 AS hit FROM memtable_rows WHERE key = ? AND seq > ? LIMIT 1",
          key,
          afterSeq,
        );
        if (hits.length > 0) return true;
      }
      for (const range of ranges) {
        const hits = Option.isSome(range.end)
          ? yield* exec(
              "writtenAfter",
              "SELECT 1 AS hit FROM memtable_rows WHERE key >= ? AND key < ? AND seq > ? LIMIT 1",
              range.start,
              range.end.value,
              afterSeq,
            )
          : yield* exec(
              "writtenAfter",
              "SELECT 1 AS hit FROM memtable_rows WHERE key >= ? AND seq > ? LIMIT 1",
              range.start,
              afterSeq,
            );
        if (hits.length > 0) return true;
      }
      return false;
    }),
    clear: sql
      .transaction(
        Effect.all([
          sql.exec("DELETE FROM memtable_rows"),
          sql.exec("DELETE FROM writer_meta"),
          sql.exec("DELETE FROM flush_marks"),
        ]),
      )
      .pipe(Effect.asVoid, Effect.mapError(failed("clear")), Effect.withSpan("MemTable.clear")),
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
