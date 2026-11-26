import type * as cf from "@cloudflare/workers-types";
import { Context, Data, Effect, Exit, Layer } from "effect";

/** Thrown inside `transactionSync` to roll back a failed transaction body. */
const ROLLBACK = Symbol("rollback");

/** A value bound to or read from SQLite: integer, text, blob, or null. */
export type SqlValue = number | string | Uint8Array | null;

/**
 * A SQLite statement failed (disk full, constraint, or a platform fault).
 *
 * **Details**
 *
 * Writes inside a failed transaction are rolled back, so the caller can retry the same logical
 * operation.
 */
export class WriterSqlError extends Data.TaggedError("WriterSqlError")<{
  readonly message: string;
}> {}

/**
 * The writer's SQLite database: the Durable Object's storage in production, `node:sqlite` in tests.
 *
 * **Details**
 *
 * Statements run synchronously, as Durable Object SQLite does. Rows come back as raw records
 * (`unknown` field values) for the owning module to decode.
 */
export interface IWriterSqlStorage {
  /** Runs one statement and returns every result row. */
  readonly exec: (
    query: string,
    ...bindings: ReadonlyArray<SqlValue>
  ) => Effect.Effect<ReadonlyArray<Readonly<Record<string, unknown>>>, WriterSqlError>;
  /** Runs `body` in one transaction: every statement commits together, or none do. */
  readonly transaction: <A, E>(body: Effect.Effect<A, E>) => Effect.Effect<A, E | WriterSqlError>;
  /** Size of the database file in bytes. */
  readonly databaseSize: Effect.Effect<number>;
}

/** The contextual writer SQLite storage. */
export class WriterSqlStorage extends Context.Service<WriterSqlStorage, IWriterSqlStorage>()(
  "@shaledb/WriterSqlStorage",
) {}

const sqlError = (cause: unknown) =>
  new WriterSqlError({
    message: `Writer storage failed: ${cause instanceof Error ? cause.message : "unknown SQLite error"}. Retry the operation; failed statements were rolled back.`,
  });

/**
 * Adapts a Durable Object's native storage: blobs are bound as `ArrayBuffer` and read back as
 * `Uint8Array`, and transactions use `transactionSync`.
 */
export const writerSqlStorageFromDurableObject = (
  storage: cf.DurableObjectStorage,
): IWriterSqlStorage => {
  const toBinding = (value: SqlValue): cf.SqlStorageValue =>
    value instanceof Uint8Array ? value.slice().buffer : value;
  const fromColumn = (value: unknown): unknown =>
    value instanceof ArrayBuffer ? new Uint8Array(value) : value;
  const exec = (query: string, ...bindings: ReadonlyArray<SqlValue>) =>
    Effect.try({
      try: () =>
        storage.sql
          .exec(query, ...bindings.map(toBinding))
          .toArray()
          .map((row) =>
            Object.fromEntries(Object.entries(row).map(([key, value]) => [key, fromColumn(value)])),
          ),
      catch: sqlError,
    });
  const transaction = <A, E>(body: Effect.Effect<A, E>): Effect.Effect<A, E | WriterSqlError> =>
    Effect.suspend((): Effect.Effect<A, E | WriterSqlError> => {
      // Durable Object transactions are synchronous, and the body is synchronous SQL: run it to an
      // `Exit` inside `transactionSync`; throwing the sentinel rolls a failed body back.
      let exit: Exit.Exit<A, E> | undefined;
      try {
        storage.transactionSync(() => {
          exit = Effect.runSyncExit(body);
          if (Exit.isFailure(exit)) throw ROLLBACK;
        });
      } catch (cause) {
        if (cause !== ROLLBACK) return Effect.fail(sqlError(cause));
      }
      return exit ?? Effect.die(new Error("writer storage transaction produced no result"));
    });
  return { exec, transaction, databaseSize: Effect.sync(() => storage.sql.databaseSize) };
};

/** Provides writer storage from a Durable Object's native storage. */
export const writerSqlStorageLayer = (storage: cf.DurableObjectStorage) =>
  Layer.succeed(WriterSqlStorage, writerSqlStorageFromDurableObject(storage));

interface NodeSqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): {
    all(...bindings: ReadonlyArray<SqlValue>): ReadonlyArray<Record<string, unknown>>;
  };
}

/**
 * Provides writer storage on an in-memory `node:sqlite` database: the same SQL engine, statements,
 * BLOB ordering, and transactions as Durable Object SQLite. `node:sqlite` is loaded dynamically, so
 * Worker bundles never import it.
 */
export const writerSqlStorageTestLayer = Layer.effect(
  WriterSqlStorage,
  Effect.gen(function* () {
    const sqlite = yield* Effect.promise(() => import("node:sqlite"));
    const database: NodeSqliteDatabase = new sqlite.DatabaseSync(":memory:");
    let depth = 0;
    const exec = (query: string, ...bindings: ReadonlyArray<SqlValue>) =>
      Effect.try({ try: () => database.prepare(query).all(...bindings), catch: sqlError });
    const transaction = <A, E>(body: Effect.Effect<A, E>) =>
      Effect.gen(function* () {
        const savepoint = `sp${depth++}`;
        yield* Effect.try({ try: () => database.exec(`SAVEPOINT ${savepoint}`), catch: sqlError });
        return yield* body.pipe(
          Effect.tap(() =>
            Effect.try({ try: () => database.exec(`RELEASE ${savepoint}`), catch: sqlError }),
          ),
          Effect.onError(() =>
            Effect.sync(() => {
              database.exec(`ROLLBACK TO ${savepoint}`);
              database.exec(`RELEASE ${savepoint}`);
            }),
          ),
          Effect.ensuring(Effect.sync(() => depth--)),
        );
      });
    const databaseSize = Effect.sync(() => {
      const [row] = database
        .prepare(
          "SELECT page_count * page_size AS size FROM pragma_page_count(), pragma_page_size()",
        )
        .all();
      return typeof row?.["size"] === "number" ? row["size"] : 0;
    });
    return { exec, transaction, databaseSize };
  }),
);
