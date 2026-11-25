import { DatabaseId } from "@cf-slatedb/lsm-core";
import { Schema } from "effect";

/**
 * No database has this id: it has no manifest.
 *
 * **Details**
 *
 * Not retryable. Create the database first, or check the id.
 */
export class DatabaseNotFoundError extends Schema.TaggedError<DatabaseNotFoundError>()(
  "DatabaseNotFoundError",
  {
    databaseId: DatabaseId,
    message: Schema.String,
  },
) {}

/**
 * The writer refused the write because compaction or flushing is behind (backpressure).
 *
 * **Details**
 *
 * Retryable: nothing was written. Retry the same batch after `retryAfterMs`. `reason` is
 * `UnflushedBytes` when too much data waits to be flushed, and `L0Backlog` when L0 holds too many
 * SSTs for compaction to keep up.
 */
export class WriteStalledError extends Schema.TaggedError<WriteStalledError>()(
  "WriteStalledError",
  {
    databaseId: DatabaseId,
    reason: Schema.Literals(["UnflushedBytes", "L0Backlog"]),
    retryAfterMs: Schema.Int,
    message: Schema.String,
  },
) {}

/**
 * The writer could not complete the operation because storage (its SQLite database, R2, or the
 * manifest) was unavailable or contended.
 *
 * **Details**
 *
 * Retryable. A write that fails with this error was not acknowledged: retry the same batch. A flush
 * or seal is safe to repeat.
 */
export class WriterUnavailableError extends Schema.TaggedError<WriterUnavailableError>()(
  "WriterUnavailableError",
  {
    databaseId: DatabaseId,
    operation: Schema.String,
    reason: Schema.Literals(["StorageUnavailable", "ManifestContention", "StoredDataInvalid"]),
    message: Schema.String,
  },
) {}

/**
 * A writer received a request for a database other than the one it serves. Not retryable: the
 * request was routed to the wrong writer.
 */
export class DatabaseIdMismatchError extends Schema.TaggedError<DatabaseIdMismatchError>()(
  "DatabaseIdMismatchError",
  {
    databaseId: DatabaseId,
    servedDatabaseId: DatabaseId,
    message: Schema.String,
  },
) {}
