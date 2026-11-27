import { Schema } from "effect";

import { DatabaseId, SeqNum } from "./ids.ts";
import { KeyRange } from "./key-range.ts";
import { RequestKeyBytes, RequestKeyRange, RequestWriteBatch, WriteBatch } from "./write-batch.ts";

/**
 * Schema for a transaction's isolation (SlateDB RFC 0011).
 *
 * **Details**
 *
 * `Snapshot`: reads see the database at the transaction's read sequence number, and the commit
 * fails if another commit wrote one of its keys since (write skew is possible).
 * `SerializableSnapshot`: additionally, the commit fails if another commit wrote a key it read or
 * a key in a range it scanned since, so committed transactions are serializable in commit order.
 */
export const TransactionIsolation = Schema.Literals(["Snapshot", "SerializableSnapshot"]);

/** A transaction's isolation. */
export type TransactionIsolation = typeof TransactionIsolation.Type;

/**
 * Schema for a transaction's commit: what it read (at `readSeq`) and what it writes.
 *
 * **Details**
 *
 * `readKeys` and `readRanges` are checked only for `SerializableSnapshot`. The writes commit
 * atomically with one sequence number, as a write batch.
 */
export const TransactionCommit = Schema.Struct({
  readSeq: SeqNum,
  isolation: TransactionIsolation,
  batch: WriteBatch,
  readKeys: Schema.Array(Schema.Uint8ArrayFromBase64),
  readRanges: Schema.Array(KeyRange),
});

/** A transaction's commit. */
export type TransactionCommit = typeof TransactionCommit.Type;

/** Most keys a transaction's commit may name as read (D100): as many as a batch's operations. */
export const MAX_TRANSACTION_READ_KEYS = 10_000;

/** Most ranges a transaction's commit may name as scanned (D100): each costs the writer a query. */
export const MAX_TRANSACTION_READ_RANGES = 1_000;

/** Schema for a transaction's commit in a request, every field bounded (D100). */
export const RequestTransactionCommit = Schema.Struct({
  readSeq: SeqNum,
  isolation: TransactionIsolation,
  batch: RequestWriteBatch,
  readKeys: Schema.Array(RequestKeyBytes).check(Schema.isMaxLength(MAX_TRANSACTION_READ_KEYS)),
  readRanges: Schema.Array(RequestKeyRange).check(Schema.isMaxLength(MAX_TRANSACTION_READ_RANGES)),
});

/**
 * The transaction cannot commit: another commit wrote a key it writes (`WriteConflict`) or read
 * (`ReadConflict`) after its read sequence number, or data newer than its reads has been flushed
 * (`SnapshotTooOld`), so its snapshot can no longer be checked.
 *
 * **Details**
 *
 * Retryable: nothing was written. Begin a new transaction and run it again.
 */
export class TransactionConflictError extends Schema.TaggedError<TransactionConflictError>()(
  "TransactionConflictError",
  {
    databaseId: DatabaseId,
    reason: Schema.Literals(["WriteConflict", "ReadConflict", "SnapshotTooOld"]),
    message: Schema.String,
  },
) {}
