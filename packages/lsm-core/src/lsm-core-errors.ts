import { Schema } from "effect";

/**
 * An SST, block, or manifest failed validation: a checksum mismatch, a truncated section, or an
 * unknown format version.
 *
 * **Details**
 *
 * Not retryable: the stored bytes are wrong, and reading them again returns the same bytes. Carries
 * the SST and section so an operator can find the object. `offset` is the byte offset of the section
 * within the SST, when known. A schema-backed class, so it crosses HTTP boundaries unchanged.
 */
export class SstCorruptedError extends Schema.TaggedError<SstCorruptedError>()(
  "SstCorruptedError",
  {
    sstId: Schema.String,
    section: Schema.Literals(["DataBlock", "Filter", "Index", "Footer", "Trailer"]),
    reason: Schema.Literals(["ChecksumMismatch", "Truncated", "UnsupportedVersion", "Malformed"]),
    offset: Schema.Number,
    message: Schema.String,
  },
) {}

/**
 * A merge row was read, but the database has no merge operator configured, so its value cannot be
 * resolved.
 *
 * **Details**
 *
 * Not retryable. Configure a merge operator for the database (`DatabaseOptions.mergeOperator`), or
 * stop writing merge operands to it.
 */
export class MergeOperatorMissingError extends Schema.TaggedError<MergeOperatorMissingError>()(
  "MergeOperatorMissingError",
  { message: Schema.String },
) {}

/**
 * A key's merge operands resolve to a value larger than a read returns (D99): more than
 * `MAX_MERGED_VALUE_BYTES`, the largest value a put may write.
 *
 * **Details**
 *
 * Not retryable: the key stays unreadable until it is replaced. Write the key with a put (or delete
 * it); later merges then apply to that value.
 */
export class MergedValueTooLargeError extends Schema.TaggedError<MergedValueTooLargeError>()(
  "MergedValueTooLargeError",
  { bytes: Schema.Int, limit: Schema.Int, message: Schema.String },
) {}
