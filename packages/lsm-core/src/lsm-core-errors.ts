import { Data } from "effect";

/**
 * An SST, block, or manifest failed validation: a checksum mismatch, a truncated section, or an
 * unknown format version.
 *
 * **Details**
 *
 * Not retryable: the stored bytes are wrong, and reading them again returns the same bytes. Carries
 * the SST and section so an operator can find the object. `offset` is the byte offset of the section
 * within the SST, when known.
 */
export class SstCorruptedError extends Data.TaggedError("SstCorruptedError")<{
  readonly sstId: string;
  readonly section: "DataBlock" | "Filter" | "Index" | "Footer" | "Trailer";
  readonly reason: "ChecksumMismatch" | "Truncated" | "UnsupportedVersion" | "Malformed";
  readonly offset: number;
  readonly message: string;
}> {}

/**
 * A merge row was read, but the database has no merge operator configured, so its value cannot be
 * resolved.
 *
 * **Details**
 *
 * Not retryable. Configure a merge operator for the database (`DatabaseOptions.mergeOperator`), or
 * stop writing merge operands to it.
 */
export class MergeOperatorMissingError extends Data.TaggedError("MergeOperatorMissingError")<{
  readonly message: string;
}> {}
