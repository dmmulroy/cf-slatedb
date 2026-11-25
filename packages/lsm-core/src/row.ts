import { Bytes, Data, Option, Order, Schema } from "effect";

import { SeqNum, Timestamp } from "./ids.ts";

/**
 * What one version of a key holds.
 *
 * - `Value`: the key's value as of this version.
 * - `Merge`: a merge operand, combined with older versions by the database's merge operator.
 * - `Tombstone`: the key was deleted as of this version.
 */
export type RowValue = Data.TaggedEnum<{
  Value: { readonly bytes: Uint8Array };
  Merge: { readonly operand: Uint8Array };
  Tombstone: {};
}>;

/** Constructors and matchers for {@link RowValue}. */
export const RowValue = Data.taggedEnum<RowValue>();

/**
 * One version of one key.
 *
 * **Details**
 *
 * `seq` orders versions and decides visibility. `createTs` is the commit time of the write batch
 * that produced it (metadata only). `expireTs` is the absolute time after which the version is
 * treated as deleted (TTL); deletes never carry one.
 */
export interface Row {
  readonly key: Uint8Array;
  readonly seq: SeqNum;
  readonly value: RowValue;
  readonly createTs: Option.Option<Timestamp>;
  readonly expireTs: Option.Option<Timestamp>;
}

/** Key order: memcmp over the key bytes. */
export const keyOrder: Order.Order<Uint8Array> = Bytes.Order;

/** Row order inside the LSM: key ascending, then sequence number descending (newest first). */
export const rowOrder: Order.Order<Row> = Order.make((left, right) => {
  const byKey = Bytes.Order(left.key, right.key);
  if (byKey !== 0) return byKey;
  return left.seq > right.seq ? -1 : left.seq < right.seq ? 1 : 0;
});

/** Changelog order: sequence number ascending, then key ascending. */
export const changelogRowOrder: Order.Order<Row> = Order.make((left, right) => {
  if (left.seq !== right.seq) return left.seq < right.seq ? -1 : 1;
  return Bytes.Order(left.key, right.key);
});

/** Bytes a row adds to the memtable or an SST before compression: key, payload, and metadata. */
export const rowEncodedSizeEstimate = (row: Row): number => {
  const payload =
    row.value._tag === "Value"
      ? row.value.bytes.length
      : row.value._tag === "Merge"
        ? row.value.operand.length
        : 0;
  return (
    row.key.length +
    payload +
    8 +
    1 +
    (Option.isSome(row.createTs) ? 8 : 0) +
    (Option.isSome(row.expireTs) ? 8 : 0)
  );
};

/** Schema for {@link RowValue} at JSON boundaries, with bytes as base64. */
export const RowValueSchema = Schema.Union([
  Schema.TaggedStruct("Value", { bytes: Schema.Uint8ArrayFromBase64 }),
  Schema.TaggedStruct("Merge", { operand: Schema.Uint8ArrayFromBase64 }),
  Schema.TaggedStruct("Tombstone", {}),
]);

/** Schema for a {@link Row} at JSON boundaries, with bytes as base64 and absent times as `null`. */
export const RowSchema = Schema.Struct({
  key: Schema.Uint8ArrayFromBase64,
  seq: SeqNum,
  value: RowValueSchema,
  createTs: Schema.OptionFromNullOr(Timestamp),
  expireTs: Schema.OptionFromNullOr(Timestamp),
});
