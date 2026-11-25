import { Arbitrary, Effect, Option, Schema } from "effect";

import { SeqNum, Timestamp } from "./ids.ts";
import { type Row, RowValue, rowOrder } from "./row.ts";

/**
 * Generators and the `Map` oracle the property tests share. Imported only by `*.test.ts` files.
 */

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/**
 * Keys from a four-symbol byte alphabet (0, 1, 2, 255) of length 0–5, so keys collide, share
 * prefixes, and include the empty key and `0xff` bytes.
 */
export const keyArbitrary: Arbitrary.Arbitrary<Uint8Array> = Arbitrary.array(smallInt(0, 3), {
  maxLength: 5,
}).pipe(
  Arbitrary.map((symbols) => Uint8Array.from(symbols.map((symbol) => [0, 1, 2, 255][symbol] ?? 0))),
);

/** Keys from only three values (empty, `0x00`, `0x01`), so each key has many versions. */
export const narrowKeyArbitrary: Arbitrary.Arbitrary<Uint8Array> = smallInt(0, 2).pipe(
  Arbitrary.map((symbol) => (symbol === 0 ? new Uint8Array(0) : Uint8Array.of(symbol - 1))),
);

/** Values of 0–24 arbitrary bytes. */
export const valueArbitrary: Arbitrary.Arbitrary<Uint8Array> = Arbitrary.array(smallInt(0, 255), {
  maxLength: 24,
}).pipe(Arbitrary.map((bytes) => Uint8Array.from(bytes)));

/** One generated write: a put, a delete, or a merge operand, with an optional expiry offset. */
export interface GeneratedWrite {
  readonly key: Uint8Array;
  readonly kind: number;
  readonly bytes: Uint8Array;
  /** 0 means no TTL; otherwise the row expires at `createTs + ttl`. */
  readonly ttl: number;
}

/** A write: 50% put, 25% delete, 25% merge; one in four puts or merges carries a TTL. */
export const writeArbitrary: Arbitrary.Arbitrary<GeneratedWrite> = Arbitrary.all({
  key: keyArbitrary,
  kind: smallInt(0, 3),
  bytes: valueArbitrary,
  ttl: smallInt(0, 12),
}).pipe(Arbitrary.map((write) => ({ ...write, ttl: write.ttl > 3 ? 0 : write.ttl * 10 })));

/** A history of up to `maxLength` writes. */
export const historyArbitrary = (maxLength: number) =>
  Arbitrary.array(writeArbitrary, { maxLength });

/** A history of up to `maxLength` writes to three keys, so versions of a key span many blocks. */
export const narrowHistoryArbitrary = (maxLength: number) =>
  Arbitrary.array(
    Arbitrary.all({ write: writeArbitrary, key: narrowKeyArbitrary }).pipe(
      Arbitrary.map(({ write, key }) => ({ ...write, key })),
    ),
    { minLength: 20, maxLength },
  );

/**
 * Turns generated writes into rows: write `i` gets sequence number `i + 1` and `createTs = 100 + i`.
 */
export const rowsFromWrites = (writes: ReadonlyArray<GeneratedWrite>): ReadonlyArray<Row> =>
  writes.map((write, index) => {
    const createTs = Timestamp.make(100 + index);
    const value =
      write.kind === 0 || write.kind === 1
        ? RowValue.Value({ bytes: write.bytes })
        : write.kind === 2
          ? RowValue.Tombstone()
          : RowValue.Merge({ operand: write.bytes });
    return {
      key: write.key,
      seq: SeqNum.make(index + 1),
      value,
      createTs: Option.some(createTs),
      expireTs:
        write.ttl > 0 && value._tag !== "Tombstone"
          ? Option.some(Timestamp.make(createTs + write.ttl))
          : Option.none(),
    };
  });

/** Rows sorted in LSM order (key ascending, sequence descending). */
export const sortRows = (rows: ReadonlyArray<Row>): Array<Row> => [...rows].sort(rowOrder);

const hexKey = (key: Uint8Array): string =>
  Array.from(key, (byte) => byte.toString(16).padStart(2, "0")).join("");

/**
 * The oracle: replays rows in sequence order up to `snapshotSeq` into a plain `Map`, with the
 * `Concat` merge operator and TTL evaluated at `now`.
 *
 * @returns Each live key (hex) and its value bytes.
 */
export const oracleState = (
  rows: ReadonlyArray<Row>,
  snapshotSeq: number,
  now: number,
): ReadonlyMap<string, Uint8Array> => {
  // Per key, the visible history since the latest put or delete: a base and then operands.
  const ordered = [...rows]
    .sort((left, right) => left.seq - right.seq)
    .filter((row) => row.seq <= snapshotSeq);
  const histories = new Map<string, Array<Row>>();
  for (const row of ordered) {
    const key = hexKey(row.key);
    if (row.value._tag === "Merge") histories.set(key, [...(histories.get(key) ?? []), row]);
    else histories.set(key, [row]);
  }
  const result = new Map<string, Uint8Array>();
  for (const [key, history] of histories) {
    const live = history.filter((row) => Option.isNone(row.expireTs) || row.expireTs.value > now);
    const [first] = history;
    if (first === undefined) continue;
    const baseRow = first.value._tag === "Merge" ? undefined : first;
    const baseLive =
      baseRow !== undefined && live.includes(baseRow) && baseRow.value._tag === "Value";
    const operands = live.filter((row) => row.value._tag === "Merge");
    if (!baseLive && operands.length === 0) continue;
    const parts: Array<Uint8Array> = [];
    if (baseLive && baseRow.value._tag === "Value") parts.push(baseRow.value.bytes);
    for (const operand of operands)
      if (operand.value._tag === "Merge") parts.push(operand.value.operand);
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
      bytes.set(part, offset);
      offset += part.length;
    }
    result.set(key, bytes);
  }
  return result;
};

/** Hex form of a key, for comparing against {@link oracleState}. */
export const keyToHex = hexKey;

/**
 * Runs a property check and fails the test with Effect's formatted counterexample when it does not
 * pass.
 */
export const assertProperty = <A>(
  arbitrary: Arbitrary.Arbitrary<A>,
  property: (value: A) => boolean,
  options: Arbitrary.CheckOptions = {},
): void => {
  const result = Effect.runSync(
    Arbitrary.checkEffect(arbitrary, property, { runs: 200, ...options }),
  );
  const failure = Arbitrary.formatCheckFailure(result);
  if (failure !== undefined) throw new Error(failure);
};

const rowPayload = (row: Row): Uint8Array =>
  row.value._tag === "Value"
    ? row.value.bytes
    : row.value._tag === "Merge"
      ? row.value.operand
      : new Uint8Array(0);

/** Whether two rows are the same version: key, sequence, value, and timestamps. */
export const rowsEqual = (left: Row, right: Row): boolean =>
  left.seq === right.seq &&
  left.value._tag === right.value._tag &&
  keyToHex(left.key) === keyToHex(right.key) &&
  keyToHex(rowPayload(left)) === keyToHex(rowPayload(right)) &&
  Option.getOrUndefined(left.createTs) === Option.getOrUndefined(right.createTs) &&
  Option.getOrUndefined(left.expireTs) === Option.getOrUndefined(right.expireTs);

/** Whether two row lists hold the same versions in the same order. */
export const rowListsEqual = (left: ReadonlyArray<Row>, right: ReadonlyArray<Row>): boolean =>
  left.length === right.length &&
  left.every((row, index) => {
    const other = right[index];
    return other !== undefined && rowsEqual(row, other);
  });
