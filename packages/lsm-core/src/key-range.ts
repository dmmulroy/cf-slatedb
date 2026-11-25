import { Bytes, Option, Schema } from "effect";

/**
 * Schema for a half-open key range `[start, end)`, encoded as base64 JSON.
 *
 * **Details**
 *
 * `end` absent means unbounded. Keys compare as bytes (memcmp order, {@link Bytes.Order}).
 */
export const KeyRange = Schema.Struct({
  start: Schema.Uint8ArrayFromBase64,
  end: Schema.OptionFromNullOr(Schema.Uint8ArrayFromBase64),
});

/** A half-open key range `[start, end)`; `end` none means unbounded. */
export type KeyRange = typeof KeyRange.Type;

/** The range of every key. */
export const FULL_KEY_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.none() };

/** Whether `key` is inside `range`. */
export const keyRangeContains = (range: KeyRange, key: Uint8Array): boolean =>
  Bytes.Order(key, range.start) >= 0 &&
  (Option.isNone(range.end) || Bytes.Order(key, range.end.value) < 0);

/**
 * Whether `range` and the closed interval `[first, last]` share a key, as for an SST whose first and
 * last keys are known.
 */
export const keyRangeOverlapsClosed = (
  range: KeyRange,
  first: Uint8Array,
  last: Uint8Array,
): boolean =>
  Bytes.Order(last, range.start) >= 0 &&
  (Option.isNone(range.end) || Bytes.Order(first, range.end.value) < 0);

/** Whether two half-open ranges share a key. */
export const keyRangesOverlap = (left: KeyRange, right: KeyRange): boolean => {
  const startsBeforeRightEnds =
    Option.isNone(right.end) || Bytes.Order(left.start, right.end.value) < 0;
  const rightStartsBeforeLeftEnds =
    Option.isNone(left.end) || Bytes.Order(right.start, left.end.value) < 0;
  return startsBeforeRightEnds && rightStartsBeforeLeftEnds;
};

/**
 * Intersects two half-open ranges.
 *
 * @returns The shared range, or `Option.none()` when they share no key.
 */
export const intersectKeyRanges = (left: KeyRange, right: KeyRange): Option.Option<KeyRange> => {
  const start = Bytes.Order(left.start, right.start) >= 0 ? left.start : right.start;
  const end = Option.isNone(left.end)
    ? right.end
    : Option.isNone(right.end)
      ? left.end
      : Option.some(
          Bytes.Order(left.end.value, right.end.value) <= 0 ? left.end.value : right.end.value,
        );
  if (Option.isSome(end) && Bytes.Order(start, end.value) >= 0) return Option.none();
  return Option.some({ start, end });
};

/**
 * The smallest key greater than every key that starts with `prefix`, used as the exclusive end of a
 * prefix scan.
 *
 * @returns The successor, or `Option.none()` when `prefix` is empty or all `0xff` bytes (no finite
 *   bound exists).
 */
export const prefixSuccessor = (prefix: Uint8Array): Option.Option<Uint8Array> => {
  for (let index = prefix.length - 1; index >= 0; index--) {
    const byte = prefix[index] ?? 0xff;
    if (byte !== 0xff) {
      const successor = prefix.slice(0, index + 1);
      successor[index] = byte + 1;
      return Option.some(successor);
    }
  }
  return Option.none();
};

/** The range of every key that starts with `prefix`. */
export const keyRangeOfPrefix = (prefix: Uint8Array): KeyRange => ({
  start: prefix,
  end: prefixSuccessor(prefix),
});

/** Whether `inner` lies entirely inside `outer`. */
export const keyRangeCovers = (outer: KeyRange, inner: KeyRange): boolean =>
  Bytes.Order(inner.start, outer.start) >= 0 &&
  (Option.isNone(outer.end) ||
    (Option.isSome(inner.end) && Bytes.Order(inner.end.value, outer.end.value) <= 0));
