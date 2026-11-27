import { Arbitrary, Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  intersectKeyRanges,
  type KeyRange,
  keyRangeContains,
  keyRangeCovers,
  keyRangeOfPrefix,
  keyRangeOverlapsClosed,
  keyRangesOverlap,
} from "./key-range.ts";
import { assertProperty, keyArbitrary } from "./lsm-core-arbitraries.ts";

const rangeArbitrary: Arbitrary.Arbitrary<KeyRange> = Arbitrary.all({
  start: keyArbitrary,
  end: keyArbitrary,
  bounded: Arbitrary.schema(Schema.Boolean),
}).pipe(
  Arbitrary.map(({ start, end, bounded }) => ({
    start,
    end: bounded ? Option.some(end) : Option.none(),
  })),
);

const startsWith = (key: Uint8Array, prefix: Uint8Array) =>
  key.length >= prefix.length && prefix.every((byte, index) => key[index] === byte);

describe("key ranges", () => {
  it("intersect exactly to the keys both ranges contain, and overlap agrees", () => {
    assertProperty(
      Arbitrary.all({ left: rangeArbitrary, right: rangeArbitrary, key: keyArbitrary }),
      ({ left, right, key }) => {
        const both = keyRangeContains(left, key) && keyRangeContains(right, key);
        const intersection = intersectKeyRanges(left, right);
        const inIntersection = Option.match(intersection, {
          onNone: () => false,
          onSome: (range) => keyRangeContains(range, key),
        });
        return both === inIntersection && (!both || keyRangesOverlap(left, right));
      },
    );
  });

  it("make a prefix range that holds exactly the keys with that prefix", () => {
    assertProperty(
      Arbitrary.all({ prefix: keyArbitrary, key: keyArbitrary }),
      ({ prefix, key }) =>
        keyRangeContains(keyRangeOfPrefix(prefix), key) === startsWith(key, prefix),
    );
  });
});

describe("key ranges over every small key (mutation sweep, D117)", () => {
  // Every two-byte key over {0, 1, 2}. Each range's bounds are among them, so a range holds some
  // byte string exactly when it holds one of these: the enumeration is a complete oracle.
  const keys: Array<Uint8Array> = [];
  for (const a of [0, 1, 2]) for (const b of [0, 1, 2]) keys.push(Uint8Array.of(a, b));
  // Non-empty ranges only: a database's range always holds a key (start < end), and these functions
  // assume it.
  const ranges: Array<KeyRange> = [];
  for (const [index, start] of keys.entries()) {
    ranges.push({ start, end: Option.none() });
    for (const end of keys.slice(index + 1)) ranges.push({ start, end: Option.some(end) });
  }
  const members = (range: KeyRange) => keys.filter((key) => keyRangeContains(range, key));
  const compare = (left: Uint8Array, right: Uint8Array) => {
    for (let index = 0; index < Math.min(left.length, right.length); index++) {
      const order = (left[index] ?? 0) - (right[index] ?? 0);
      if (order !== 0) return order;
    }
    return left.length - right.length;
  };
  // The oracle: membership written out, independent of the code under test.
  const holds = (range: KeyRange, key: Uint8Array) =>
    compare(key, range.start) >= 0 &&
    (Option.isNone(range.end) || compare(key, range.end.value) < 0);

  it("contains, covers, overlaps and intersects exactly as the membership oracle says", () => {
    const mismatches: Array<string> = [];
    for (const range of ranges) {
      if (members(range).length !== keys.filter((key) => holds(range, key)).length)
        mismatches.push("contains");
      for (const [first, last] of [
        [keys[1], keys[4]],
        [keys[4], keys[4]],
        [keys[0], keys[0]],
        [keys[8], keys[8]],
        [keys[3], keys[7]],
      ] as const) {
        if (first === undefined || last === undefined) continue;
        const expected = keys.some(
          (key) => compare(key, first) >= 0 && compare(key, last) <= 0 && holds(range, key),
        );
        if (keyRangeOverlapsClosed(range, first, last) !== expected)
          mismatches.push("overlapsClosed");
      }
    }
    for (const left of ranges) {
      const leftKeys = keys.filter((key) => holds(left, key));
      for (const right of ranges) {
        const both = leftKeys.filter((key) => holds(right, key));
        if (keyRangesOverlap(left, right) !== both.length > 0) mismatches.push("overlap");
        const intersection = intersectKeyRanges(left, right);
        const intersected = Option.match(intersection, {
          onNone: () => [],
          onSome: (range) => keys.filter((key) => holds(range, key)),
        });
        if (intersected.length !== both.length) mismatches.push("intersect");
        // Covers: every key the inner range holds, the outer holds; an empty inner range aside.
        const rightKeys = keys.filter((key) => holds(right, key));
        if (
          rightKeys.length > 0 &&
          keyRangeCovers(left, right) !== rightKeys.every((key) => holds(left, key))
        )
          mismatches.push(`covers`);
      }
    }
    expect(mismatches).toEqual([]);
  });

  it("intersects ranges that only touch to none, not to an empty range", () => {
    const range = (start: number, end: number): KeyRange => ({
      start: Uint8Array.of(start),
      end: Option.some(Uint8Array.of(end)),
    });
    expect(Option.isNone(intersectKeyRanges(range(1, 3), range(3, 5)))).toBe(true);
    expect(Option.isNone(intersectKeyRanges(range(3, 5), range(1, 3)))).toBe(true);
    expect(
      Option.map(intersectKeyRanges(range(1, 4), range(3, 5)), (both) => [
        [...both.start],
        Option.map(both.end, (end) => [...end]),
      ]),
    ).toEqual(Option.some([[3], Option.some([4])]));
  });

  it("gives a prefix of all 0xff no end, and increments the last byte that is not 0xff", () => {
    const end = (prefix: ReadonlyArray<number>) =>
      Option.map(keyRangeOfPrefix(Uint8Array.from(prefix)).end, (bytes) => [...bytes]);
    expect(end([0xff, 0xff])).toEqual(Option.none());
    expect(end([])).toEqual(Option.none());
    expect(end([0x01, 0xff])).toEqual(Option.some([0x02]));
    expect(end([0x01, 0x02])).toEqual(Option.some([0x01, 0x03]));
  });
});
