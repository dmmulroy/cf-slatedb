import { Arbitrary, Option, Schema } from "effect";
import { describe, it } from "vite-plus/test";

import {
  intersectKeyRanges,
  type KeyRange,
  keyRangeContains,
  keyRangeOfPrefix,
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
