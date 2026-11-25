import { Arbitrary, Effect, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { SstId } from "./ids.ts";
import {
  intersectKeyRanges,
  type KeyRange,
  keyRangeContains,
  keyRangeOfPrefix,
  keyRangesOverlap,
} from "./key-range.ts";
import { assertProperty, keyArbitrary } from "./lsm-core-arbitraries.ts";
import { generateSstId, ulidTimeMillis } from "./ulid.ts";

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

describe("ULIDs", () => {
  it("are valid SST ids that encode the clock time", async () => {
    const ids = await Effect.gen(function* () {
      yield* TestClock.setTime(1_700_000_000_000);
      const first = yield* generateSstId;
      yield* TestClock.adjust("1 second");
      const second = yield* generateSstId;
      return [first, second] as const;
    }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);
    expect(Schema.is(SstId)(ids[0])).toBe(true);
    expect(ulidTimeMillis(ids[0])).toBe(1_700_000_000_000);
    expect(ulidTimeMillis(ids[1])).toBe(1_700_000_001_000);
    expect(ids[0] < ids[1]).toBe(true);
  });
});
