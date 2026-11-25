import { Arbitrary, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { BloomFilterBuilder, bloomFilterMayContain, buildBloomFilter } from "./bloom-filter.ts";
import { assertProperty, keyToHex } from "./lsm-core-arbitraries.ts";

const keyBytes = Arbitrary.array(
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 255 }))),
  {
    minLength: 1,
    maxLength: 12,
  },
).pipe(Arbitrary.map((bytes) => Uint8Array.from(bytes)));

/** Distinct 8-byte keys built from `prefix` and a counter. */
const keysFrom = (prefix: number, count: number): Array<Uint8Array> =>
  Array.from({ length: count }, (_, index) => {
    const key = new Uint8Array(8);
    const view = new DataView(key.buffer);
    view.setUint32(0, prefix);
    view.setUint32(4, index);
    return key;
  });

describe("Bloom filter", () => {
  it("never reports an added key as absent", () => {
    assertProperty(
      Arbitrary.all({
        keys: Arbitrary.array(keyBytes, { maxLength: 200 }),
        bitsPerKey: Arbitrary.schema(
          Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })),
        ),
      }),
      ({ keys, bitsPerKey }) => {
        const filter = buildBloomFilter(keys, bitsPerKey);
        return keys.every((key) => bloomFilterMayContain(filter, key));
      },
    );
  });

  it("keeps the false-positive rate near the theory for the configured bits per key", () => {
    for (const bitsPerKey of [6, 10, 14]) {
      const builder = new BloomFilterBuilder();
      for (const key of keysFrom(1, 10_000)) builder.addKey(key);
      const filter = builder.build(bitsPerKey);
      const probes = keysFrom(2, 20_000);
      const falsePositives = probes.filter((key) => bloomFilterMayContain(filter, key)).length;
      const rate = falsePositives / probes.length;
      // Optimal-probe theory: (1/2)^(bitsPerKey ln 2) ~ 0.6185^bitsPerKey (10 bits per key: ~0.8%).
      // Allow 2x for rounding the probe count, hash imperfection, and sampling noise.
      const theory = 0.6185 ** bitsPerKey;
      expect(rate).toBeLessThan(theory * 2 + 0.002);
      expect(filter.bits.length * 8).toBeGreaterThanOrEqual(10_000 * bitsPerKey);
    }
  });

  it("builds the same filter from hashes as from keys, whatever the insertion order", () => {
    assertProperty(Arbitrary.array(keyBytes, { maxLength: 100 }), (keys) => {
      const forward = buildBloomFilter(keys, 10);
      const backward = buildBloomFilter([...keys].reverse(), 10);
      return (
        forward.numHashes === backward.numHashes &&
        keyToHex(forward.bits) === keyToHex(backward.bits)
      );
    });
  });
});
