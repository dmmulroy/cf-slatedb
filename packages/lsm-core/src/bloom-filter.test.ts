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

describe("the Bloom filter's on-disk bits (mutation sweep, D117)", () => {
  // Key n is n bytes long, so the keys cover every tail length of the hash (n mod 4).
  const keys = Array.from({ length: 10 }, (_, n) =>
    Uint8Array.from({ length: n }, (_, index) => (n * 37 + index * 11) & 0xff),
  );
  const hex = (bytes: Uint8Array) =>
    Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

  it("builds exactly the filters SSTs already store: the hash and the probe positions are a format", () => {
    // Written SSTs hold these bits. A filter built differently answers `absent` for keys they hold.
    expect(keys.map((key) => hex(buildBloomFilter([key], 10).bits))).toEqual([
      "8040000402011008",
      "0882000041100020",
      "0208102040800001",
      "1280002000094000",
      "0880400004022010",
      "0000489224000000",
      "0102040810408000",
      "0040100401008220",
      "0240008800100120",
      "0088001001200240",
    ]);
    const all = buildBloomFilter(keys, 10);
    expect([all.numHashes, hex(all.bits)]).toEqual([7, "9020f8108f8e9a92cd2b737f50"]);
  });

  it("holds every key after the builder grows, and a filter with no bits answers maybe", () => {
    const many = Array.from({ length: 3_000 }, (_, index) =>
      Uint8Array.of(index >> 8, index & 0xff, 7),
    );
    const filter = buildBloomFilter(many, 10);
    expect(many.every((key) => bloomFilterMayContain(filter, key))).toBe(true);
    expect(bloomFilterMayContain({ numHashes: 7, bits: new Uint8Array(0) }, Uint8Array.of(1))).toBe(
      true,
    );
  });

  it("clamps the probe count to 1-30, as documented", () => {
    const probes = (bitsPerKey: number) =>
      buildBloomFilter([Uint8Array.of(1)], bitsPerKey).numHashes;
    expect([probes(0.1), probes(10), probes(43), probes(44), probes(1_000)]).toEqual([
      1, 7, 30, 30, 30,
    ]);
  });
});
