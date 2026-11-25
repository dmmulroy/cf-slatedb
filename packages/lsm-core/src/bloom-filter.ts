/**
 * 32-bit MurmurHash3 (x86) of `bytes` with `seed`.
 */
const murmur3 = (bytes: Uint8Array, seed: number): number => {
  let hash = seed >>> 0;
  const blocks = bytes.length >>> 2;
  for (let index = 0; index < blocks; index++) {
    const offset = index * 4;
    let k =
      (bytes[offset] ?? 0) |
      ((bytes[offset + 1] ?? 0) << 8) |
      ((bytes[offset + 2] ?? 0) << 16) |
      ((bytes[offset + 3] ?? 0) << 24);
    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);
    hash ^= k;
    hash = (hash << 13) | (hash >>> 19);
    hash = (Math.imul(hash, 5) + 0xe6546b64) | 0;
  }
  const tailOffset = blocks * 4;
  const remainder = bytes.length & 3;
  if (remainder > 0) {
    let tail = 0;
    if (remainder === 3) tail ^= (bytes[tailOffset + 2] ?? 0) << 16;
    if (remainder >= 2) tail ^= (bytes[tailOffset + 1] ?? 0) << 8;
    tail ^= bytes[tailOffset] ?? 0;
    tail = Math.imul(tail, 0xcc9e2d51);
    tail = (tail << 15) | (tail >>> 17);
    tail = Math.imul(tail, 0x1b873593);
    hash ^= tail;
  }
  hash ^= bytes.length;
  hash ^= hash >>> 16;
  hash = Math.imul(hash, 0x85ebca6b);
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0xc2b2ae35);
  hash ^= hash >>> 16;
  return hash >>> 0;
};

/**
 * The two halves of a key's 64-bit filter hash: two independently seeded 32-bit MurmurHash3 values,
 * combined by double hashing into `numHashes` probe positions.
 */
const keyHashes = (key: Uint8Array): readonly [number, number] => [
  murmur3(key, 0x9747b28c),
  murmur3(key, 0x5bd1e995) | 1,
];

/** A Bloom filter over an SST's keys: a definite "no" lets a point read skip the SST. */
export interface BloomFilter {
  readonly numHashes: number;
  readonly bits: Uint8Array;
}

/**
 * Accumulates the hashes of an SST's keys (8 bytes per key, not the keys themselves) and builds the
 * SST's Bloom filter at the end.
 */
export class BloomFilterBuilder {
  private hashes = new Uint32Array(1024);
  private count = 0;

  /** Number of keys added. */
  get keyCount(): number {
    return this.count;
  }

  /** Adds one key; add each distinct key once. */
  addKey(key: Uint8Array): void {
    if ((this.count + 1) * 2 > this.hashes.length) {
      const next = new Uint32Array(this.hashes.length * 2);
      next.set(this.hashes);
      this.hashes = next;
    }
    const [h1, h2] = keyHashes(key);
    this.hashes[this.count * 2] = h1;
    this.hashes[this.count * 2 + 1] = h2;
    this.count++;
  }

  /**
   * Builds the filter at `bitsPerKey` bits per key (rounded up to whole bytes, at least 64 bits),
   * with `round(bitsPerKey * ln 2)` probes clamped to 1–30.
   */
  build(bitsPerKey: number): BloomFilter {
    const numBits = Math.max(64, Math.ceil((this.count * bitsPerKey) / 8) * 8);
    const numHashes = Math.min(30, Math.max(1, Math.round(bitsPerKey * Math.LN2)));
    const bits = new Uint8Array(numBits / 8);
    for (let index = 0; index < this.count; index++) {
      const h1 = this.hashes[index * 2] ?? 0;
      const h2 = this.hashes[index * 2 + 1] ?? 1;
      for (let probe = 0; probe < numHashes; probe++) {
        const position = ((h1 + Math.imul(probe, h2)) >>> 0) % numBits;
        bits[position >>> 3] = (bits[position >>> 3] ?? 0) | (1 << (position & 7));
      }
    }
    return { numHashes, bits };
  }
}

/** Builds a Bloom filter over `keys` at `bitsPerKey` bits per key (see {@link BloomFilterBuilder}). */
export const buildBloomFilter = (keys: Iterable<Uint8Array>, bitsPerKey: number): BloomFilter => {
  const builder = new BloomFilterBuilder();
  for (const key of keys) builder.addKey(key);
  return builder.build(bitsPerKey);
};

/**
 * Whether `key` may be in the filter's set.
 *
 * @returns `false` only when the key is definitely absent.
 */
export const bloomFilterMayContain = (filter: BloomFilter, key: Uint8Array): boolean => {
  const numBits = filter.bits.length * 8;
  if (numBits === 0) return true;
  const [h1, h2] = keyHashes(key);
  for (let probe = 0; probe < filter.numHashes; probe++) {
    const position = ((h1 + Math.imul(probe, h2)) >>> 0) % numBits;
    if (((filter.bits[position >>> 3] ?? 0) & (1 << (position & 7))) === 0) return false;
  }
  return true;
};
