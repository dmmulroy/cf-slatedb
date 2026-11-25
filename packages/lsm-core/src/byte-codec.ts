/**
 * Growable byte buffer for the hand-written SST codecs: varints, fixed-width little-endian
 * integers, and raw bytes.
 */
export class ByteWriter {
  private buffer: Uint8Array;
  private view: DataView;
  /** Bytes written so far. */
  length = 0;

  constructor(initialCapacity = 256) {
    this.buffer = new Uint8Array(initialCapacity);
    this.view = new DataView(this.buffer.buffer);
  }

  private reserve(additional: number): void {
    const needed = this.length + additional;
    if (needed <= this.buffer.length) return;
    let capacity = this.buffer.length * 2;
    while (capacity < needed) capacity *= 2;
    const next = new Uint8Array(capacity);
    next.set(this.buffer.subarray(0, this.length));
    this.buffer = next;
    this.view = new DataView(next.buffer);
  }

  /** Appends an unsigned LEB128 varint; `value` must be a non-negative safe integer. */
  varint(value: number): this {
    this.reserve(8);
    let remaining = value;
    while (remaining >= 0x80) {
      this.buffer[this.length++] = (remaining % 0x80) | 0x80;
      remaining = Math.floor(remaining / 0x80);
    }
    this.buffer[this.length++] = remaining;
    return this;
  }

  /** Appends one byte. */
  u8(value: number): this {
    this.reserve(1);
    this.buffer[this.length++] = value & 0xff;
    return this;
  }

  /** Appends a little-endian u16. */
  u16(value: number): this {
    this.reserve(2);
    this.view.setUint16(this.length, value, true);
    this.length += 2;
    return this;
  }

  /** Appends a little-endian u32. */
  u32(value: number): this {
    this.reserve(4);
    this.view.setUint32(this.length, value >>> 0, true);
    this.length += 4;
    return this;
  }

  /** Appends a little-endian u64 from a non-negative safe integer. */
  u64(value: number): this {
    this.reserve(8);
    this.view.setBigUint64(this.length, BigInt(value), true);
    this.length += 8;
    return this;
  }

  /** Appends raw bytes. */
  bytes(value: Uint8Array): this {
    this.reserve(value.length);
    this.buffer.set(value, this.length);
    this.length += value.length;
    return this;
  }

  /** Appends a varint length followed by the bytes. */
  lengthPrefixed(value: Uint8Array): this {
    return this.varint(value.length).bytes(value);
  }

  /** The written bytes, copied into a right-sized array. */
  toBytes(): Uint8Array {
    return this.buffer.slice(0, this.length);
  }

  /** Forgets the written bytes, keeping the allocated capacity. */
  reset(): void {
    this.length = 0;
  }
}

/** Thrown by {@link ByteReader} when a read runs past the end; codecs translate it to a typed error. */
export class ByteReadOutOfBounds extends Error {}

/** Cursor over bytes written by {@link ByteWriter}. */
export class ByteReader {
  private readonly view: DataView;
  /** Current read position. */
  offset: number;

  constructor(
    private readonly source: Uint8Array,
    offset = 0,
  ) {
    this.view = new DataView(source.buffer, source.byteOffset, source.byteLength);
    this.offset = offset;
  }

  private need(count: number): void {
    if (this.offset + count > this.source.length) {
      throw new ByteReadOutOfBounds(
        `read of ${count} bytes at ${this.offset} past ${this.source.length}`,
      );
    }
  }

  /** Reads an unsigned LEB128 varint. */
  varint(): number {
    let result = 0;
    let multiplier = 1;
    for (let index = 0; index < 8; index++) {
      this.need(1);
      const byte = this.source[this.offset++] ?? 0;
      result += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0) return result;
      multiplier *= 0x80;
    }
    throw new ByteReadOutOfBounds("varint longer than 8 bytes");
  }

  /** Reads one byte. */
  u8(): number {
    this.need(1);
    return this.source[this.offset++] ?? 0;
  }

  /** Reads a little-endian u16. */
  u16(): number {
    this.need(2);
    const value = this.view.getUint16(this.offset, true);
    this.offset += 2;
    return value;
  }

  /** Reads a little-endian u32. */
  u32(): number {
    this.need(4);
    const value = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  /** Reads a little-endian u64 that must fit a safe integer. */
  u64(): number {
    this.need(8);
    const value = this.view.getBigUint64(this.offset, true);
    this.offset += 8;
    if (value > BigInt(Number.MAX_SAFE_INTEGER))
      throw new ByteReadOutOfBounds("u64 above 2^53 - 1");
    return Number(value);
  }

  /** Reads `count` raw bytes as a view (no copy). */
  bytes(count: number): Uint8Array {
    this.need(count);
    const value = this.source.subarray(this.offset, this.offset + count);
    this.offset += count;
    return value;
  }

  /** Reads a varint length and that many bytes. */
  lengthPrefixed(): Uint8Array {
    return this.bytes(this.varint());
  }

  /** Whether every byte has been read. */
  get done(): boolean {
    return this.offset >= this.source.length;
  }
}
