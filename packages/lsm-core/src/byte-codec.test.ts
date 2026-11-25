import { Arbitrary, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ByteReader, ByteReadOutOfBounds, ByteWriter } from "./byte-codec.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";

const safeInteger = Arbitrary.schema(
  Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
);
const smallInteger = Arbitrary.schema(
  Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 300 })),
);

/** Boundaries where a varint gains a byte, and the largest safe integer. */
const VARINT_BOUNDARIES = [
  0,
  1,
  127,
  128,
  16_383,
  16_384,
  2 ** 21 - 1,
  2 ** 21,
  2 ** 28,
  2 ** 35,
  2 ** 49,
  Number.MAX_SAFE_INTEGER,
];

describe("varint", () => {
  it("round-trips every safe integer, in sequence, using the minimal number of bytes", () => {
    assertProperty(
      Arbitrary.array(Arbitrary.all({ large: safeInteger, small: smallInteger }), {
        maxLength: 40,
      }),
      (pairs) => {
        const values = pairs.flatMap(({ large, small }) => [large, small]);
        const writer = new ByteWriter(4);
        for (const value of values) writer.varint(value);
        const reader = new ByteReader(writer.toBytes());
        const decoded = values.map(() => reader.varint());
        const expectedLength = values.reduce(
          (sum, value) => sum + Math.max(1, Math.ceil(Math.log2(value + 1) / 7)),
          0,
        );
        return (
          reader.done &&
          decoded.every((value, index) => value === values[index]) &&
          writer.length === expectedLength
        );
      },
    );
  });

  it("round-trips the byte-length boundaries", () => {
    const writer = new ByteWriter();
    for (const value of VARINT_BOUNDARIES) writer.varint(value);
    const reader = new ByteReader(writer.toBytes());
    expect(VARINT_BOUNDARIES.map(() => reader.varint())).toEqual(VARINT_BOUNDARIES);
  });

  it("rejects a truncated varint instead of reading past the end", () => {
    const bytes = new ByteWriter()
      .varint(2 ** 40)
      .toBytes()
      .slice(0, 3);
    expect(() => new ByteReader(bytes).varint()).toThrow(ByteReadOutOfBounds);
  });
});

describe("fixed-width integers", () => {
  it("round-trips u8, u16, u32, u64, and length-prefixed bytes", () => {
    assertProperty(
      Arbitrary.all({ value: safeInteger, small: smallInteger }),
      ({ value, small }) => {
        const payload = Uint8Array.from({ length: small % 40 }, (_, index) => index);
        const bytes = new ByteWriter(1)
          .u8(small & 0xff)
          .u16(small)
          .u32(value % 2 ** 32)
          .u64(value)
          .lengthPrefixed(payload)
          .toBytes();
        const reader = new ByteReader(bytes);
        return (
          reader.u8() === (small & 0xff) &&
          reader.u16() === small &&
          reader.u32() === value % 2 ** 32 &&
          reader.u64() === value &&
          reader.lengthPrefixed().every((byte, index) => byte === payload[index]) &&
          reader.done
        );
      },
    );
  });
});
