import { createHash } from "node:crypto";

import { Arbitrary, Effect, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { md5Hex } from "./md5.ts";

const nodeMd5 = (bytes: Uint8Array) => createHash("md5").update(bytes).digest("hex");

describe("md5Hex", () => {
  it("matches RFC 1321's test vectors", () => {
    const text = (value: string) => md5Hex(new TextEncoder().encode(value));
    expect(text("")).toBe("d41d8cd98f00b204e9800998ecf8427e");
    expect(text("abc")).toBe("900150983cd24fb0d6963f7d28e17f72");
    expect(text("message digest")).toBe("f96b697d7cb7938d525a2f31aaf161d0");
    expect(
      text("12345678901234567890123456789012345678901234567890123456789012345678901234567890"),
    ).toBe("57edf4a22be3c955ac49da2e2107b67a");
  });

  it("matches node:crypto for any bytes, at every padding boundary and from any buffer offset", () => {
    const result = Effect.runSync(
      Arbitrary.checkEffect(
        Arbitrary.all({
          bytes: Arbitrary.schema(Schema.Uint8Array),
          offset: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 7 }))),
        }),
        ({ bytes, offset }) => {
          // A view into a larger buffer, as a part sliced from a stream is.
          const backing = new Uint8Array(bytes.length + offset);
          backing.set(bytes, offset);
          const view = backing.subarray(offset);
          return Effect.succeed(md5Hex(view) === nodeMd5(bytes));
        },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
    for (let length = 50; length < 140; length++) {
      const bytes = Uint8Array.from({ length }, (_, index) => (index * 31 + length) & 0xff);
      expect(md5Hex(bytes)).toBe(nodeMd5(bytes));
    }
  });

  it("hashes a whole 8 MiB part like node:crypto", () => {
    const part = Uint8Array.from(
      { length: 8 * 1024 * 1024 },
      (_, index) => (index * 2_654_435_761) >>> 24,
    );
    expect(md5Hex(part)).toBe(nodeMd5(part));
  });
});
