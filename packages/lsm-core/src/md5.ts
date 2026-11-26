/** Per-round left-rotation amounts (RFC 1321). */
const MD5_SHIFTS = Uint8Array.of(
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
);

/** Per-step constants: floor(|sin(i + 1)| × 2^32) (RFC 1321). */
const MD5_CONSTANTS = (() => {
  const table = new Int32Array(64);
  for (let index = 0; index < 64; index++) {
    table[index] = Math.floor(Math.abs(Math.sin(index + 1)) * 2 ** 32) | 0;
  }
  return table;
})();

/** Message word used by each step (RFC 1321). */
const MD5_WORD_INDEX = (() => {
  const table = new Uint8Array(64);
  for (let step = 0; step < 64; step++) {
    table[step] =
      step < 16
        ? step
        : step < 32
          ? (5 * step + 1) % 16
          : step < 48
            ? (3 * step + 5) % 16
            : (7 * step) % 16;
  }
  return table;
})();

/** Runs the compression function over one 64-byte block at `offset`, updating `state`. */
const md5Block = (state: Int32Array, view: DataView, offset: number, words: Int32Array): void => {
  for (let word = 0; word < 16; word++) words[word] = view.getInt32(offset + word * 4, true);
  let a = state[0] ?? 0;
  let b = state[1] ?? 0;
  let c = state[2] ?? 0;
  let d = state[3] ?? 0;
  for (let step = 0; step < 64; step++) {
    const mixed =
      step < 16
        ? (b & c) | (~b & d)
        : step < 32
          ? (d & b) | (~d & c)
          : step < 48
            ? b ^ c ^ d
            : c ^ (b | ~d);
    const sum =
      (a + mixed + (MD5_CONSTANTS[step] ?? 0) + (words[MD5_WORD_INDEX[step] ?? 0] ?? 0)) | 0;
    const shift = MD5_SHIFTS[step] ?? 0;
    a = d;
    d = c;
    c = b;
    b = (b + ((sum << shift) | (sum >>> (32 - shift)))) | 0;
  }
  state[0] = ((state[0] ?? 0) + a) | 0;
  state[1] = ((state[1] ?? 0) + b) | 0;
  state[2] = ((state[2] ?? 0) + c) | 0;
  state[3] = ((state[3] ?? 0) + d) | 0;
};

/**
 * Computes the MD5 digest of `bytes` (RFC 1321) as lowercase hex: what R2 returns as a multipart
 * part's ETag, so an upload can check that the part R2 stored is the part it sent (D62).
 *
 * MD5 is used only to detect accidental corruption in transit, never as a security property.
 */
export const md5Hex = (bytes: Uint8Array): string => {
  const state = Int32Array.of(0x67452301, 0xefcdab89 | 0, 0x98badcfe | 0, 0x10325476);
  const words = new Int32Array(16);
  const whole = bytes.length - (bytes.length % 64);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let offset = 0; offset < whole; offset += 64) md5Block(state, view, offset, words);
  // Padding: 0x80, zeros to 56 mod 64, then the bit length as a 64-bit little-endian integer.
  const tailLength = bytes.length - whole;
  const tail = new Uint8Array(tailLength < 56 ? 64 : 128);
  tail.set(bytes.subarray(whole));
  tail[tailLength] = 0x80;
  const tailView = new DataView(tail.buffer);
  const bitLength = bytes.length * 8;
  tailView.setUint32(tail.length - 8, bitLength >>> 0, true);
  tailView.setUint32(tail.length - 4, Math.floor(bitLength / 2 ** 32), true);
  for (let offset = 0; offset < tail.length; offset += 64) md5Block(state, tailView, offset, words);
  let hex = "";
  const digest = new DataView(state.buffer);
  for (let index = 0; index < 16; index++) {
    hex += digest.getUint8(index).toString(16).padStart(2, "0");
  }
  return hex;
};
