/** CRC32C (Castagnoli) lookup table, reflected polynomial 0x82F63B78. */
const CRC32C_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index++) {
    let crc = index;
    for (let bit = 0; bit < 8; bit++) crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    table[index] = crc >>> 0;
  }
  return table;
})();

/**
 * Computes the CRC32C (Castagnoli) checksum of `bytes`, as SST blocks and sections store it.
 *
 * @param initial - A running checksum to continue from, for data split across buffers.
 * @returns The checksum as an unsigned 32-bit integer.
 */
export const crc32c = (bytes: Uint8Array, initial = 0): number => {
  let crc = ~initial >>> 0;
  for (let index = 0; index < bytes.length; index++) {
    crc = (CRC32C_TABLE[(crc ^ (bytes[index] ?? 0)) & 0xff] ?? 0) ^ (crc >>> 8);
  }
  return ~crc >>> 0;
};
