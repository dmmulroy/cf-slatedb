import { Clock, Effect, Random } from "effect";

import { CheckpointId, SstId } from "./ids.ts";

const CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/**
 * Encodes a ULID from a millisecond time and 80 random bits (as 16 random 5-bit symbols).
 *
 * @param randomSymbols - 16 integers in `[0, 32)`.
 */
export const encodeUlid = (timeMillis: number, randomSymbols: ReadonlyArray<number>): string => {
  let time = "";
  let remaining = timeMillis;
  for (let index = 0; index < 10; index++) {
    time = (CROCKFORD_BASE32[remaining % 32] ?? "0") + time;
    remaining = Math.floor(remaining / 32);
  }
  return time + randomSymbols.map((symbol) => CROCKFORD_BASE32[symbol & 31] ?? "0").join("");
};

const ulidString = Effect.gen(function* () {
  const now = yield* Clock.currentTimeMillis;
  const symbols: Array<number> = [];
  for (let index = 0; index < 16; index++) symbols.push(yield* Random.nextIntBetween(0, 32));
  return encodeUlid(now, symbols);
});

/**
 * Generates a new SST id: a ULID from the contextual `Clock` and `Random`, so SST names sort by
 * creation time and a fenced writer can only ever leave uniquely named orphans.
 */
export const generateSstId: Effect.Effect<SstId> = Effect.map(ulidString, (value) =>
  SstId.make(value),
);

/** Generates a new compaction job id (a ULID, so job ids sort by start time). */
export const generateCompactionJobId: Effect.Effect<string> = ulidString;

/** Generates a new checkpoint id (a ULID). */
export const generateCheckpointId: Effect.Effect<CheckpointId> = Effect.map(ulidString, (value) =>
  CheckpointId.make(value),
);

/** Creation time in milliseconds encoded in a ULID's first ten characters. */
export const ulidTimeMillis = (ulid: string): number => {
  let time = 0;
  for (const char of ulid.slice(0, 10)) time = time * 32 + CROCKFORD_BASE32.indexOf(char);
  return time;
};
