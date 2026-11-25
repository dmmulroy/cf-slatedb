import { Schema } from "effect";

import { SeqNum } from "./ids.ts";

/**
 * Schema for how fresh a read must be.
 *
 * - `Latest`: sees every acknowledged write; costs the writer one memtable probe (SlateDB's
 *   `Memory` durability).
 * - `AtLeast`: sees at least the write with sequence number `seq` (read-your-writes from a
 *   `WriteReceipt`); skips the writer when the cached manifest already covers `seq`.
 * - `Flushed`: sees L0 and below, up to one memtable behind; never calls the writer (SlateDB's
 *   `Remote`).
 */
export const ReadConsistency = Schema.Union([
  Schema.TaggedStruct("Latest", {}),
  Schema.TaggedStruct("AtLeast", { seq: SeqNum }),
  Schema.TaggedStruct("Flushed", {}),
]);

/** How fresh a read must be. */
export type ReadConsistency = typeof ReadConsistency.Type;
