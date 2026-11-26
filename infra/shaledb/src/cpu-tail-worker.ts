/**
 * A live stage's Tail Worker: Cloudflare hands it every finished invocation of the Workers that list
 * it as a tail consumer (the API Worker with its writer Durable Objects, and the compactor Worker),
 * and it stores a compact record of each invocation it keeps under `tails/` in the stage's trace
 * bucket.
 *
 * **Details**
 *
 * It keeps every invocation that did not end `ok` (a limit hit, an exception, a cancellation), and
 * of the `ok` ones the fraction the stage samples traces at (`TAIL_SAMPLE_RATE`: 10% in
 * production, all of them on previews). Each stored object is one R2 write, and a busy stage
 * finishes a hundred invocations a second, so keeping them all would cost millions of writes a
 * day and could not be read back as fast as it is written (D71). A record carries the rate it
 * was kept at, so a report scales `ok` counts back up.
 *
 * A Worker cannot read its own CPU time (timers advance only across I/O), so tail events are where
 * `cpuTime` and `wallTime` come from (`docs/measurements.md`). Each record keeps only safe fields:
 * the script, the Durable Object id, the event kind, the request method and URL path (database ids,
 * never keys or values), the response status, the outcome, and the times. Headers, logs, and
 * exception messages are dropped.
 *
 * It is a plain module Worker, not an Effect one: Alchemy's Effect-native Workers do not export a
 * `tail()` handler.
 */
import type { R2Bucket, TraceItem } from "@cloudflare/workers-types";

/** Environment of the tail Worker. */
interface CpuTailEnv {
  /** The stage's trace bucket. */
  readonly TRACE_BUCKET: R2Bucket;
  /** Fraction of `ok` invocations to keep, from 0 to 1; unset or unreadable keeps all. */
  readonly TAIL_SAMPLE_RATE?: string;
}

/** The fraction of `ok` invocations a Tail Worker bound `TAIL_SAMPLE_RATE = bound` keeps. */
export const tailSampleRateOf = (bound: string | undefined): number => {
  const rate = Number(bound ?? "1");
  return Number.isFinite(rate) && rate >= 0 && rate <= 1 ? rate : 1;
};

/**
 * Whether an invocation's record is kept: always when it did not end `ok`, otherwise when
 * `random` (uniform in [0, 1)) falls below `sampleRate`.
 */
export const keepsInvocationRecord = (
  outcome: string,
  sampleRate: number,
  random: number,
): boolean => outcome !== "ok" || random < sampleRate;

/** One invocation's resource use, as stored under `tails/`. */
export interface InvocationRecord {
  readonly scriptName: string | null;
  readonly durableObjectId: string | null;
  readonly eventKind: "fetch" | "alarm" | "rpc" | "scheduled" | "other";
  readonly method: string | null;
  readonly path: string | null;
  readonly status: number | null;
  readonly outcome: string;
  readonly eventTimestamp: number | null;
  readonly cpuTimeMs: number;
  readonly wallTimeMs: number;
  /**
   * The fraction of invocations like this one that are kept: this record stands for
   * `1 / sampleRate` invocations. 1 for every record that did not end `ok`.
   */
  readonly sampleRate: number;
}

const toRecord = (item: TraceItem, sampleRate: number): InvocationRecord => {
  const event = item.event;
  const fetch = event !== null && "request" in event ? event : null;
  return {
    scriptName: item.scriptName,
    durableObjectId: item.durableObjectId ?? null,
    eventKind:
      fetch !== null
        ? "fetch"
        : event !== null && "scheduledTime" in event && !("cron" in event)
          ? "alarm"
          : event !== null && "rpcMethod" in event
            ? "rpc"
            : event !== null && "cron" in event
              ? "scheduled"
              : "other",
    method: fetch?.request.method ?? null,
    path: fetch === null ? null : new URL(fetch.request.url).pathname,
    status: fetch?.response?.status ?? null,
    outcome: item.outcome,
    eventTimestamp: item.eventTimestamp,
    cpuTimeMs: item.cpuTime,
    wallTimeMs: item.wallTime,
    sampleRate: item.outcome === "ok" ? sampleRate : 1,
  };
};

export default {
  async tail(events: ReadonlyArray<TraceItem>, env: CpuTailEnv): Promise<void> {
    const sampleRate = tailSampleRateOf(env.TAIL_SAMPLE_RATE);
    const kept = events.filter((event) =>
      keepsInvocationRecord(event.outcome, sampleRate, Math.random()),
    );
    if (kept.length === 0) return;
    const key = `tails/${Date.now().toString().padStart(15, "0")}-${crypto.randomUUID()}.json`;
    const records = kept.map((event) => toRecord(event, sampleRate));
    await env.TRACE_BUCKET.put(key, JSON.stringify(records), {
      httpMetadata: { contentType: "application/json" },
    });
  },
};
