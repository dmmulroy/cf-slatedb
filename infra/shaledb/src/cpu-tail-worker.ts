/**
 * A live stage's Tail Worker: Cloudflare hands it every finished invocation of the Workers that list
 * it as a tail consumer (the API Worker with its writer Durable Objects, and the compactor Worker),
 * and it stores one compact record per invocation under `tails/` in the stage's trace bucket.
 *
 * **Details**
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
}

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
}

const toRecord = (item: TraceItem): InvocationRecord => {
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
  };
};

export default {
  async tail(events: ReadonlyArray<TraceItem>, env: CpuTailEnv): Promise<void> {
    if (events.length === 0) return;
    const key = `tails/${Date.now().toString().padStart(15, "0")}-${crypto.randomUUID()}.json`;
    await env.TRACE_BUCKET.put(key, JSON.stringify(events.map(toRecord)), {
      httpMetadata: { contentType: "application/json" },
    });
  },
};
