import * as Alchemy from "alchemy";
import * as Telemetry from "alchemy/Telemetry";
import { Config, Context, Effect, Layer, Option, Schema, Tracer } from "effect";

/**
 * Name of the binding that carries the Stack's trace sample rate (0 to 1) to every Worker. Bound
 * with the other worker settings on every deploy (D55, D63).
 */
export const LSM_TRACE_SAMPLE_RATE_SETTING = "CF_SLATEDB_TRACE_SAMPLE_RATE";

/**
 * First segments of the span attributes cf-slatedb's own code sets (`database.id`, `sst.id`,
 * `manifest.id`, `object.size`, ...). Each holds only safe identifiers, sizes, counts, and
 * outcomes: never a key, a value, or a secret (`AGENTS.md`).
 */
export const LSM_SPAN_ATTRIBUTE_NAMESPACES: ReadonlySet<string> = new Set([
  "batch",
  "block",
  "budget",
  "cache",
  "changelog",
  "changes",
  "checkpoint",
  "collection",
  "compaction",
  "compactor",
  "database",
  "drop",
  "filter",
  "flush",
  "fork",
  "gc",
  "maintenance",
  "manifest",
  "memtable",
  "object",
  "probe",
  "read",
  "remove",
  "scan",
  "seal",
  "shard",
  "snapshot",
  "split",
  "sst",
  "transaction",
  "workflow",
  "writer",
]);

/**
 * Span attributes outside those namespaces that are exported: `seq`, and the HTTP and runtime
 * attributes that name an operation without naming a caller.
 */
export const LSM_SPAN_ATTRIBUTE_KEYS: ReadonlySet<string> = new Set([
  "seq",
  "http.request.method",
  "http.response.status_code",
  "http.route",
  "url.path",
  "url.scheme",
  "server.address",
  "server.port",
  "error.type",
  "effect.fiber.interrupted",
]);

/**
 * Whether a span attribute may leave the Worker (D63): project attributes and the allowlisted
 * HTTP ones. Everything else is dropped where it is set, including `client.address`,
 * `user_agent.original`, `url.full` and `url.query` (which carry query strings), and request and
 * response headers.
 */
export const isExportedSpanAttribute = (key: string): boolean => {
  if (LSM_SPAN_ATTRIBUTE_KEYS.has(key)) return true;
  const dot = key.indexOf(".");
  return dot > 0 && LSM_SPAN_ATTRIBUTE_NAMESPACES.has(key.slice(0, dot));
};

/** How a Worker samples and redacts its spans. */
export interface LsmTracePolicy {
  /** Fraction of traces kept (1: every trace). */
  readonly sampleRate: number;
}

/**
 * Whether a trace is kept at `sampleRate`, from its id alone: its low 32 bits, read as a fraction
 * of 2^32, fall below the rate (OpenTelemetry's trace-id-ratio sampler). Every Worker and Durable
 * Object computes the same answer for the same trace.
 */
export const traceIdSampled = (traceId: string, sampleRate: number): boolean => {
  if (sampleRate >= 1) return true;
  const low = Number.parseInt(traceId.slice(-8), 16);
  return Number.isFinite(low) && low / 2 ** 32 < sampleRate;
};

/**
 * Wraps a tracer with a {@link LsmTracePolicy} (D63).
 *
 * **Details**
 *
 * - **Head sampling by trace id.** A span that starts a trace here (no parent, an explicit root, or
 *   a parent from another process: a caller's or another cf-slatedb Worker's `traceparent`) is
 *   kept when {@link traceIdSampled} says so for its trace id. Every hop of one trace, the API
 *   Worker and the Durable Objects and Workers it calls, decides the same, so a kept trace stays
 *   whole. A caller's `traceparent` joins our spans to its trace but cannot force them to be kept:
 *   instrumented clients send the sampled flag on every request, which would keep every trace.
 *   Spans under a local parent inherit its decision, as Effect does.
 * - **Redaction at the source.** Every attribute set on a span, by cf-slatedb or by the HTTP
 *   middleware, is checked with {@link isExportedSpanAttribute}; anything else is never recorded.
 */
export const lsmPolicyTracer = (inner: Tracer.Tracer, policy: LsmTracePolicy): Tracer.Tracer =>
  Tracer.make({
    span(options) {
      const fromAnotherProcess =
        Option.isSome(options.parent) && options.parent.value._tag === "ExternalSpan";
      const startsHere = options.root || Option.isNone(options.parent) || fromAnotherProcess;
      const span = inner.span(options);
      if (startsHere) {
        // Effect already turned an unsampled `traceparent` into `sampled: false`; the trace id
        // decides instead. A root keeps Effect's own (trace level) decision as a ceiling.
        const allowed = fromAnotherProcess || options.sampled;
        const sampled = allowed && traceIdSampled(span.traceId, policy.sampleRate);
        // The trace id exists only once the span does; nothing has read the decision yet.
        if (sampled !== span.sampled) Object.defineProperty(span, "sampled", { value: sampled });
      }
      const record = span.attribute.bind(span);
      span.attribute = (key, value) => {
        if (isExportedSpanAttribute(key)) record(key, value);
      };
      return span;
    },
    ...(inner.context === undefined ? {} : { context: inner.context }),
  });

const TraceSampleRate = Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

/**
 * The Worker's bound trace sample rate, read at runtime; 1 when unbound (tests, and Workers deployed
 * before D63).
 */
export const lsmTraceSampleRate = Config.schema(
  Schema.NumberFromString.pipe(Schema.decodeTo(TraceSampleRate)),
  LSM_TRACE_SAMPLE_RATE_SETTING,
).pipe(Config.withDefault(1));

/**
 * Applies the trace policy to a Worker's telemetry Layer (D63): the Layer still binds the exporters'
 * destinations, and each event's tracer becomes {@link lsmPolicyTracer} over them, with the bound
 * sample rate. Every cf-slatedb Worker provides its telemetry through this.
 *
 * **Details**
 *
 * Alchemy builds each event's exporters from the bound destinations, then merges any Layer
 * registered with `Telemetry.layer` over them, where a `Tracer` wins. At runtime this registers
 * {@link applyLsmTracePolicy} over those same destinations, so every span an event creates, the
 * HTTP middleware's server span included, goes through the policy. The bound exporters are also
 * built, and stay idle: nothing reaches their tracer. At plan time it only binds.
 */
export const withLsmTracePolicy = <E, R>(
  telemetry: Layer.Layer<never, E, R>,
): Layer.Layer<never, E, R> =>
  Layer.unwrap(
    Effect.gen(function* () {
      if ((yield* Alchemy.ALCHEMY_PHASE) === "plan") return telemetry;
      const sampleRate = yield* lsmTraceSampleRate.pipe(Effect.orElseSucceed(() => 1));
      return Layer.mergeAll(
        telemetry,
        Telemetry.layer(
          Layer.unwrap(
            Effect.map(Telemetry.Telemetry, (bound) => applyLsmTracePolicy(bound, { sampleRate })),
          ),
        ),
      );
    }),
  );

/** The event telemetry Layer `bound`, with its tracer wrapped by {@link lsmPolicyTracer}. */
export const applyLsmTracePolicy = (
  bound: Telemetry.TelemetryLayer,
  policy: LsmTracePolicy,
): Telemetry.TelemetryLayer =>
  Layer.effectContext(
    Effect.gen(function* () {
      const context = yield* Layer.build(bound);
      return Context.add(
        context,
        Tracer.Tracer,
        lsmPolicyTracer(Context.get(context, Tracer.Tracer), policy),
      );
    }),
  );
