import { Option, Schema } from "effect";

/** Schema for one span attribute value as the trace store keeps it: a scalar or a list of scalars. */
export const SpanAttributeValue = Schema.Union([
  Schema.String,
  Schema.Number,
  Schema.Boolean,
  Schema.Array(Schema.Union([Schema.String, Schema.Number, Schema.Boolean])),
]);

/** One span attribute value: a scalar or a list of scalars. */
export type SpanAttributeValue = typeof SpanAttributeValue.Type;

/** Schema for a span's status: `Error` carries the first error message the exporter recorded. */
export const SpanStatus = Schema.Struct({
  code: Schema.Literals(["Unset", "Ok", "Error"]),
  message: Schema.OptionFromNullOr(Schema.String),
});

/** A span's status, as OTLP reports it. */
export type SpanStatus = typeof SpanStatus.Type;

/** Schema for one span event, such as an `exception` event recorded with an error. */
export const SpanEvent = Schema.Struct({
  name: Schema.String,
  timeUnixNano: Schema.String,
  attributes: Schema.Record(Schema.String, SpanAttributeValue),
});

/** One span event. */
export type SpanEvent = typeof SpanEvent.Type;

/**
 * Schema for one span collected by the trace CLI's OTLP receiver.
 *
 * **Details**
 *
 * Timestamps stay as decimal strings of Unix nanoseconds, as OTLP/JSON sends them, because they do
 * not fit a JavaScript number. `durationMs` is derived once at collection.
 */
export const CollectedSpan = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  parentSpanId: Schema.OptionFromNullOr(Schema.String),
  name: Schema.String,
  kind: Schema.Number,
  serviceName: Schema.String,
  startTimeUnixNano: Schema.String,
  endTimeUnixNano: Schema.String,
  durationMs: Schema.Number,
  attributes: Schema.Record(Schema.String, SpanAttributeValue),
  status: SpanStatus,
  events: Schema.Array(SpanEvent),
});

/** One span collected by the OTLP receiver. */
export type CollectedSpan = typeof CollectedSpan.Type;

/** Codec for one line of a trace file in the store: a JSON-encoded {@link CollectedSpan}. */
export const CollectedSpanJsonLine = Schema.fromJsonString(CollectedSpan);

/**
 * Computes a span's duration in milliseconds from its OTLP nanosecond timestamps.
 *
 * @returns The duration, or `0` when the end precedes the start.
 */
export const spanDurationMs = (startTimeUnixNano: string, endTimeUnixNano: string): number => {
  const nanos = BigInt(endTimeUnixNano) - BigInt(startTimeUnixNano);
  return nanos <= 0n ? 0 : Number(nanos) / 1_000_000;
};

/**
 * Reads a span attribute as a string, for filtering by identifiers such as `database.id`.
 *
 * @returns The attribute rendered as a string, or `Option.none()` when the span lacks it.
 */
export const spanAttributeText = (span: CollectedSpan, key: string): Option.Option<string> => {
  const value = span.attributes[key];
  if (value === undefined) return Option.none();
  return Option.some(Array.isArray(value) ? value.join(",") : String(value));
};
