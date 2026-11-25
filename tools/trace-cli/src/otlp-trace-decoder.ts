import { Data, Effect, Option, Schema } from "effect";

import {
  type CollectedSpan,
  type SpanAttributeValue,
  type SpanEvent,
  type SpanStatus,
  spanDurationMs,
} from "./collected-span.ts";

/**
 * Failure to read an OTLP trace export request body.
 *
 * `reason` is `MalformedJson` when a JSON body does not match the OTLP/JSON trace schema, and
 * `MalformedProtobuf` when a protobuf body cannot be parsed as an `ExportTraceServiceRequest`.
 */
export class OtlpTraceDecodeError extends Data.TaggedError("OtlpTraceDecodeError")<{
  readonly reason: "MalformedJson" | "MalformedProtobuf";
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------------------------
// OTLP/JSON
// ---------------------------------------------------------------------------------------------

interface OtlpJsonAnyValue {
  readonly stringValue?: string | undefined;
  readonly boolValue?: boolean | undefined;
  readonly intValue?: string | number | undefined;
  readonly doubleValue?: string | number | undefined;
  readonly arrayValue?:
    | { readonly values?: ReadonlyArray<OtlpJsonAnyValue> | undefined }
    | undefined;
  readonly kvlistValue?:
    | { readonly values?: ReadonlyArray<OtlpJsonKeyValue> | undefined }
    | undefined;
  readonly bytesValue?: string | undefined;
}

interface OtlpJsonKeyValue {
  readonly key: string;
  readonly value?: OtlpJsonAnyValue | undefined;
}

const OtlpJsonAnyValue: Schema.Codec<OtlpJsonAnyValue> = Schema.Struct({
  stringValue: Schema.optional(Schema.String),
  boolValue: Schema.optional(Schema.Boolean),
  intValue: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  doubleValue: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  arrayValue: Schema.optional(
    Schema.Struct({
      values: Schema.optional(Schema.Array(Schema.suspend(() => OtlpJsonAnyValue))),
    }),
  ),
  kvlistValue: Schema.optional(
    Schema.Struct({
      values: Schema.optional(Schema.Array(Schema.suspend(() => OtlpJsonKeyValue))),
    }),
  ),
  bytesValue: Schema.optional(Schema.String),
});

const OtlpJsonKeyValue: Schema.Codec<OtlpJsonKeyValue> = Schema.Struct({
  key: Schema.String,
  value: Schema.optional(OtlpJsonAnyValue),
});

const OtlpJsonAttributes = Schema.optional(Schema.Array(OtlpJsonKeyValue));

const OtlpJsonSpan = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  parentSpanId: Schema.optional(Schema.NullOr(Schema.String)),
  name: Schema.String,
  kind: Schema.optional(Schema.Number),
  startTimeUnixNano: Schema.Union([Schema.String, Schema.Number]),
  endTimeUnixNano: Schema.Union([Schema.String, Schema.Number]),
  attributes: OtlpJsonAttributes,
  events: Schema.optional(
    Schema.Array(
      Schema.Struct({
        name: Schema.String,
        timeUnixNano: Schema.Union([Schema.String, Schema.Number]),
        attributes: OtlpJsonAttributes,
      }),
    ),
  ),
  status: Schema.optional(
    Schema.Struct({
      code: Schema.optional(Schema.Number),
      message: Schema.optional(Schema.String),
    }),
  ),
});

const OtlpJsonTraceRequest = Schema.Struct({
  resourceSpans: Schema.optional(
    Schema.Array(
      Schema.Struct({
        resource: Schema.optional(Schema.Struct({ attributes: OtlpJsonAttributes })),
        scopeSpans: Schema.optional(
          Schema.Array(Schema.Struct({ spans: Schema.optional(Schema.Array(OtlpJsonSpan)) })),
        ),
      }),
    ),
  ),
});

const decodeOtlpJsonTraceRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OtlpJsonTraceRequest),
);

/** Flattens a nested list attribute into one comma-separated scalar. */
const scalarOf = (value: SpanAttributeValue): string | number | boolean =>
  typeof value === "object" ? value.join(",") : value;

const jsonAnyValueToAttribute = (value: OtlpJsonAnyValue | undefined): SpanAttributeValue => {
  if (value === undefined) return "";
  if (value.stringValue !== undefined) return value.stringValue;
  if (value.boolValue !== undefined) return value.boolValue;
  if (value.intValue !== undefined) return Number(value.intValue);
  if (value.doubleValue !== undefined) return Number(value.doubleValue);
  if (value.bytesValue !== undefined) return value.bytesValue;
  if (value.arrayValue !== undefined) {
    return (value.arrayValue.values ?? []).map((item) => scalarOf(jsonAnyValueToAttribute(item)));
  }
  if (value.kvlistValue !== undefined) {
    return JSON.stringify(
      Object.fromEntries(
        (value.kvlistValue.values ?? []).map((kv) => [kv.key, jsonAnyValueToAttribute(kv.value)]),
      ),
    );
  }
  return "";
};

const jsonAttributes = (
  attributes: ReadonlyArray<OtlpJsonKeyValue> | undefined,
): Record<string, SpanAttributeValue> =>
  Object.fromEntries((attributes ?? []).map((kv) => [kv.key, jsonAnyValueToAttribute(kv.value)]));

const statusFromCode = (code: number | undefined, message: string | undefined): SpanStatus => ({
  code: code === 2 ? "Error" : code === 1 ? "Ok" : "Unset",
  message: message === undefined || message === "" ? Option.none() : Option.some(message),
});

const serviceNameOf = (attributes: Record<string, SpanAttributeValue>): string => {
  const name = attributes["service.name"];
  return typeof name === "string" ? name : "unknown";
};

/**
 * Decodes an OTLP/JSON `ExportTraceServiceRequest` body into collected spans.
 *
 * @param body - The raw request body text.
 * @returns The spans of every resource and scope, or `OtlpTraceDecodeError` (`MalformedJson`).
 */
export const decodeOtlpJsonTraces = Effect.fn("TraceCli.decodeOtlpJsonTraces")(
  function* (body: string) {
    const request = yield* decodeOtlpJsonTraceRequest(body);
    const spans: Array<CollectedSpan> = [];
    for (const resourceSpan of request.resourceSpans ?? []) {
      const serviceName = serviceNameOf(jsonAttributes(resourceSpan.resource?.attributes));
      for (const scopeSpan of resourceSpan.scopeSpans ?? []) {
        for (const span of scopeSpan.spans ?? []) {
          const start = String(span.startTimeUnixNano);
          const end = String(span.endTimeUnixNano);
          spans.push({
            traceId: span.traceId,
            spanId: span.spanId,
            parentSpanId:
              span.parentSpanId === undefined ||
              span.parentSpanId === null ||
              span.parentSpanId === ""
                ? Option.none()
                : Option.some(span.parentSpanId),
            name: span.name,
            kind: span.kind ?? 0,
            serviceName,
            startTimeUnixNano: start,
            endTimeUnixNano: end,
            durationMs: spanDurationMs(start, end),
            attributes: jsonAttributes(span.attributes),
            status: statusFromCode(span.status?.code, span.status?.message),
            events: (span.events ?? []).map((event): SpanEvent => ({
              name: event.name,
              timeUnixNano: String(event.timeUnixNano),
              attributes: jsonAttributes(event.attributes),
            })),
          });
        }
      }
    }
    return spans;
  },
  Effect.catchTags({
    SchemaError: (error) =>
      Effect.fail(
        new OtlpTraceDecodeError({
          reason: "MalformedJson",
          message: `The OTLP/JSON trace export body does not match the OTLP trace schema: ${error.message}`,
        }),
      ),
  }),
);

// ---------------------------------------------------------------------------------------------
// OTLP/protobuf
// ---------------------------------------------------------------------------------------------

/** Protobuf field read from a message: its number, wire type, and raw payload. */
type ProtobufField =
  | { readonly field: number; readonly wire: 0; readonly varint: bigint }
  | { readonly field: number; readonly wire: 1; readonly fixed64: DataView }
  | { readonly field: number; readonly wire: 2; readonly bytes: Uint8Array }
  | { readonly field: number; readonly wire: 5; readonly fixed32: DataView };

/** Thrown inside the private protobuf reader and caught at {@link decodeOtlpProtobufTraces}. */
class ProtobufReadFailure extends Error {}

const readFields = (bytes: Uint8Array): ReadonlyArray<ProtobufField> => {
  const fields: Array<ProtobufField> = [];
  let offset = 0;
  const readVarint = (): bigint => {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      const byte = bytes[offset];
      if (byte === undefined) throw new ProtobufReadFailure("truncated varint");
      offset += 1;
      result |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7n;
      if (shift > 70n) throw new ProtobufReadFailure("varint too long");
    }
  };
  while (offset < bytes.length) {
    const key = readVarint();
    const field = Number(key >> 3n);
    const wire = Number(key & 7n);
    if (wire === 0) {
      fields.push({ field, wire, varint: readVarint() });
    } else if (wire === 1) {
      if (offset + 8 > bytes.length) throw new ProtobufReadFailure("truncated fixed64");
      fields.push({
        field,
        wire,
        fixed64: new DataView(bytes.buffer, bytes.byteOffset + offset, 8),
      });
      offset += 8;
    } else if (wire === 2) {
      const length = Number(readVarint());
      if (offset + length > bytes.length) throw new ProtobufReadFailure("truncated bytes field");
      fields.push({ field, wire, bytes: bytes.subarray(offset, offset + length) });
      offset += length;
    } else if (wire === 5) {
      if (offset + 4 > bytes.length) throw new ProtobufReadFailure("truncated fixed32");
      fields.push({
        field,
        wire,
        fixed32: new DataView(bytes.buffer, bytes.byteOffset + offset, 4),
      });
      offset += 4;
    } else {
      throw new ProtobufReadFailure(`unsupported wire type ${wire}`);
    }
  }
  return fields;
};

const textDecoder = new TextDecoder();
const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const bytesOf = (field: ProtobufField): Uint8Array =>
  field.wire === 2 ? field.bytes : new Uint8Array(0);
const stringOf = (field: ProtobufField): string => textDecoder.decode(bytesOf(field));
const fixed64Of = (field: ProtobufField): bigint =>
  field.wire === 1 ? field.fixed64.getBigUint64(0, true) : 0n;
const varintOf = (field: ProtobufField): bigint => (field.wire === 0 ? field.varint : 0n);

const protobufAnyValue = (bytes: Uint8Array): SpanAttributeValue => {
  for (const field of readFields(bytes)) {
    switch (field.field) {
      case 1:
        return stringOf(field);
      case 2:
        return varintOf(field) !== 0n;
      case 3:
        return Number(BigInt.asIntN(64, varintOf(field)));
      case 4:
        return field.wire === 1 ? field.fixed64.getFloat64(0, true) : 0;
      case 5:
        return readFields(bytesOf(field))
          .filter((item) => item.field === 1)
          .map((item) => scalarOf(protobufAnyValue(bytesOf(item))));
      case 6:
        return JSON.stringify(
          protobufAttributes(readFields(bytesOf(field)).filter((f) => f.field === 1)),
        );
      case 7:
        return toHex(bytesOf(field));
    }
  }
  return "";
};

const protobufAttributes = (
  keyValues: ReadonlyArray<ProtobufField>,
): Record<string, SpanAttributeValue> => {
  const attributes: Record<string, SpanAttributeValue> = {};
  for (const keyValue of keyValues) {
    let key = "";
    let value: SpanAttributeValue = "";
    for (const field of readFields(bytesOf(keyValue))) {
      if (field.field === 1) key = stringOf(field);
      if (field.field === 2) value = protobufAnyValue(bytesOf(field));
    }
    attributes[key] = value;
  }
  return attributes;
};

const protobufSpan = (bytes: Uint8Array, serviceName: string): CollectedSpan => {
  let traceId = "";
  let spanId = "";
  let parentSpanId = Option.none<string>();
  let name = "";
  let kind = 0;
  let start = 0n;
  let end = 0n;
  const attributeFields: Array<ProtobufField> = [];
  const events: Array<SpanEvent> = [];
  let status: SpanStatus = { code: "Unset", message: Option.none() };
  for (const field of readFields(bytes)) {
    switch (field.field) {
      case 1:
        traceId = toHex(bytesOf(field));
        break;
      case 2:
        spanId = toHex(bytesOf(field));
        break;
      case 4: {
        const parent = toHex(bytesOf(field));
        parentSpanId = parent === "" ? Option.none() : Option.some(parent);
        break;
      }
      case 5:
        name = stringOf(field);
        break;
      case 6:
        kind = Number(varintOf(field));
        break;
      case 7:
        start = fixed64Of(field);
        break;
      case 8:
        end = fixed64Of(field);
        break;
      case 9:
        attributeFields.push(field);
        break;
      case 11: {
        let eventName = "";
        let time = 0n;
        const eventAttributes: Array<ProtobufField> = [];
        for (const eventField of readFields(bytesOf(field))) {
          if (eventField.field === 1) time = fixed64Of(eventField);
          if (eventField.field === 2) eventName = stringOf(eventField);
          if (eventField.field === 3) eventAttributes.push(eventField);
        }
        events.push({
          name: eventName,
          timeUnixNano: time.toString(),
          attributes: protobufAttributes(eventAttributes),
        });
        break;
      }
      case 15: {
        let code = 0;
        let message: string | undefined;
        for (const statusField of readFields(bytesOf(field))) {
          if (statusField.field === 2) message = stringOf(statusField);
          if (statusField.field === 3) code = Number(varintOf(statusField));
        }
        status = statusFromCode(code, message);
        break;
      }
    }
  }
  return {
    traceId,
    spanId,
    parentSpanId,
    name,
    kind,
    serviceName,
    startTimeUnixNano: start.toString(),
    endTimeUnixNano: end.toString(),
    durationMs: spanDurationMs(start.toString(), end.toString()),
    attributes: protobufAttributes(attributeFields),
    status,
    events,
  };
};

const readProtobufTraceRequest = (body: Uint8Array): ReadonlyArray<CollectedSpan> => {
  const spans: Array<CollectedSpan> = [];
  for (const resourceSpans of readFields(body).filter((field) => field.field === 1)) {
    const resourceFields = readFields(bytesOf(resourceSpans));
    const resource = resourceFields.find((field) => field.field === 1);
    const resourceAttributes = protobufAttributes(
      resource === undefined
        ? []
        : readFields(bytesOf(resource)).filter((field) => field.field === 1),
    );
    const serviceName = serviceNameOf(resourceAttributes);
    for (const scopeSpans of resourceFields.filter((field) => field.field === 2)) {
      for (const span of readFields(bytesOf(scopeSpans)).filter((field) => field.field === 2)) {
        spans.push(protobufSpan(bytesOf(span), serviceName));
      }
    }
  }
  return spans;
};

/**
 * Decodes an OTLP/protobuf `ExportTraceServiceRequest` body into collected spans.
 *
 * @param body - The raw request body bytes.
 * @returns The spans of every resource and scope, or `OtlpTraceDecodeError`
 *   (`MalformedProtobuf`).
 */
export const decodeOtlpProtobufTraces = (
  body: Uint8Array,
): Effect.Effect<ReadonlyArray<CollectedSpan>, OtlpTraceDecodeError> =>
  Effect.try({
    try: () => readProtobufTraceRequest(body),
    catch: (cause) =>
      new OtlpTraceDecodeError({
        reason: "MalformedProtobuf",
        message: `The OTLP/protobuf trace export body could not be parsed: ${
          cause instanceof ProtobufReadFailure ? cause.message : "unexpected structure"
        }.`,
      }),
  }).pipe(Effect.withSpan("TraceCli.decodeOtlpProtobufTraces"));
