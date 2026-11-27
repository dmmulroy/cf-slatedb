import {
  collapseWriteBatch,
  DatabaseId,
  MAX_KEY_BYTES,
  MERGE_OPERATORS,
  keyVersionsOf,
  resolveVisibleRow,
  RowValue,
  SeqNum,
  Timestamp,
} from "@shaledb/lsm-core";
import { Cause, Context, Effect, Option, Result, Schema, Tracer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ReaderUnavailableError } from "./db-reader.ts";
import { GetRequest, ScanRequest } from "./db-http-api.ts";
import { type DbInternalError, toPublicError } from "./db-public-error-translation.ts";
import { exportedEventName, lsmPolicyTracer, OMITTED_LOG_PART } from "./lsm-trace-policy.ts";

/**
 * No key, value, or secret leaves through telemetry or an error (D123): random markers are put
 * where a careless line of code would put a key, and every exported span, event, and public error
 * body is searched for them in each form a key takes on its way out (UTF-8, base64, hex, the
 * comma-separated bytes of a serialized `Uint8Array`).
 */
const bytes = (text: string) => new TextEncoder().encode(text);
const forms = (marker: string): ReadonlyArray<string> => {
  const raw = bytes(marker);
  return [
    marker,
    btoa(marker),
    Array.from(raw, (byte) => byte.toString(16).padStart(2, "0")).join(""),
    Array.from(raw).join(","),
    Array.from(raw, (byte, index) => `"${index}":${byte}`).join(","),
  ];
};
const leaks = (exported: string, marker: string) =>
  forms(marker).filter((form) => exported.includes(form));

/** 25 random markers of 12 to 24 capital letters, unlikely in any code-written text. */
const markers = Array.from({ length: 25 }, () => {
  const length = 10 + Math.floor(Math.random() * 13);
  return `MK${Array.from({ length }, () => String.fromCharCode(65 + Math.floor(Math.random() * 26))).join("")}`;
});

/** A tracer under the policy whose spans record everything that reaches the exporter. */
const recordingPolicyTracer = () => {
  const exported: Array<string> = [];
  const inner = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const attribute = span.attribute.bind(span);
      span.attribute = (key, value) => {
        exported.push(`ATTR ${key}=${JSON.stringify(value)}`);
        attribute(key, value);
      };
      const event = span.event.bind(span);
      span.event = (name, time, attributes) => {
        exported.push(`EVENT ${name} ${JSON.stringify(attributes ?? {})}`);
        event(name, time, attributes);
      };
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        // What the OTLP exporter takes from a failure: each error's name, message, and stack.
        if (exit._tag === "Failure") {
          for (const error of Cause.prettyErrors(exit.cause)) {
            exported.push(`EXCEPTION ${error.name} ${error.message} ${error.stack ?? ""}`);
          }
        }
        end(time, exit);
      };
      return span;
    },
  });
  return { tracer: lsmPolicyTracer(inner, { sampleRate: 1 }), exported };
};

const DB = DatabaseId.make("orders");

describe("no key or value leaves through telemetry (D123)", () => {
  it("drops markers a log line or an attribute carries outside the policy, from every exported form", async () => {
    for (const marker of markers) {
      {
        const { tracer, exported } = recordingPolicyTracer();
        const key = bytes(marker);
        await Effect.runPromise(
          Effect.gen(function* () {
            // A log line with an object part, as `Effect.logError(text, { cause })` writes.
            yield* Effect.logWarning("Writer alarm could not flush", { cause: { key: marker } });
            // An annotation under a name the policy does not know, and a known name holding an object.
            yield* Effect.logError("read failed").pipe(
              Effect.annotateLogs({ key: marker, "database.id": { key: marker } }),
            );
            // Attributes: an unknown name, and a project name given an object.
            yield* Effect.annotateCurrentSpan({ "request.key": marker });
            yield* Effect.annotateCurrentSpan({ "sst.id": marker }).pipe(Effect.withSpan("named"));
            // A typed error carrying the key in a field (a scan's resumeFrom).
            yield* Effect.fail(
              new ReaderUnavailableError({
                databaseId: DB,
                reason: "StorageUnavailable",
                message: "A scan was overtaken; resume from resumeFrom.",
                resumeFrom: key,
              }),
            ).pipe(Effect.withSpan("failing"), Effect.ignore);
          }).pipe(Effect.withSpan("request"), Effect.withTracer(tracer)),
        );
        // `sst.id` is a project attribute: its values are ids by construction, which the policy
        // trusts, so that one deliberate line is the only place the marker may appear.
        const outside = exported.filter((line) => !line.startsWith("ATTR sst.id="));
        expect(leaks(outside.join("\n"), marker)).toEqual([]);
        // The log lines themselves still arrive, without their object parts.
        expect(
          outside.some((line) => line.includes(`Writer alarm could not flush ${OMITTED_LOG_PART}`)),
        ).toBe(true);
      }
    }
  });

  it("keeps an event's allowed attributes, and an event with none (mutation sweep, D128)", () => {
    const recorded: Array<[string, Record<string, unknown> | undefined]> = [];
    const inner = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        span.event = (name, _time, attributes) => {
          recorded.push([name, attributes]);
        };
        return span;
      },
    });
    const span = lsmPolicyTracer(inner, { sampleRate: 1 }).span({
      name: "s",
      parent: Option.none(),
      annotations: Context.empty(),
      links: [],
      startTime: 0n,
      kind: "internal",
      root: false,
      sampled: true,
    });
    span.event("plain", 0n, undefined);
    span.event("logged", 0n, {
      "effect.logLevel": "WARN",
      "database.id": "orders",
      key: "MKSECRET",
    });
    expect(recorded).toEqual([
      ["plain", undefined],
      ["logged", { "effect.logLevel": "WARN", "database.id": "orders" }],
    ]);
  });

  it("keeps the inner tracer's context hook, so spans run in the exporter's context (D128)", () => {
    const context: Tracer.Tracer["context"] = (primitive, fiber) =>
      primitive["~effect/Effect/evaluate"](fiber);
    const inner = Tracer.make({ span: (options) => new Tracer.NativeSpan(options), context });
    expect(lsmPolicyTracer(inner, { sampleRate: 1 }).context).toBe(context);
    const bare = Tracer.make({ span: (options) => new Tracer.NativeSpan(options) });
    expect(lsmPolicyTracer(bare, { sampleRate: 1 }).context).toBeUndefined();
  });

  it("keeps a log message's string parts and only those", () => {
    expect(exportedEventName("plain line")).toBe("plain line");
    expect(exportedEventName(JSON.stringify(["text", { key: "MKSECRET" }, "more"]))).toBe(
      `text ${OMITTED_LOG_PART} more`,
    );
    expect(exportedEventName("[not json")).toBe("[not json");
  });
});

describe("no key or value leaves through a public error (D123)", () => {
  it("public error bodies from failing operations on marked keys and values carry neither", () => {
    for (const marker of markers) {
      {
        const key = bytes(marker);
        const failures = [
          // A key outside the database's range.
          collapseWriteBatch(
            { operations: [{ _tag: "Put", key, value: key, ttlMillis: Option.none() }] },
            {
              databaseId: DB,
              keyRange: Option.some({ start: bytes("zzz"), end: Option.none() }),
              mergeOperator: Option.none(),
            },
          ),
          // A key over the limit.
          collapseWriteBatch(
            {
              operations: [
                {
                  _tag: "Put",
                  key: new Uint8Array(MAX_KEY_BYTES + 1).fill(key[0] ?? 0),
                  value: key,
                  ttlMillis: Option.none(),
                },
              ],
            },
            { databaseId: DB, keyRange: Option.none(), mergeOperator: Option.none() },
          ),
          // A merge whose operands resolve past the bound.
          resolveVisibleRow(
            keyVersionsOf(
              [0, 1].map((index) => ({
                key,
                seq: SeqNum.make(index + 1),
                value: RowValue.Merge({ operand: new Uint8Array(600 * 1024).fill(key[0] ?? 0) }),
                createTs: Option.none(),
                expireTs: Option.none(),
              })),
            ),
            {
              snapshotSeq: SeqNum.make(10),
              now: Timestamp.make(0),
              mergeOperator: Option.some(MERGE_OPERATORS.Concat),
            },
          ),
        ];
        const errors = failures.map((failure: Result.Result<unknown, DbInternalError>) =>
          Result.isFailure(failure) ? failure.failure : undefined,
        );
        expect(errors.every((error) => error !== undefined)).toBe(true);
        for (const error of errors) {
          if (error === undefined) continue;
          const body = JSON.stringify(toPublicError("write", "req")(error));
          expect(leaks(body, marker)).toEqual([]);
        }
      }
    }
  });

  it("a malformed bytes field's schema issue does not quote the input it refused", () => {
    // The request middleware repeats the issue to the caller (D69); it must not echo a key.
    const marker = "MK!!NOTBASE64KEY";
    for (const [schema, input] of [
      [GetRequest, { key: marker, consistency: { _tag: "Latest" }, snapshot: null }],
      [
        ScanRequest,
        {
          range: { start: marker, end: null },
          consistency: { _tag: "Latest" },
          snapshot: null,
          limit: 1,
        },
      ],
    ] as const) {
      const exit = Schema.decodeUnknownExit(schema)(input);
      expect(exit._tag).toBe("Failure");
      expect(String(exit._tag === "Failure" ? exit.cause : "")).not.toContain(marker);
    }
  });
});
