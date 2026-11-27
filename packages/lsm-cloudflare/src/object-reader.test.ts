import * as Cloudflare from "@alchemy.run/cloudflare";
import { Effect, Stream, Tracer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { makeObjectReaderOver } from "./object-reader.ts";
import { R2_READ_RETRIES } from "./object-store.ts";

/** The `BlockServer`'s read-only reader (D134): the read the 130-minute soak saw fail once. */
describe("ObjectReader retries R2's transient failures (D134)", () => {
  const TRANSIENT = "get: We encountered an internal error. Please try again. (10001)";
  const object: Cloudflare.R2.R2Object = {
    key: "db/orders/compacted/01M8DESA16MVS0WC7JWRMP9GX6.sst",
    version: "v1",
    size: 3,
    etag: "v1",
    httpEtag: '"v1"',
    checksums: { toJSON: () => ({}) },
    uploaded: new Date(0),
    storageClass: "Standard",
    writeHttpMetadata: () => Effect.void,
  };
  const body: Cloudflare.R2.ObjectBody = {
    ...object,
    body: Stream.make(Uint8Array.of(7, 8, 9)),
    bodyUsed: false,
    arrayBuffer: () => Effect.succeed(Uint8Array.of(7, 8, 9).buffer),
    bytes: () => Effect.succeed(Uint8Array.of(7, 8, 9)),
    text: () => Effect.succeed(""),
    json: () => Effect.die(new Error("not used by this test")),
    blob: () => Effect.die(new Error("not used by this test")),
  };
  const read = (failures: number, kind: "get" | "getRange") => {
    let calls = 0;
    const reader = makeObjectReaderOver({
      get: () =>
        Effect.suspend(() =>
          calls++ < failures
            ? Effect.fail(
                new Cloudflare.R2.R2Error({ message: TRANSIENT, cause: new Error(TRANSIENT) }),
              )
            : Effect.succeed(body),
        ),
    });
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const effect =
      kind === "get"
        ? reader.get(object.key)
        : reader.getRange(object.key, { offset: 0, length: 3 });
    return Effect.result(effect).pipe(
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.withTracer(tracer),
      Effect.runPromise,
      (promise) =>
        promise.then((result) => {
          const span = spans.find((candidate) => candidate.name.startsWith("ObjectReader."));
          return {
            result: result._tag,
            calls,
            retried: span?.attributes.get("object.read_retried"),
            spanOk: span?.status._tag === "Ended" && span.status.exit._tag === "Success",
          };
        }),
    );
  };

  for (const kind of ["get", "getRange"] as const) {
    it(`${kind}: one transient failure is retried; the span ends ok, marked`, async () => {
      expect(await read(1, kind)).toEqual({
        result: "Success",
        calls: 2,
        retried: 1,
        spanOk: true,
      });
    });
    it(`${kind}: a transient failure on every attempt still fails, typed`, async () => {
      expect(await read(Number.POSITIVE_INFINITY, kind)).toEqual({
        result: "Failure",
        calls: R2_READ_RETRIES + 1,
        retried: R2_READ_RETRIES,
        spanOk: false,
      });
    });
  }
});
