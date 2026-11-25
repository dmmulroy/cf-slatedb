import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option } from "effect";

import { LsmBucket } from "./lsm-bucket.ts";
import { type ByteRange, ObjectStore, ObjectStoreUnavailableError } from "./object-store.ts";

/** Read-only object storage, for Workers bound to the bucket read-only (`BlockServer`). */
export interface IObjectReader {
  /** Reads a whole object; none when it does not exist. */
  readonly get: (
    key: string,
  ) => Effect.Effect<Option.Option<Uint8Array>, ObjectStoreUnavailableError, RuntimeContext>;
  /** Reads a byte range of an object (one ranged GET); none when it does not exist. */
  readonly getRange: (
    key: string,
    range: ByteRange,
  ) => Effect.Effect<Option.Option<Uint8Array>, ObjectStoreUnavailableError, RuntimeContext>;
}

/** The contextual read-only object reader. */
export class ObjectReader extends Context.Service<ObjectReader, IObjectReader>()(
  "@cf-slatedb/ObjectReader",
) {}

const unavailable =
  (operation: "get" | "getRange", key: string) => (error: { readonly message: string }) =>
    new ObjectStoreUnavailableError({
      operation,
      key,
      message: `Object storage ${operation} of ${key} failed: ${error.message}. Retry the read.`,
    });

/** Constructs the reader over the R2 bucket bound read-only to the Worker being built. */
export const makeObjectReader = Effect.gen(function* () {
  const bucket = yield* Cloudflare.R2.ReadBucket(yield* LsmBucket);
  return ObjectReader.of({
    get: Effect.fn("ObjectReader.get")(function* (key: string) {
      const object = yield* bucket.get(key).pipe(Effect.mapError(unavailable("get", key)));
      if (object === null) return Option.none<Uint8Array>();
      return Option.some(yield* object.bytes().pipe(Effect.mapError(unavailable("get", key))));
    }),
    getRange: Effect.fn("ObjectReader.getRange")(function* (key: string, range: ByteRange) {
      yield* Effect.annotateCurrentSpan({
        "object.offset": range.offset,
        "object.length": range.length,
      });
      const object = yield* bucket
        .get(key, { range: { offset: range.offset, length: range.length } })
        .pipe(Effect.mapError(unavailable("getRange", key)));
      if (object === null) return Option.none<Uint8Array>();
      return Option.some(yield* object.bytes().pipe(Effect.mapError(unavailable("getRange", key))));
    }),
  });
});

/** Provides the reader, binding the LSM bucket read-only to the Worker being built. */
export const objectReaderLayerWithoutDependencies = Layer.effect(
  ObjectReader,
  makeObjectReader,
).pipe(Layer.provide(Cloudflare.R2.ReadBucketBinding));

/** Provides the read-only R2 reader. */
export const objectReaderLayer = objectReaderLayerWithoutDependencies;

/** Provides the reader as the read half of the contextual (in-memory, in tests) object store. */
export const objectReaderFromObjectStoreLayer = Layer.effect(
  ObjectReader,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    return ObjectReader.of({ get: objects.get, getRange: objects.getRange });
  }),
);
