import { Timestamp } from "@cf-slatedb/lsm-core";
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Bytes, Clock, Context, Data, Effect, Layer, Option, Ref, Schedule, Stream } from "effect";

import { LsmBucketClient, lsmBucketClientLayer } from "./lsm-bucket.ts";

/** Metadata of one stored object. */
export interface ObjectInfo {
  readonly key: string;
  readonly sizeBytes: number;
  readonly uploadedAt: Timestamp;
}

/** A byte range of an object. */
export interface ByteRange {
  readonly offset: number;
  readonly length: number;
}

/** One page of a prefix listing. */
export interface ObjectListing {
  readonly objects: ReadonlyArray<ObjectInfo>;
  /** Cursor for the next page, or none on the last page. */
  readonly cursor: Option.Option<string>;
}

/** Whether a create-if-absent write created the object or found it already there. */
export type PutIfAbsentOutcome = "Created" | "AlreadyExists";

/**
 * The object store could not complete an operation after retries.
 *
 * **Details**
 *
 * Retryable: R2 was unavailable or failed transiently. For writes, the object may or may not have
 * been written; callers make writes idempotent (ULID names, create-if-absent manifests) so retrying
 * the same logical operation is safe.
 */
export class ObjectStoreUnavailableError extends Data.TaggedError("ObjectStoreUnavailableError")<{
  readonly operation:
    | "get"
    | "getRange"
    | "head"
    | "putIfAbsent"
    | "put"
    | "putMultipart"
    | "list"
    | "delete";
  readonly key: string;
  readonly message: string;
}> {}

/** Object storage capability over the shared bucket (R2 in Workers, in memory in tests). */
export interface IObjectStore {
  /**
   * Reads a whole object.
   *
   * @returns The bytes, none when the object does not exist, or `ObjectStoreUnavailableError`.
   */
  readonly get: (
    key: string,
  ) => Effect.Effect<Option.Option<Uint8Array>, ObjectStoreUnavailableError, RuntimeContext>;
  /**
   * Reads a byte range of an object (one ranged GET).
   *
   * @returns The bytes (shorter when the range passes the end), none when the object does not
   *   exist, or `ObjectStoreUnavailableError`.
   */
  readonly getRange: (
    key: string,
    range: ByteRange,
  ) => Effect.Effect<Option.Option<Uint8Array>, ObjectStoreUnavailableError, RuntimeContext>;
  /** Reads an object's metadata; none when it does not exist. */
  readonly head: (
    key: string,
  ) => Effect.Effect<Option.Option<ObjectInfo>, ObjectStoreUnavailableError, RuntimeContext>;
  /**
   * Creates an object only if no object has the key (R2 `If-None-Match: *`).
   *
   * @returns `Created`, or `AlreadyExists` without changing the stored object.
   */
  readonly putIfAbsent: (
    key: string,
    body: Uint8Array,
  ) => Effect.Effect<PutIfAbsentOutcome, ObjectStoreUnavailableError, RuntimeContext>;
  /** Writes an object, replacing any existing one. */
  readonly put: (
    key: string,
    body: Uint8Array,
  ) => Effect.Effect<void, ObjectStoreUnavailableError, RuntimeContext>;
  /**
   * Streams an object out as a multipart upload with uniform `partSize` parts (the last may be
   * shorter), or as one PUT when it fits in one part. An interrupted or failed upload is aborted.
   *
   * @returns The object's size in bytes.
   */
  readonly putMultipart: <E, R>(
    key: string,
    body: Stream.Stream<Uint8Array, E, R>,
    partSize: number,
  ) => Effect.Effect<number, ObjectStoreUnavailableError | E, RuntimeContext | R>;
  /**
   * Lists objects under a prefix, at most 1000 per page, in key order. Never used on read paths.
   */
  readonly list: (
    prefix: string,
    cursor: Option.Option<string>,
  ) => Effect.Effect<ObjectListing, ObjectStoreUnavailableError, RuntimeContext>;
  /** Deletes objects; deleting a missing key succeeds. */
  readonly delete: (
    keys: ReadonlyArray<string>,
  ) => Effect.Effect<void, ObjectStoreUnavailableError, RuntimeContext>;
}

/** The contextual object store. */
export class ObjectStore extends Context.Service<ObjectStore, IObjectStore>()(
  "@cf-slatedb/ObjectStore",
) {}

/** Retry for transient R2 failures: three retries, exponential from 50 ms with jitter. */
const r2RetrySchedule = {
  schedule: Schedule.exponential("50 millis").pipe(Schedule.jittered),
  times: 3,
};

/**
 * Re-chunks a byte stream into parts of exactly `partSize` bytes, except a shorter final part.
 */
const toUniformParts = <E, R>(body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
  body.pipe(
    Stream.mapAccum(
      (): { pending: Array<Uint8Array>; size: number } => ({ pending: [], size: 0 }),
      (state, chunk) => {
        const pending = [...state.pending, chunk];
        let buffered = Bytes.concat(pending);
        const parts: Array<Uint8Array> = [];
        while (buffered.length >= partSize) {
          parts.push(buffered.slice(0, partSize));
          buffered = buffered.slice(partSize);
        }
        return [
          { pending: buffered.length > 0 ? [buffered] : [], size: buffered.length },
          parts,
        ] as const;
      },
      { onHalt: (state) => (state.size > 0 ? [Bytes.concat(state.pending)] : []) },
    ),
  );

/**
 * Constructs the object store over the contextual R2 bucket client; it owns `R2Error` translation
 * and retries.
 */
export const makeObjectStore = Effect.gen(function* () {
  const bucket = yield* LsmBucketClient;

  const unavailable =
    (operation: ObjectStoreUnavailableError["operation"], key: string) =>
    (error: { readonly message: string }) =>
      new ObjectStoreUnavailableError({
        operation,
        key,
        message: `Object storage ${operation} of ${key} failed after retries: ${error.message}. Retry the operation; it is safe to repeat.`,
      });

  const info = (object: {
    readonly key: string;
    readonly size: number;
    readonly uploaded: Date;
  }): ObjectInfo => ({
    key: object.key,
    sizeBytes: object.size,
    uploadedAt: Timestamp.make(object.uploaded.getTime()),
  });

  return ObjectStore.of({
    get: Effect.fn("ObjectStore.get")(function* (key: string) {
      const object = yield* bucket
        .get(key)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("get", key)));
      if (object === null) return Option.none<Uint8Array>();
      return Option.some(yield* object.bytes().pipe(Effect.mapError(unavailable("get", key))));
    }),
    getRange: Effect.fn("ObjectStore.getRange")(function* (key: string, range: ByteRange) {
      yield* Effect.annotateCurrentSpan({
        "object.offset": range.offset,
        "object.length": range.length,
      });
      const object = yield* bucket
        .get(key, { range: { offset: range.offset, length: range.length } })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("getRange", key)));
      if (object === null) return Option.none<Uint8Array>();
      return Option.some(yield* object.bytes().pipe(Effect.mapError(unavailable("getRange", key))));
    }),
    head: Effect.fn("ObjectStore.head")(function* (key: string) {
      const object = yield* bucket
        .head(key)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("head", key)));
      return object === null ? Option.none() : Option.some(info(object));
    }),
    putIfAbsent: Effect.fn("ObjectStore.putIfAbsent")(function* (key: string, body: Uint8Array) {
      // The Headers form, not the `R2Conditional` object: the object form once treated `*` as a
      // literal etag (workerd #2572).
      const written = yield* bucket
        .put(key, body, { onlyIf: new Headers({ "If-None-Match": "*" }) })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("putIfAbsent", key)));
      const outcome: PutIfAbsentOutcome = written === null ? "AlreadyExists" : "Created";
      yield* Effect.annotateCurrentSpan({ "object.outcome": outcome, "object.size": body.length });
      return outcome;
    }),
    put: Effect.fn("ObjectStore.put")(function* (key: string, body: Uint8Array) {
      yield* Effect.annotateCurrentSpan({ "object.size": body.length });
      yield* bucket
        .put(key, body)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("put", key)));
    }),
    putMultipart: <E, R>(key: string, body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
      Effect.gen(function* () {
        let upload = Option.none<Cloudflare.R2.MultipartUpload>();
        let pendingFirstPart = Option.none<Uint8Array>();
        const uploaded: Array<Cloudflare.R2.UploadedPart> = [];
        let sizeBytes = 0;
        const uploadPart = (multipart: Cloudflare.R2.MultipartUpload, part: Uint8Array) =>
          multipart.uploadPart(uploaded.length + 1, part).pipe(
            Effect.retry(r2RetrySchedule),
            Effect.mapError(unavailable("putMultipart", key)),
            Effect.map((result) => {
              uploaded.push(result);
            }),
          );
        const write = Effect.gen(function* () {
          yield* Stream.runForEach(toUniformParts(body, partSize), (part) =>
            Effect.gen(function* () {
              sizeBytes += part.length;
              if (Option.isSome(upload)) return yield* uploadPart(upload.value, part);
              if (Option.isNone(pendingFirstPart)) {
                pendingFirstPart = Option.some(part);
                return;
              }
              const multipart = yield* bucket
                .createMultipartUpload(key)
                .pipe(
                  Effect.retry(r2RetrySchedule),
                  Effect.mapError(unavailable("putMultipart", key)),
                );
              upload = Option.some(multipart);
              yield* uploadPart(multipart, pendingFirstPart.value);
              yield* uploadPart(multipart, part);
            }),
          );
          if (Option.isNone(upload)) {
            // Fits in one part: one PUT, no multipart upload.
            yield* bucket
              .put(
                key,
                Option.getOrElse(pendingFirstPart, () => new Uint8Array(0)),
              )
              .pipe(
                Effect.retry(r2RetrySchedule),
                Effect.mapError(unavailable("putMultipart", key)),
              );
          } else {
            yield* upload.value
              .complete(uploaded)
              .pipe(
                Effect.retry(r2RetrySchedule),
                Effect.mapError(unavailable("putMultipart", key)),
              );
          }
          yield* Effect.annotateCurrentSpan({
            "object.size": sizeBytes,
            "object.parts": Math.max(1, uploaded.length),
          });
          return sizeBytes;
        });
        return yield* write.pipe(
          Effect.onExit((exit) =>
            exit._tag === "Failure" && Option.isSome(upload)
              ? Effect.ignore(upload.value.abort())
              : Effect.void,
          ),
        );
      }).pipe(Effect.withSpan("ObjectStore.putMultipart")),
    list: Effect.fn("ObjectStore.list")(function* (prefix: string, cursor: Option.Option<string>) {
      const page = yield* bucket
        .list({ prefix, limit: 1000, ...(Option.isSome(cursor) ? { cursor: cursor.value } : {}) })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("list", prefix)));
      return {
        objects: page.objects.map(info),
        cursor: page.truncated ? Option.some(page.cursor) : Option.none(),
      };
    }),
    delete: Effect.fn("ObjectStore.delete")(function* (keys: ReadonlyArray<string>) {
      for (let start = 0; start < keys.length; start += 1000) {
        const batch = keys.slice(start, start + 1000);
        yield* bucket
          .delete(batch)
          .pipe(
            Effect.retry(r2RetrySchedule),
            Effect.mapError(unavailable("delete", batch[0] ?? "")),
          );
      }
    }),
  });
});

/** Provides the object store, leaving the R2 bucket client requirement visible. */
export const objectStoreLayerWithoutDependencies = Layer.effect(ObjectStore, makeObjectStore);

/** Provides the object store over {@link LsmBucket}, bound read-write to the Worker being built. */
export const objectStoreLayer = objectStoreLayerWithoutDependencies.pipe(
  Layer.provide(lsmBucketClientLayer),
);

/** R2's smallest multipart part, except the last: 5 MiB. */
const R2_MIN_PART_SIZE = 5 * 1024 * 1024;

/** One object held by the in-memory store. */
interface StoredObject {
  readonly bytes: Uint8Array;
  readonly uploadedAt: Timestamp;
}

/**
 * Builds an in-memory object store with R2's semantics: create-if-absent is atomic, ranged reads
 * clamp at the end, listings are key-ordered pages of at most 1000 with a cursor, deletes are
 * idempotent, upload times come from the contextual `Clock`, and multipart uploads must use uniform
 * parts of at least 5 MiB (except the last). Stored bytes are copied in and out.
 */
const inMemoryObjectStore = Effect.gen(function* () {
  const objects = yield* Ref.make(new Map<string, StoredObject>());
  const store = (key: string, bytes: Uint8Array) =>
    Effect.gen(function* () {
      const uploadedAt = Timestamp.make(yield* Clock.currentTimeMillis);
      yield* Ref.update(objects, (current) =>
        new Map(current).set(key, { bytes: bytes.slice(), uploadedAt }),
      );
    });
  const read = (key: string) =>
    Ref.get(objects).pipe(Effect.map((current) => Option.fromUndefinedOr(current.get(key))));

  return ObjectStore.of({
    get: Effect.fn("ObjectStore.get")(function* (key: string) {
      return Option.map(yield* read(key), (object) => object.bytes.slice());
    }),
    getRange: Effect.fn("ObjectStore.getRange")(function* (key: string, range: ByteRange) {
      yield* Effect.annotateCurrentSpan({
        "object.offset": range.offset,
        "object.length": range.length,
      });
      return Option.map(yield* read(key), (object) =>
        object.bytes.slice(range.offset, range.offset + range.length),
      );
    }),
    head: Effect.fn("ObjectStore.head")(function* (key: string) {
      return Option.map(yield* read(key), (object) => ({
        key,
        sizeBytes: object.bytes.length,
        uploadedAt: object.uploadedAt,
      }));
    }),
    putIfAbsent: Effect.fn("ObjectStore.putIfAbsent")(function* (key: string, body: Uint8Array) {
      const uploadedAt = Timestamp.make(yield* Clock.currentTimeMillis);
      const outcome = yield* Ref.modify(
        objects,
        (current): readonly [PutIfAbsentOutcome, Map<string, StoredObject>] =>
          current.has(key)
            ? ["AlreadyExists", current]
            : ["Created", new Map(current).set(key, { bytes: body.slice(), uploadedAt })],
      );
      yield* Effect.annotateCurrentSpan({ "object.outcome": outcome, "object.size": body.length });
      return outcome;
    }),
    put: Effect.fn("ObjectStore.put")(function* (key: string, body: Uint8Array) {
      yield* Effect.annotateCurrentSpan({ "object.size": body.length });
      yield* store(key, body);
    }),
    putMultipart: <E, R>(key: string, body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
      Effect.gen(function* () {
        const parts = yield* Stream.runCollect(toUniformParts(body, partSize));
        if (parts.length > 1 && partSize < R2_MIN_PART_SIZE) {
          return yield* Effect.die(
            new Error(
              `Multipart part size ${partSize} is below R2's 5 MiB minimum for non-final parts.`,
            ),
          );
        }
        const bytes = Bytes.concat(parts);
        yield* store(key, bytes);
        yield* Effect.annotateCurrentSpan({
          "object.size": bytes.length,
          "object.parts": Math.max(1, parts.length),
        });
        return bytes.length;
      }).pipe(Effect.withSpan("ObjectStore.putMultipart")),
    list: Effect.fn("ObjectStore.list")(function* (prefix: string, cursor: Option.Option<string>) {
      const current = yield* Ref.get(objects);
      const keys = [...current.keys()]
        .filter((key) => key.startsWith(prefix) && (Option.isNone(cursor) || key > cursor.value))
        .sort();
      const page = keys.slice(0, 1000);
      return {
        objects: page.flatMap((key) => {
          const object = current.get(key);
          return object === undefined
            ? []
            : [{ key, sizeBytes: object.bytes.length, uploadedAt: object.uploadedAt }];
        }),
        cursor: keys.length > 1000 ? Option.fromUndefinedOr(page[page.length - 1]) : Option.none(),
      };
    }),
    delete: Effect.fn("ObjectStore.delete")(function* (keys: ReadonlyArray<string>) {
      yield* Ref.update(objects, (current) => {
        const next = new Map(current);
        for (const key of keys) next.delete(key);
        return next;
      });
    }),
  });
});

/** Provides a fresh, faithful in-memory object store (see `inMemoryObjectStore`). */
export const objectStoreTestLayer = Layer.effect(ObjectStore, inMemoryObjectStore);
