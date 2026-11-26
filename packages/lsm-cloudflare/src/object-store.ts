import { md5Hex, Timestamp } from "@shaledb/lsm-core";
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
  Bytes,
  Clock,
  Context,
  Data,
  Effect,
  Layer,
  Metric,
  Option,
  Ref,
  Schedule,
  Stream,
} from "effect";

import { LsmBucketClient, lsmBucketClientLayer } from "./lsm-bucket.ts";
import { objectKindOfKey } from "./object-keys.ts";

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

/** An object's bytes with its version (R2's HTTP etag), for conditional replacement. */
export interface VersionedObject {
  readonly bytes: Uint8Array;
  readonly version: string;
}

/** Whether a conditional replacement wrote the object or found another version stored. */
export type PutIfVersionOutcome = "Written" | "VersionChanged";

/** One page of a delimited listing: the next-level prefixes under a prefix. */
export interface PrefixListing {
  readonly prefixes: ReadonlyArray<string>;
  /** The last prefix returned when more follow (pass it as `after` for the next page), or none. */
  readonly cursor: Option.Option<string>;
}

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
    | "putIfVersion"
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
  /** Reads a whole object with its version; none when it does not exist. */
  readonly getVersioned: (
    key: string,
  ) => Effect.Effect<Option.Option<VersionedObject>, ObjectStoreUnavailableError, RuntimeContext>;
  /**
   * Replaces an object only if its stored version is still `version` (R2 `If-Match`).
   *
   * @returns `Written`, or `VersionChanged` without writing when another write came first.
   */
  readonly putIfVersion: (
    key: string,
    body: Uint8Array,
    version: string,
  ) => Effect.Effect<PutIfVersionOutcome, ObjectStoreUnavailableError, RuntimeContext>;
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
  /**
   * Up to `limit` (at most 1000) delimited prefixes directly under `prefix` (`db/<id>/`), in key
   * order, strictly after `after` (a prefix an earlier page returned).
   */
  readonly listPrefixes: (
    prefix: string,
    after: Option.Option<string>,
    limit: number,
  ) => Effect.Effect<PrefixListing, ObjectStoreUnavailableError, RuntimeContext>;
  /** Deletes objects; deleting a missing key succeeds. */
  readonly delete: (
    keys: ReadonlyArray<string>,
  ) => Effect.Effect<void, ObjectStoreUnavailableError, RuntimeContext>;
}

/** The contextual object store. */
export class ObjectStore extends Context.Service<ObjectStore, IObjectStore>()(
  "@shaledb/ObjectStore",
) {}

/** Retry for transient R2 failures: three retries, exponential from 50 ms with jitter. */
const r2RetrySchedule = {
  schedule: Schedule.exponential("50 millis").pipe(Schedule.jittered),
  times: 3,
};

/**
 * Re-chunks a byte stream into parts of exactly `partSize` bytes, except a shorter final part. Each
 * byte is copied once.
 */
export const toUniformParts = <E, R>(body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
  body.pipe(
    Stream.mapAccum(
      (): { pending: Array<Uint8Array>; size: number } => ({ pending: [], size: 0 }),
      (state, chunk) => {
        // Chunks are only collected until a part is full, so each byte is copied once into its part.
        state.pending.push(chunk);
        state.size += chunk.length;
        if (state.size < partSize) return [state, []] as const;
        const buffered = Bytes.concat(state.pending);
        const parts: Array<Uint8Array> = [];
        let offset = 0;
        for (; buffered.length - offset >= partSize; offset += partSize) {
          parts.push(buffered.subarray(offset, offset + partSize));
        }
        const rest = buffered.subarray(offset);
        return [{ pending: rest.length > 0 ? [rest] : [], size: rest.length }, parts] as const;
      },
      { onHalt: (state) => (state.size > 0 ? [Bytes.concat(state.pending)] : []) },
    ),
  );

/** Attempts to store one multipart part intact before the upload fails (D62). */
export const PART_INTEGRITY_ATTEMPTS = 3;

/** Multipart parts R2 stored with a different checksum than was sent, then uploaded again. */
export const objectPartIntegrityRetries = Metric.counter("lsm_object_part_integrity_retries", {
  description: "Multipart parts whose stored MD5 differed from the part sent, uploaded again",
});

/**
 * Multipart parts stored without a check, because the store's ETag was not an MD5 (local R2 returns
 * opaque ETags; real R2 returns each part's MD5).
 */
export const objectPartsUnverified = Metric.counter("lsm_object_parts_unverified", {
  description: "Multipart parts whose ETag was not an MD5, so their integrity was not checked",
});

/** The hex digest in an ETag: R2 quotes it, and a part's ETag is its MD5. */
const etagDigest = (etag: string) => etag.replaceAll('"', "").toLowerCase();

/** Lowercase hex of a digest. */
const toHex = (digest: ArrayBuffer) =>
  Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");

/**
 * The MD5 of a part as lowercase hex: the runtime's native digest where WebCrypto offers MD5 (the
 * Workers runtime does, as a non-standard algorithm), else the portable `md5Hex` (Node, in tests).
 * The JavaScript digest cost several seconds of CPU per 64 MiB SST in workerd (D62).
 */
const partMd5 = (part: Uint8Array) =>
  Effect.tryPromise(() => crypto.subtle.digest("MD5", part)).pipe(
    Effect.map(toHex),
    Effect.orElseSucceed(() => md5Hex(part)),
  );

/** Whether an ETag digest has an MD5's shape (32 hex digits). */
const isMd5Digest = (digest: string) => /^[0-9a-f]{32}$/.test(digest);

/**
 * Uploads one multipart part and checks that what R2 stored is what was sent (D62).
 *
 * **Details**
 *
 * R2 returns a part's MD5 as its ETag; `uploadPart` cannot send a checksum, so the check is after
 * the fact: a part whose ETag differs from the MD5 of `part` changed in transit, and is uploaded
 * again under the same part number (which replaces it), up to {@link PART_INTEGRITY_ATTEMPTS}
 * times. A mismatch every time fails the upload, which is then aborted; nothing references it.
 *
 * An ETag that is not an MD5 at all (local R2's are opaque) cannot be checked: the part is accepted
 * as `verified: false` and counted in `lsm_object_parts_unverified`, so a store that stops
 * reporting digests shows up rather than silently passing.
 *
 * @param upload - Uploads `part` as `partNumber` (with R2's own transient-failure retries).
 * @returns The uploaded part to complete the upload with and whether it was checked, or
 *   `ObjectStoreUnavailableError`.
 */
export const uploadPartVerified = <E extends { readonly message: string }, R>(
  key: string,
  partNumber: number,
  part: Uint8Array,
  upload: (
    partNumber: number,
    part: Uint8Array,
  ) => Effect.Effect<{ readonly partNumber: number; readonly etag: string }, E, R>,
) =>
  Effect.gen(function* () {
    const expected = yield* partMd5(part);
    let stored = "";
    for (let attempt = 1; attempt <= PART_INTEGRITY_ATTEMPTS; attempt++) {
      const uploaded = yield* upload(partNumber, part).pipe(
        Effect.mapError(
          (error) =>
            new ObjectStoreUnavailableError({
              operation: "putMultipart",
              key,
              message: `Object storage putMultipart of ${key} failed after retries: ${error.message}. Retry the operation; it is safe to repeat.`,
            }),
        ),
      );
      stored = etagDigest(uploaded.etag);
      if (stored === expected) return { uploaded, verified: true };
      if (!isMd5Digest(stored)) {
        yield* Metric.update(objectPartsUnverified, 1);
        return { uploaded, verified: false };
      }
      yield* Metric.update(objectPartIntegrityRetries, 1);
      yield* Effect.annotateCurrentSpan({ "object.part_retried": true });
      yield* Effect.logWarning(
        `Part ${partNumber} of ${key} was stored with MD5 ${stored}, not the ${expected} sent; uploading it again.`,
      );
    }
    return yield* new ObjectStoreUnavailableError({
      operation: "putMultipart",
      key,
      message: `Object storage putMultipart of ${key} stored part ${partNumber} with MD5 ${stored} instead of ${expected} on ${PART_INTEGRITY_ATTEMPTS} attempts, so the upload was aborted. Retry the operation; it is safe to repeat.`,
    });
  });

/**
 * The SHA-256 of `body`, sent with single-part puts so R2 refuses a body that changed in transit
 * (D62).
 */
const sha256Of = (body: Uint8Array) => Effect.promise(() => crypto.subtle.digest("SHA-256", body));

/**
 * Constructs the object store over the contextual R2 bucket client; it owns `R2Error` translation
 * and retries. Every write carries an integrity check (D62): single-part puts send their SHA-256
 * (R2 refuses a mismatch), and multipart parts are checked against the MD5 R2 returns.
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
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      const object = yield* bucket
        .get(key)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("get", key)));
      if (object === null) return Option.none<Uint8Array>();
      return Option.some(yield* object.bytes().pipe(Effect.mapError(unavailable("get", key))));
    }),
    getRange: Effect.fn("ObjectStore.getRange")(function* (key: string, range: ByteRange) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
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
    getVersioned: Effect.fn("ObjectStore.getVersioned")(function* (key: string) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      const object = yield* bucket
        .get(key)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("get", key)));
      if (object === null) return Option.none<VersionedObject>();
      const bytes = yield* object.bytes().pipe(Effect.mapError(unavailable("get", key)));
      return Option.some({ bytes, version: object.httpEtag });
    }),
    putIfVersion: Effect.fn("ObjectStore.putIfVersion")(function* (
      key: string,
      body: Uint8Array,
      version: string,
    ) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      const sha256 = yield* sha256Of(body);
      const written = yield* bucket
        .put(key, body, { onlyIf: new Headers({ "If-Match": version }), sha256 })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("putIfVersion", key)));
      const outcome: PutIfVersionOutcome = written === null ? "VersionChanged" : "Written";
      yield* Effect.annotateCurrentSpan({ "object.outcome": outcome });
      return outcome;
    }),
    head: Effect.fn("ObjectStore.head")(function* (key: string) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      const object = yield* bucket
        .head(key)
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("head", key)));
      return object === null ? Option.none() : Option.some(info(object));
    }),
    putIfAbsent: Effect.fn("ObjectStore.putIfAbsent")(function* (key: string, body: Uint8Array) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      // The Headers form, not the `R2Conditional` object: the object form once treated `*` as a
      // literal etag (workerd #2572).
      const sha256 = yield* sha256Of(body);
      const written = yield* bucket
        .put(key, body, { onlyIf: new Headers({ "If-None-Match": "*" }), sha256 })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("putIfAbsent", key)));
      const outcome: PutIfAbsentOutcome = written === null ? "AlreadyExists" : "Created";
      yield* Effect.annotateCurrentSpan({ "object.outcome": outcome, "object.size": body.length });
      return outcome;
    }),
    put: Effect.fn("ObjectStore.put")(function* (key: string, body: Uint8Array) {
      yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
      yield* Effect.annotateCurrentSpan({ "object.size": body.length });
      const sha256 = yield* sha256Of(body);
      yield* bucket
        .put(key, body, { sha256 })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("put", key)));
    }),
    putMultipart: <E, R>(key: string, body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
      Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        let upload = Option.none<Cloudflare.R2.MultipartUpload>();
        let pendingFirstPart = Option.none<Uint8Array>();
        const uploaded: Array<Cloudflare.R2.UploadedPart> = [];
        let unverified = 0;
        let sizeBytes = 0;
        const uploadPart = (multipart: Cloudflare.R2.MultipartUpload, part: Uint8Array) =>
          uploadPartVerified(key, uploaded.length + 1, part, (partNumber, bytes) =>
            multipart.uploadPart(partNumber, bytes).pipe(Effect.retry(r2RetrySchedule)),
          ).pipe(
            Effect.map((result) => {
              uploaded.push(result.uploaded);
              if (!result.verified) unverified++;
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
            const single = Option.getOrElse(pendingFirstPart, () => new Uint8Array(0));
            const sha256 = yield* sha256Of(single);
            yield* bucket
              .put(key, single, { sha256 })
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
            "object.parts_unverified": unverified,
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
    listPrefixes: Effect.fn("ObjectStore.listPrefixes")(function* (
      prefix: string,
      after: Option.Option<string>,
      limit: number,
    ) {
      const pageLimit = Math.max(1, Math.min(limit, 1000));
      // Every key under `<p>/` sorts before `<p>0` ("/" is 0x2F, "0" is 0x30), so starting after it
      // skips the whole earlier prefix and nothing that follows it.
      const startAfter = Option.map(after, (previous) => `${previous.slice(0, -1)}0`);
      const page = yield* bucket
        .list({
          prefix,
          delimiter: "/",
          limit: pageLimit,
          ...(Option.isSome(startAfter) ? { startAfter: startAfter.value } : {}),
        })
        .pipe(Effect.retry(r2RetrySchedule), Effect.mapError(unavailable("list", prefix)));
      const prefixes = page.delimitedPrefixes;
      return {
        prefixes,
        cursor: page.truncated
          ? Option.fromUndefinedOr(prefixes[prefixes.length - 1])
          : Option.none(),
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

/** Fault injection for the in-memory store. */
interface InMemoryObjectStoreOptions {
  /** Whether attempt `attempt` of part `partNumber` of `key` arrives changed. */
  readonly corruptPartInTransit: (key: string, partNumber: number, attempt: number) => boolean;
}

/** A copy of `bytes` with its first byte inverted: a part damaged in transit. */
const flipFirstByte = (bytes: Uint8Array) => {
  const copy = bytes.slice();
  if (copy.length > 0) copy[0] = (copy[0] ?? 0) ^ 0xff;
  return copy;
};

/** One object held by the in-memory store. */
interface StoredObject {
  readonly bytes: Uint8Array;
  readonly uploadedAt: Timestamp;
  /** Bumped on every write of the key, like an etag. */
  readonly version: number;
}

/**
 * Builds an in-memory object store with R2's semantics: create-if-absent is atomic, ranged reads
 * clamp at the end, listings are key-ordered pages of at most 1000 with a cursor, deletes are
 * idempotent, upload times come from the contextual `Clock`, and multipart uploads must use uniform
 * parts of at least 5 MiB (except the last). Stored bytes are copied in and out.
 */
const inMemoryObjectStore = (options: InMemoryObjectStoreOptions) =>
  Effect.gen(function* () {
    const objects = yield* Ref.make(new Map<string, StoredObject>());
    let nextVersion = 1;
    const stored = (bytes: Uint8Array, uploadedAt: Timestamp): StoredObject => ({
      bytes: bytes.slice(),
      uploadedAt,
      version: nextVersion++,
    });
    const store = (key: string, bytes: Uint8Array) =>
      Effect.gen(function* () {
        const uploadedAt = Timestamp.make(yield* Clock.currentTimeMillis);
        yield* Ref.update(objects, (current) =>
          new Map(current).set(key, stored(bytes, uploadedAt)),
        );
      });
    const read = (key: string) =>
      Ref.get(objects).pipe(Effect.map((current) => Option.fromUndefinedOr(current.get(key))));

    return ObjectStore.of({
      get: Effect.fn("ObjectStore.get")(function* (key: string) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        return Option.map(yield* read(key), (object) => object.bytes.slice());
      }),
      getRange: Effect.fn("ObjectStore.getRange")(function* (key: string, range: ByteRange) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        yield* Effect.annotateCurrentSpan({
          "object.offset": range.offset,
          "object.length": range.length,
        });
        return Option.map(yield* read(key), (object) =>
          object.bytes.slice(range.offset, range.offset + range.length),
        );
      }),
      getVersioned: Effect.fn("ObjectStore.getVersioned")(function* (key: string) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        return Option.map(yield* read(key), (object) => ({
          bytes: object.bytes.slice(),
          version: `"${object.version}"`,
        }));
      }),
      putIfVersion: Effect.fn("ObjectStore.putIfVersion")(function* (
        key: string,
        body: Uint8Array,
        version: string,
      ) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        const uploadedAt = Timestamp.make(yield* Clock.currentTimeMillis);
        const outcome = yield* Ref.modify(
          objects,
          (current): readonly [PutIfVersionOutcome, Map<string, StoredObject>] => {
            const existing = current.get(key);
            return existing !== undefined && `"${existing.version}"` === version
              ? ["Written", new Map(current).set(key, stored(body, uploadedAt))]
              : ["VersionChanged", current];
          },
        );
        yield* Effect.annotateCurrentSpan({ "object.outcome": outcome });
        return outcome;
      }),
      head: Effect.fn("ObjectStore.head")(function* (key: string) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        return Option.map(yield* read(key), (object) => ({
          key,
          sizeBytes: object.bytes.length,
          uploadedAt: object.uploadedAt,
        }));
      }),
      putIfAbsent: Effect.fn("ObjectStore.putIfAbsent")(function* (key: string, body: Uint8Array) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        const uploadedAt = Timestamp.make(yield* Clock.currentTimeMillis);
        const outcome = yield* Ref.modify(
          objects,
          (current): readonly [PutIfAbsentOutcome, Map<string, StoredObject>] =>
            current.has(key)
              ? ["AlreadyExists", current]
              : ["Created", new Map(current).set(key, stored(body, uploadedAt))],
        );
        yield* Effect.annotateCurrentSpan({
          "object.outcome": outcome,
          "object.size": body.length,
        });
        return outcome;
      }),
      put: Effect.fn("ObjectStore.put")(function* (key: string, body: Uint8Array) {
        yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
        yield* Effect.annotateCurrentSpan({ "object.size": body.length });
        yield* store(key, body);
      }),
      putMultipart: <E, R>(key: string, body: Stream.Stream<Uint8Array, E, R>, partSize: number) =>
        Effect.gen(function* () {
          yield* Effect.annotateCurrentSpan({ "object.kind": objectKindOfKey(key) });
          const sent = yield* Stream.runCollect(toUniformParts(body, partSize));
          if (sent.length > 1 && partSize < R2_MIN_PART_SIZE) {
            return yield* Effect.die(
              new Error(
                `Multipart part size ${partSize} is below R2's 5 MiB minimum for non-final parts.`,
              ),
            );
          }
          // Like R2: each part is stored as it arrived and its ETag is the MD5 of what was stored.
          const storedParts = new Map<number, Uint8Array>();
          const attempts = new Map<number, number>();
          for (const [index, part] of sent.entries()) {
            yield* uploadPartVerified(key, index + 1, part, (partNumber, bytes) =>
              Effect.sync(() => {
                const attempt = (attempts.get(partNumber) ?? 0) + 1;
                attempts.set(partNumber, attempt);
                const arrived = options.corruptPartInTransit(key, partNumber, attempt)
                  ? flipFirstByte(bytes)
                  : bytes.slice();
                storedParts.set(partNumber, arrived);
                return { partNumber, etag: `"${md5Hex(arrived)}"` };
              }),
            );
          }
          const bytes = Bytes.concat(
            sent.map((_, index) => storedParts.get(index + 1) ?? new Uint8Array(0)),
          );
          yield* store(key, bytes);
          yield* Effect.annotateCurrentSpan({
            "object.size": bytes.length,
            "object.parts": Math.max(1, sent.length),
          });
          return bytes.length;
        }).pipe(Effect.withSpan("ObjectStore.putMultipart")),
      list: Effect.fn("ObjectStore.list")(function* (
        prefix: string,
        cursor: Option.Option<string>,
      ) {
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
          cursor:
            keys.length > 1000 ? Option.fromUndefinedOr(page[page.length - 1]) : Option.none(),
        };
      }),
      listPrefixes: Effect.fn("ObjectStore.listPrefixes")(function* (
        prefix: string,
        after: Option.Option<string>,
        limit: number,
      ) {
        const current = yield* Ref.get(objects);
        const prefixes = new Set<string>();
        for (const key of current.keys()) {
          if (!key.startsWith(prefix)) continue;
          const slash = key.indexOf("/", prefix.length);
          if (slash !== -1) prefixes.add(key.slice(0, slash + 1));
        }
        const sorted = [...prefixes]
          .sort()
          .filter((candidate) => Option.isNone(after) || candidate > after.value);
        const pageLimit = Math.max(1, Math.min(limit, 1000));
        const page = sorted.slice(0, pageLimit);
        return {
          prefixes: page,
          cursor:
            sorted.length > pageLimit
              ? Option.fromUndefinedOr(page[page.length - 1])
              : Option.none(),
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
export const objectStoreTestLayer = Layer.effect(
  ObjectStore,
  inMemoryObjectStore({ corruptPartInTransit: () => false }),
);

/**
 * Provides an in-memory object store whose multipart parts arrive changed whenever
 * `corruptPartInTransit(key, partNumber, attempt)` says so, as a part damaged between the Worker
 * and R2 would (attempts count from 1).
 */
export const objectStoreWithTransitCorruptionTestLayer = (
  corruptPartInTransit: InMemoryObjectStoreOptions["corruptPartInTransit"],
) => Layer.effect(ObjectStore, inMemoryObjectStore({ corruptPartInTransit }));
