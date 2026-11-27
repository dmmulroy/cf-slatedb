import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  CheckpointId,
  CompactorEpoch,
  DatabaseId,
  dropSstFromManifest,
  FULL_KEY_RANGE,
  ManifestId,
  replaceManifestFields,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "@shaledb/lsm-core";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option, Ref, Result, type Scope } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import {
  CompactorClient,
  type CompactorClientCalls,
  CompactorClientRecord,
  type ICompactorClient,
} from "./compactor-client.ts";
import { CompactorUnavailableError } from "./compactor.ts";
import { databaseLifecycleLayerWithoutDependencies } from "./database-lifecycle.ts";
import { dbClientTestLayer } from "./db-client.ts";
import { DbHttpApi } from "./db-http-api.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { manifestKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * Every route of the public database API calls its service with the request's fields, reports its
 * failures as its own operation, and pages as its schema says (D130's mutation sweep of
 * `db-http-handlers.ts`). The routes are reached through the real router and a generated client.
 *
 * **Details**
 *
 * The in-process writers (`writerClientTestLayer`) are built on a database's first writer call.
 * A database whose first writer call comes from inside an in-process API request gets a writer
 * router that cannot decode its payloads (D130, open): each database here is warmed by one
 * direct writer call before its first request. Workerd does not have this limit.
 */
const run = <A, E>(
  body: (client: DbApiClient) => Effect.Effect<A, E, TestServices>,
  compactors: Option.Option<ICompactorClient> = Option.none(),
) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
    const runtime = yield* RuntimeContext;
    const client = yield* HttpApiClient.makeWith(DbHttpApi, {
      baseUrl: "http://api.internal",
      httpClient: Cloudflare.toHttpClient({
        fetch: (request: HttpServerRequest.HttpServerRequest) =>
          app.pipe(
            Effect.provideService(HttpServerRequest.HttpServerRequest, request),
            Effect.scoped,
            Effect.provideService(RuntimeContext, runtime),
          ),
      }),
    });
    return yield* body(client);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Option.match(compactors, {
        onNone: () => dbServicesTestLayer(),
        // As `dbServicesTestLayer`, with `stub` as every service's compactor client.
        onSome: (stub) =>
          databaseLifecycleLayerWithoutDependencies.pipe(
            Layer.provide(blockSourceTestLayer),
            Layer.provide(backgroundWorkTestLayer),
            Layer.provideMerge(Layer.succeed(CompactorClient, CompactorClient.of(stub))),
            Layer.provideMerge(dbClientTestLayer()),
            Layer.provideMerge(
              Layer.effect(
                CompactorClientRecord,
                Ref.make<CompactorClientCalls>({
                  destroyed: [],
                  woken: [],
                  collectionsRequested: [],
                }),
              ),
            ),
          ),
      }),
    ),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

type TestServices =
  | Layer.Success<ReturnType<typeof dbServicesTestLayer>>
  | Layer.Success<typeof lsmTestRuntimeContextLayer>
  | Scope.Scope;
type DbApiClient = HttpApiClient.ForApi<typeof DbHttpApi>;

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);

/** Creates `databaseId` through the API and warms its in-process writer (see the module note). */
const created = (
  client: DbApiClient,
  databaseId: DatabaseId,
  options: {
    readonly keyRange?: { readonly start: Uint8Array; readonly end: Option.Option<Uint8Array> };
    readonly mergeOperator?: "Concat";
    readonly budgetGroup?: DatabaseId;
    readonly changelogRetentionMs?: number;
  } = {},
) =>
  Effect.gen(function* () {
    const answer = yield* client.databases.createDatabase({
      payload: {
        databaseId,
        keyRange: Option.fromNullishOr(options.keyRange),
        mergeOperator: Option.fromNullishOr(options.mergeOperator),
        budgetGroup: Option.fromNullishOr(options.budgetGroup),
        changelogRetentionMs: Option.fromNullishOr(options.changelogRetentionMs),
      },
    });
    yield* (yield* WriterClient).status(databaseId);
    return answer;
  });

const put = (client: DbApiClient, databaseId: DatabaseId, key: string, value: string) =>
  client.databases.write({
    params: { databaseId },
    payload: {
      operations: [{ _tag: "Put", key: bytes(key), value: bytes(value), ttlMillis: Option.none() }],
    },
  });

/** The named field of the call's public error (its `_tag` for a client-side failure). */
const failureOf = (
  call: Effect.Effect<unknown, { readonly _tag: string }>,
  field: "operation" | "code",
) =>
  Effect.flip(call).pipe(
    Effect.map((failure) =>
      field in failure ? String(Reflect.get(failure, field)) : failure._tag,
    ),
  );

const latest = { consistency: { _tag: "Latest" as const }, snapshot: Option.none() };

describe("the public database API's routes (D130)", () => {
  it("report every failure as the route's own operation", async () => {
    const answers = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("gone");
        yield* created(client, databaseId);
        yield* client.databases.removeDatabase({ params: { databaseId } });
        const params = { databaseId };
        const checkpointId = CheckpointId.make("01JD0000000000000000000AAA");
        const sstId = SstId.make("01JD0000000000000000000AAA");
        const calls = {
          write: client.databases.write({
            params,
            payload: { operations: [{ _tag: "Delete", key: Uint8Array.of(1) }] },
          }),
          flush: client.databases.flush({ params }),
          status: client.databases.status({ params }),
          compaction: client.databases.compaction({ params }),
          dropSst: client.databases.dropSst({
            params: { databaseId, sstId },
            payload: { confirm: sstId },
          }),
          createCheckpoint: client.databases.createCheckpoint({
            params,
            payload: { name: Option.none(), lifetimeMs: Option.none() },
          }),
          pinSnapshot: client.databases.pinSnapshot({ params, payload: { lifetimeMs: 60_000 } }),
          listCheckpoints: client.databases.listCheckpoints({ params }),
          deleteCheckpoint: client.databases.deleteCheckpoint({
            params: { databaseId, checkpointId },
          }),
          seal: client.databases.seal({ params, payload: { successors: [] } }),
          fork: client.databases.fork({
            params,
            payload: { forkId: DatabaseId.make("gone-fork"), checkpointId, range: FULL_KEY_RANGE },
          }),
          beginTransaction: client.databases.beginTransaction({ params }),
          transactionGet: client.databases.transactionGet({
            params,
            payload: { readSeq: SeqNum.make(0), manifestId: Option.none(), key: Uint8Array.of(1) },
          }),
          transactionScan: client.databases.transactionScan({
            params,
            payload: {
              readSeq: SeqNum.make(0),
              manifestId: Option.none(),
              range: FULL_KEY_RANGE,
              limit: 1,
            },
          }),
          commitTransaction: client.databases.commitTransaction({
            params,
            payload: {
              readSeq: SeqNum.make(0),
              isolation: "Snapshot",
              batch: { operations: [{ _tag: "Delete", key: Uint8Array.of(1) }] },
              readKeys: [],
              readRanges: [],
            },
          }),
          get: client.databases.get({ params, payload: { key: Uint8Array.of(1), ...latest } }),
          scan: client.databases.scan({
            params,
            payload: { range: FULL_KEY_RANGE, ...latest, limit: 1 },
          }),
          snapshot: client.databases.snapshot({ params }),
          changes: client.databases.changes({
            params,
            query: { afterSeq: SeqNum.make(0), limit: 1 },
          }),
          // Never created: a remove's failure is its own too (a removed one may answer success).
          removeDatabase: client.databases.removeDatabase({
            params: { databaseId: DatabaseId.make("never-created") },
          }),
        };
        return yield* Effect.all(
          Object.fromEntries(
            Object.entries(calls).map(([route, call]) => [route, failureOf(call, "operation")]),
          ),
        );
      }),
    );
    expect(answers).toEqual({
      write: "write",
      flush: "flush",
      status: "status",
      compaction: "compaction",
      dropSst: "dropSst",
      createCheckpoint: "checkpoint",
      pinSnapshot: "checkpoint",
      listCheckpoints: "checkpoint",
      deleteCheckpoint: "checkpoint",
      seal: "seal",
      fork: "fork",
      beginTransaction: "transaction",
      transactionGet: "transaction",
      transactionScan: "transaction",
      commitTransaction: "transaction",
      get: "get",
      scan: "scan",
      snapshot: "get",
      changes: "changes",
      removeDatabase: "remove",
    });
  });

  it("answers a duplicate create as createDatabase", async () => {
    const failure = await run((client) =>
      Effect.gen(function* () {
        yield* created(client, DatabaseId.make("twice"));
        const again = client.databases.createDatabase({
          payload: {
            databaseId: DatabaseId.make("twice"),
            keyRange: Option.none(),
            mergeOperator: Option.none(),
            budgetGroup: Option.none(),
            changelogRetentionMs: Option.none(),
          },
        });
        return [yield* failureOf(again, "code"), yield* failureOf(again, "operation")];
      }),
    );
    expect(failure).toEqual(["DatabaseAlreadyExists", "createDatabase"]);
  });

  it("create applies the key range, the merge operator, the changelog, and the budget group", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("shaped");
        const answer = yield* created(client, databaseId, {
          keyRange: { start: bytes("b"), end: Option.some(bytes("m")) },
          mergeOperator: "Concat",
          budgetGroup: DatabaseId.make("tenant-7"),
          changelogRetentionMs: 60_000,
        });
        const outside = yield* failureOf(put(client, databaseId, "z", "1"), "code");
        for (const operand of ["x", "y"]) {
          yield* client.databases.write({
            params: { databaseId },
            payload: {
              operations: [
                {
                  _tag: "Merge",
                  key: bytes("c"),
                  operand: bytes(operand),
                  ttlMillis: Option.none(),
                },
              ],
            },
          });
        }
        const merged = yield* client.databases.get({
          params: { databaseId },
          payload: { key: bytes("c"), ...latest },
        });
        yield* client.databases.flush({ params: { databaseId } });
        const changes = yield* client.databases.changes({
          params: { databaseId },
          query: { afterSeq: SeqNum.make(0), limit: 10 },
        });
        const stored = Option.getOrThrow(
          yield* (yield* ObjectStore).get(
            manifestKey(databaseId, ManifestId.make(answer.manifestId)),
          ),
        );
        return {
          answer,
          outside,
          merged: Option.map(merged.row, (row) =>
            row.value._tag === "Value" ? text(row.value.bytes) : row.value._tag,
          ),
          changes: changes.rows.length,
          manifest: text(stored),
        };
      }),
    );
    expect(observed.answer).toEqual({ databaseId: "shaped", manifestId: 1 });
    expect(observed.outside).toBe("KeyOutOfRange");
    expect(observed.merged).toEqual(Option.some("xy"));
    expect(observed.changes).toBe(2);
    expect(observed.manifest).toContain('"budgetGroup":"tenant-7"');
  });

  it("writes every operation of a batch, and flush and seal answer the flushed sequence", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("batch");
        yield* created(client, databaseId);
        yield* client.databases.write({
          params: { databaseId },
          payload: {
            operations: [
              { _tag: "Put", key: bytes("a"), value: bytes("1"), ttlMillis: Option.none() },
              { _tag: "Put", key: bytes("b"), value: bytes("2"), ttlMillis: Option.none() },
            ],
          },
        });
        const flushed = yield* client.databases.flush({ params: { databaseId } });
        yield* put(client, databaseId, "c", "3");
        const sealed = yield* client.databases.seal({
          params: { databaseId },
          payload: { successors: [DatabaseId.make("next-a"), DatabaseId.make("next-b")] },
        });
        const status = yield* client.databases.status({ params: { databaseId } });
        const refused = yield* Effect.flip(put(client, databaseId, "d", "4"));
        const refusedDetails =
          refused._tag === "DatabaseSealedPublicError" ? refused.details.successors : [];
        // A sealed database stays readable through the database API (`refuseSealed: false`).
        const read = yield* client.databases.get({
          params: { databaseId },
          payload: { key: bytes("a"), ...latest },
        });
        const scanned = yield* client.databases.scan({
          params: { databaseId },
          payload: { range: FULL_KEY_RANGE, ...latest, limit: 10 },
        });
        return {
          flushed: flushed.flushedSeq,
          sealed: sealed.flushedSeq,
          statusFlushed: status.flushedSeq,
          refused: [refused._tag, refusedDetails],
          read: Option.isSome(read.row),
          scanned: scanned.rows.map((row) => text(row.key)),
        };
      }),
    );
    // One batch is one sequence number.
    expect(observed.flushed).toBe(1);
    expect([observed.sealed, observed.statusFlushed]).toEqual([2, 2]);
    expect(observed.refused).toEqual(["DatabaseSealedPublicError", ["next-a", "next-b"]]);
    expect(observed.read).toBe(true);
    expect(observed.scanned).toEqual(["a", "b", "c"]);
  });

  it("pages a scan by its limit, with resumeFrom at the next key and null at the end", async () => {
    const pages = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("paged");
        yield* created(client, databaseId);
        for (const key of ["a", "b", "c", "d", "e"]) yield* put(client, databaseId, key, key);
        const found: Array<{ rows: Array<string>; resumeFrom: Option.Option<string> }> = [];
        let start: Uint8Array = new Uint8Array(0);
        for (let page = 0; page < 5; page++) {
          const answer = yield* client.databases.scan({
            params: { databaseId },
            payload: { range: { start, end: Option.none() }, ...latest, limit: 2 },
          });
          found.push({
            rows: answer.rows.map((row) => text(row.key)),
            resumeFrom: Option.map(answer.resumeFrom, text),
          });
          if (Option.isNone(answer.resumeFrom)) break;
          start = answer.resumeFrom.value;
        }
        return found;
      }),
    );
    expect(pages).toEqual([
      { rows: ["a", "b"], resumeFrom: Option.some("c") },
      { rows: ["c", "d"], resumeFrom: Option.some("e") },
      { rows: ["e"], resumeFrom: Option.none() },
    ]);
  });

  it("reads a transaction at its begin manifest across a flush, by its limit", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("txn");
        yield* created(client, databaseId);
        for (const key of ["a", "b", "c"]) yield* put(client, databaseId, key, "old");
        const begun = yield* client.databases.beginTransaction({ params: { databaseId } });
        // Writes and a flush past readSeq: only the begin manifest still answers readSeq (D58).
        for (const key of ["a", "b", "c"]) yield* put(client, databaseId, key, "new");
        yield* client.databases.flush({ params: { databaseId } });
        const got = yield* client.databases.transactionGet({
          params: { databaseId },
          payload: { readSeq: begun.readSeq, manifestId: begun.manifestId, key: bytes("a") },
        });
        const page = yield* client.databases.transactionScan({
          params: { databaseId },
          payload: {
            readSeq: begun.readSeq,
            manifestId: begun.manifestId,
            range: FULL_KEY_RANGE,
            limit: 2,
          },
        });
        const changes = yield* client.databases
          .changes({
            params: { databaseId },
            query: { afterSeq: SeqNum.make(0), limit: 1 },
          })
          .pipe(Effect.option);
        return {
          manifestId: begun.manifestId,
          got: Option.map(got.row, (row) =>
            row.value._tag === "Value" ? text(row.value.bytes) : "",
          ),
          page: page.rows.map((row) => text(row.key)),
          resumeFrom: Option.map(page.resumeFrom, text),
          changes,
        };
      }),
    );
    expect(Option.isSome(observed.manifestId)).toBe(true);
    expect(observed.got).toEqual(Option.some("old"));
    expect(observed.page).toEqual(["a", "b"]);
    expect(observed.resumeFrom).toEqual(Option.some("c"));
  });

  it("answers the change feed by its limit", async () => {
    const rows = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("feed");
        yield* created(client, databaseId, { changelogRetentionMs: 60_000 });
        for (const key of ["a", "b", "c"]) yield* put(client, databaseId, key, key);
        yield* client.databases.flush({ params: { databaseId } });
        const page = yield* client.databases.changes({
          params: { databaseId },
          query: { afterSeq: SeqNum.make(0), limit: 2 },
        });
        return page.rows.map((row) => text(row.key));
      }),
    );
    expect(rows).toEqual(["a", "b"]);
  });

  it("lists checkpoints oldest first and forks only the requested range", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("source");
        yield* created(client, databaseId);
        yield* put(client, databaseId, "c", "1");
        const first = yield* client.databases.createCheckpoint({
          params: { databaseId },
          payload: { name: Option.none(), lifetimeMs: Option.none() },
        });
        yield* Effect.sleep("5 millis");
        const second = yield* client.databases.createCheckpoint({
          params: { databaseId },
          payload: { name: Option.none(), lifetimeMs: Option.none() },
        });
        const listed = yield* client.databases.listCheckpoints({ params: { databaseId } });
        const forkId = DatabaseId.make("fork");
        const forked = yield* client.databases.fork({
          params: { databaseId },
          payload: {
            forkId,
            checkpointId: second.id,
            range: { start: bytes("b"), end: Option.some(bytes("d")) },
          },
        });
        const fork = yield* (yield* ManifestReader).loadLatest(forkId);
        return {
          order: listed.checkpoints.map((checkpoint) => checkpoint.id),
          expected: [first.id, second.id],
          forked: forked.databaseId,
          range: Option.map(fork.manifest.keyRange, (range) => [
            text(range.start),
            Option.map(range.end, text),
          ]),
        };
      }),
    );
    expect(observed.order).toEqual(observed.expected);
    expect(observed.forked).toBe("fork");
    expect(observed.range).toEqual(Option.some(["b", Option.some("d")]));
  });

  it("reports l0 SSTs by id on the compaction route", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("ssts");
        yield* created(client, databaseId);
        for (const key of ["a", "b"]) {
          yield* put(client, databaseId, key, key);
          yield* client.databases.flush({ params: { databaseId } });
        }
        return yield* client.databases.compaction({ params: { databaseId } });
      }),
    );
    expect(observed.l0SstIds).toHaveLength(2);
    expect(observed.l0Ssts).toBe(2);
  });

  it("answers status and listCheckpoints over an invalid manifest with StoredDataInvalid", async () => {
    const codes = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("damaged");
        yield* created(client, databaseId);
        const { id } = yield* (yield* ManifestReader).loadLatest(databaseId);
        yield* (yield* ObjectStore).put(manifestKey(databaseId, id), bytes("not a manifest"));
        return [
          yield* failureOf(client.databases.status({ params: { databaseId } }), "code"),
          yield* failureOf(client.databases.listCheckpoints({ params: { databaseId } }), "code"),
        ];
      }),
    );
    expect(codes).toEqual(["StoredDataInvalid", "StoredDataInvalid"]);
  });

  it("seals without taking the successors' holds", async () => {
    const holds = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("sealed");
        yield* created(client, databaseId);
        yield* client.databases.seal({
          params: { databaseId },
          payload: { successors: [DatabaseId.make("next-a")] },
        });
        // A split's seal takes each successor's hold (`holdForSuccessors`); the API's does not.
        return (yield* client.databases.listCheckpoints({ params: { databaseId } })).checkpoints
          .length;
      }),
    );
    expect(holds).toBe(0);
  });

  it("reports the newest manifest's L0, sorted runs, and drops on status and compaction", async () => {
    const observed = await run((client) =>
      Effect.gen(function* () {
        const databaseId = DatabaseId.make("compacted");
        yield* created(client, databaseId);
        for (const key of ["a", "b", "c"]) {
          yield* put(client, databaseId, key, key);
          yield* client.databases.flush({ params: { databaseId } });
        }
        // A compaction behind the writer's back: the two older L0 SSTs become one sorted run (oldest
        // key first), and the newest is dropped. The writer still holds its own manifest.
        const store = yield* ManifestStore;
        const before = yield* (yield* ManifestReader).loadLatest(databaseId);
        const [newest, middle, oldest] = before.manifest.l0;
        if (newest === undefined || middle === undefined || oldest === undefined) {
          return yield* Effect.die("three L0 SSTs");
        }
        yield* store.update(databaseId, (manifest) =>
          Result.succeed(
            replaceManifestFields(manifest, {
              l0: [newest],
              sortedRuns: [{ id: SortedRunId.make(0), ssts: [oldest, middle] }],
            }),
          ),
        );
        yield* store.update(databaseId, (manifest) =>
          dropSstFromManifest(manifest, {
            sstId: newest.id,
            reason: "Forced",
            droppedAt: Timestamp.make(0),
          }),
        );
        const status = yield* client.databases.status({ params: { databaseId } });
        const compaction = yield* client.databases.compaction({ params: { databaseId } });
        return {
          l0Count: status.l0Count,
          l0Ssts: compaction.l0Ssts,
          sortedRuns: compaction.sortedRuns.map((sortedRun) => [
            sortedRun.ssts,
            sortedRun.sizeBytes,
          ]),
          runBytes: oldest.sizeBytes + middle.sizeBytes,
          dropped: compaction.droppedSsts.map((dropped) => dropped.sstId),
          newest: newest.id,
        };
      }),
    );
    expect([observed.l0Count, observed.l0Ssts]).toEqual([0, 0]);
    expect(observed.sortedRuns).toEqual([[2, observed.runBytes]]);
    expect(observed.dropped).toEqual([observed.newest]);
  });

  it("drops an SST only when confirm repeats its id, forcing only when asked, and reports every corrupted SST", async () => {
    const first = SstId.make("01JD0000000000000000000AAA");
    const second = SstId.make("01JD0000000000000000000BBB");
    const corrupt = (sstId: SstId) => ({
      sstId,
      source: Option.none(),
      section: "DataBlock",
      reason: "ChecksumMismatch",
      message: "checksum",
      detectedAt: Timestamp.make(1),
    });
    const drops = Ref.makeUnsafe<
      ReadonlyArray<{ readonly sstId: string; readonly force: boolean }>
    >([]);
    const stub: ICompactorClient = {
      wake: () => Effect.die(new Error("not called")),
      destroy: () => Effect.void,
      status: (databaseId) =>
        Effect.succeed(
          Option.some({
            formatVersion: 1,
            databaseId,
            compactorEpoch: CompactorEpoch.make(1),
            running: [],
            fenced: false,
            corruptSsts: [corrupt(first), corrupt(second)],
            collectionDueAt: Option.none(),
          }),
        ),
      collect: () => Effect.die(new Error("not called")),
      requestCollection: () => Effect.void,
      dropSst: (databaseId, request) =>
        Ref.update(drops, (all) => [...all, { sstId: request.sstId, force: request.force }]).pipe(
          Effect.andThen(
            Effect.fail(new CompactorUnavailableError({ databaseId, message: "recorded" })),
          ),
        ),
    };
    const observed = await run(
      (client) =>
        Effect.gen(function* () {
          const databaseId = DatabaseId.make("drops");
          yield* created(client, databaseId);
          const drop = (confirm: string, force?: boolean) =>
            failureOf(
              client.databases.dropSst({
                params: { databaseId, sstId: first },
                payload: force === undefined ? { confirm } : { confirm, force },
              }),
              "code",
            );
          const mismatched = yield* drop(second);
          yield* drop(first);
          yield* drop(first, true);
          const compaction = yield* client.databases.compaction({ params: { databaseId } });
          return {
            mismatched,
            drops: yield* Ref.get(drops),
            corrupt: compaction.corruptSsts.map((reported) => reported.sstId),
          };
        }),
      Option.some(stub),
    );
    expect(observed.mismatched).toBe("SstDropRefused");
    expect(observed.drops).toEqual([
      { sstId: first, force: false },
      { sstId: first, force: true },
    ]);
    expect(observed.corrupt).toEqual([first, second]);
  });
});
