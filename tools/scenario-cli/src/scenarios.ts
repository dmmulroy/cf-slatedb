import { DatabaseId, SeqNum } from "@cf-slatedb/lsm-core";
import { DbHttpApi, GcHttpApi, sstKey } from "@cf-slatedb/lsm-cloudflare";
import { ShardedHttpApi, ShardGroup } from "@cf-slatedb/sharding";
import { Effect, Option } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import {
  assertTraces,
  eventually,
  expectEqual,
  expectFailure,
  expectThat,
  ScenarioStack,
  type Scenario,
  step,
  uniqueId,
} from "./scenario.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);

const api = Effect.gen(function* () {
  return yield* HttpApiClient.makeWith(DbHttpApi, {
    baseUrl: (yield* ScenarioStack).api,
    httpClient: yield* HttpClient.HttpClient,
  });
});
const gcApi = Effect.gen(function* () {
  return yield* HttpApiClient.makeWith(GcHttpApi, {
    baseUrl: (yield* ScenarioStack).gc,
    httpClient: yield* HttpClient.HttpClient,
  });
});
const shardsApi = Effect.gen(function* () {
  return yield* HttpApiClient.makeWith(ShardedHttpApi, {
    baseUrl: (yield* ScenarioStack).shards,
    httpClient: yield* HttpClient.HttpClient,
  });
});

const createDatabase = (
  databaseId: DatabaseId,
  options: { readonly merge?: boolean; readonly changelog?: boolean } = {},
) =>
  Effect.gen(function* () {
    const client = yield* api;
    yield* step(
      "create the database",
      client.databases.createDatabase({
        payload: {
          databaseId,
          keyRange: Option.none(),
          mergeOperator: options.merge === true ? Option.some("Concat") : Option.none(),
          changelogRetentionMs: options.changelog === true ? Option.some(3_600_000) : Option.none(),
          budgetGroup: Option.none(),
        },
      }),
    );
  });

const put = (databaseId: DatabaseId, key: string, value: string) =>
  Effect.flatMap(api, (client) =>
    step(
      `put ${key}`,
      client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Put", key: bytes(key), value: bytes(value), ttlMillis: Option.none() },
          ],
        },
      }),
    ),
  );

const read = (databaseId: DatabaseId, key: string, consistency: "Latest" | "Flushed") =>
  Effect.flatMap(api, (client) =>
    step(
      `read ${key} (${consistency})`,
      client.databases
        .get({
          params: { databaseId },
          payload: { key: bytes(key), consistency: { _tag: consistency }, snapshot: Option.none() },
        })
        .pipe(
          Effect.map((response) =>
            Option.getOrUndefined(
              Option.flatMap(response.row, (row) =>
                row.value._tag === "Value" ? Option.some(text(row.value.bytes)) : Option.none(),
              ),
            ),
          ),
        ),
    ),
  );

const flush = (databaseId: DatabaseId) =>
  Effect.flatMap(api, (client) =>
    step("flush", client.databases.flush({ params: { databaseId } })),
  );

/** Flushes four times (one put each), which wakes the compactor. */
const fourL0Ssts = (databaseId: DatabaseId) =>
  Effect.gen(function* () {
    for (const round of [0, 1, 2, 3]) {
      yield* put(databaseId, `k${round}`, `v${round}`);
      yield* flush(databaseId);
    }
  });

const compacted = (databaseId: DatabaseId) =>
  Effect.flatMap(api, (client) =>
    eventually(
      "compaction settles",
      step("compaction status", client.databases.compaction({ params: { databaseId } })),
      (status) =>
        status.l0Ssts === 0 &&
        status.sortedRuns.length === 1 &&
        Option.exists(status.compactor, (compactor) => compactor.running.length === 0),
    ),
  );

const writeRead: Scenario = {
  name: "write-read",
  description:
    "Puts, merges, and deletes, then reads at Latest, AtLeast, and Flushed; a Flushed read never calls the writer or LISTs.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-rw"));
    yield* createDatabase(databaseId, { merge: true });
    const client = yield* api;
    const receipt = yield* step(
      "write a batch",
      client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Put", key: bytes("a"), value: bytes("1"), ttlMillis: Option.none() },
            { _tag: "Put", key: bytes("b"), value: bytes("2"), ttlMillis: Option.none() },
            { _tag: "Merge", key: bytes("log"), operand: bytes("x"), ttlMillis: Option.none() },
          ],
        },
      }),
    );
    yield* step(
      "merge and delete",
      client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Merge", key: bytes("log"), operand: bytes("y"), ttlMillis: Option.none() },
            { _tag: "Delete", key: bytes("a") },
          ],
        },
      }),
    );
    yield* expectEqual("Latest log", yield* read(databaseId, "log", "Latest"), "xy");
    yield* expectEqual("Latest a (deleted)", yield* read(databaseId, "a", "Latest"), undefined);
    const atLeast = yield* step(
      "AtLeast read",
      client.databases.get({
        params: { databaseId },
        payload: {
          key: bytes("b"),
          consistency: { _tag: "AtLeast", seq: receipt.seq },
          snapshot: Option.none(),
        },
      }),
    );
    yield* expectThat(
      "AtLeast b",
      Option.isSome(atLeast.row),
      "AtLeast(seq) must see the write with that seq",
    );
    yield* flush(databaseId);
    yield* Effect.sleep("1200 millis");
    yield* expectEqual("Flushed log", yield* read(databaseId, "log", "Flushed"), "xy");
    const scanned = yield* step(
      "scan",
      client.databases.scan({
        params: { databaseId },
        payload: {
          range: { start: new Uint8Array(0), end: Option.none() },
          consistency: { _tag: "Latest" },
          snapshot: Option.none(),
          limit: 10,
        },
      }),
    );
    yield* expectEqual(
      "scanned keys",
      scanned.rows.map((row) => text(row.key)),
      ["b", "log"],
    );
    yield* assertTraces("a Flushed read stays in the reader", {
      select: {
        databaseId,
        containing: { name: "DbClient.getRow", attributes: { "read.consistency": "Flushed" } },
      },
      checks: [
        { _tag: "NoSpan", span: { name: "WriterClient.probe" } },
        { _tag: "NoSpan", span: { name: "ObjectStore.list" } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const flushScenario: Scenario = {
  name: "flush",
  description:
    "A flush uploads one L0 SST and commits exactly one manifest from the writer's own view (no load), then empties the memtable.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-flush"));
    yield* createDatabase(databaseId);
    for (const key of ["a", "b", "c"]) yield* put(databaseId, key, key);
    const flushed = yield* flush(databaseId);
    const client = yield* api;
    const status = yield* step("status", client.databases.status({ params: { databaseId } }));
    yield* expectEqual(
      "flushed everything",
      [status.flushedSeq, status.unflushedRows],
      [flushed.flushedSeq, 0],
    );
    yield* expectEqual("flushedSeq", status.flushedSeq, status.lastSeq);
    yield* assertTraces("one SST and one manifest commit per flush, from the writer's own view", {
      select: { databaseId, containing: { name: "DbWriter.requestFlush" } },
      checks: [
        { _tag: "SpanCount", span: { name: "SstStore.write" }, count: { equals: 1 } },
        { _tag: "SpanCount", span: { name: "ManifestStore.create" }, count: { equals: 1 } },
        // Nothing else committed since the writer fenced in: one create, no load (D56).
        {
          _tag: "SpanCount",
          span: {
            name: "ManifestStore.update",
            attributes: { "manifest.start": "OwnView", "manifest.cas_attempts": "1" },
          },
          count: { equals: 1 },
        },
        { _tag: "NoSpan", span: { name: "ManifestReader.loadLatest" } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const compaction: Scenario = {
  name: "compaction",
  description: "Four flushes wake the compactor, whose Workflow merges L0 into one sorted run.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-compact"));
    yield* createDatabase(databaseId);
    yield* fourL0Ssts(databaseId);
    const status = yield* compacted(databaseId);
    yield* expectEqual("one sorted run", status.sortedRuns.length, 1);
    for (const round of [0, 1, 2, 3]) {
      yield* expectEqual(`k${round}`, yield* read(databaseId, `k${round}`, "Latest"), `v${round}`);
    }
    yield* assertTraces("the compaction Workflow committed once", {
      select: { databaseId, containing: { name: "CompactionExecutor.commit" } },
      checks: [
        {
          _tag: "SpanExists",
          span: { name: "CompactionExecutor.commit" },
          parent: { name: "CompactionJob.run" },
        },
        { _tag: "SpanCount", span: { name: "ManifestStore.create" }, count: { equals: 1 } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const gc: Scenario = {
  name: "gc",
  description:
    "After a compaction, the database's compactor collects the replaced L0 SSTs and old manifests from its own alarm once gcMinAge (10 s in dev) passes; a later collect-now finds nothing left, and reads are unaffected.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-gc"));
    yield* createDatabase(databaseId);
    yield* fourL0Ssts(databaseId);
    yield* compacted(databaseId);
    // The compactor notices the finished job on its next poll and collects gcMinAge later.
    yield* Effect.sleep("14 seconds");
    yield* assertTraces("the compactor collected on its own", {
      select: { databaseId, containing: { name: "Compactor.alarm" } },
      checks: [
        {
          _tag: "SpanExists",
          span: { name: "GarbageCollector.collect", attributes: { "gc.deleted_ssts": "4" } },
        },
        { _tag: "SpanExists", span: { name: "ManifestStore.advanceManifestBoundary" } },
        { _tag: "NoErrorStatus" },
      ],
    });
    const collector = yield* gcApi;
    const report = yield* step("collect", collector.gc.collect({ params: { databaseId } }));
    yield* expectEqual("nothing left to collect", report.deletedSsts, 0);
    for (const round of [0, 1, 2, 3]) {
      yield* expectEqual(`k${round}`, yield* read(databaseId, `k${round}`, "Latest"), `v${round}`);
    }
    yield* assertTraces("collect-now runs on the compactor", {
      select: { databaseId, containing: { name: "Compactor.collect" } },
      checks: [
        { _tag: "SpanExists", span: { name: "GarbageCollector.collect" } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const forkSplit: Scenario = {
  name: "fork-split",
  description:
    "The sharding package splits a shard (seal, checkpoint, two zero-copy forks, publish) while keys stay readable.",
  run: Effect.gen(function* () {
    const group = ShardGroup.make(uniqueId("sc-group"));
    const first = DatabaseId.make(`${group}-0`);
    const since = Date.now();
    const shards = yield* shardsApi;
    yield* step(
      "create the group",
      shards.groups.create({ params: { group }, payload: { databaseId: first } }),
    );
    for (const key of "abcdefgh") {
      yield* step(
        `put ${key}`,
        shards.groups.put({
          params: { group },
          payload: { key: bytes(key), value: bytes(key.toUpperCase()) },
        }),
      );
    }
    const map = yield* step(
      "split at e",
      shards.groups.split({ params: { group }, payload: { databaseId: first, atKey: bytes("e") } }),
    );
    yield* expectEqual("two shards", map.shards.length, 2);
    yield* step(
      "put x",
      shards.groups.put({ params: { group }, payload: { key: bytes("x"), value: bytes("X") } }),
    );
    for (const key of "acegx") {
      const got = yield* step(
        `get ${key}`,
        shards.groups.get({ params: { group }, payload: { key: bytes(key) } }),
      );
      yield* expectEqual(
        `value of ${key}`,
        Option.map(got.value, text),
        Option.some(key.toUpperCase()),
      );
    }
    const scanned = yield* step(
      "scan",
      shards.groups.scan({
        params: { group },
        payload: { range: { start: new Uint8Array(0), end: Option.none() }, limit: 100 },
      }),
    );
    yield* expectEqual(
      "scan order",
      scanned.rows.map((row) => text(row.key)).join(""),
      "abcdefghx",
    );
    yield* assertTraces("one split across Workers", {
      select: { sinceUnixMs: since, containing: { name: "ShardSplit.split" } },
      checks: [
        { _tag: "SameTrace", spans: [{ name: "ShardSplit.split" }, { name: "DbWriter.seal" }] },
        { _tag: "SpanCount", span: { name: "DatabaseLifecycle.forkRange" }, count: { equals: 2 } },
        { _tag: "SpanCount", span: { name: "ShardDirectory.completeSplit" }, count: { equals: 1 } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const fencing: Scenario = {
  name: "fencing",
  description:
    "Sealing fences the writer: later writes are refused with DatabaseSealed and never reach SQLite.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-fence"));
    const successor = DatabaseId.make(`${databaseId}-next`);
    yield* createDatabase(databaseId);
    yield* put(databaseId, "a", "1");
    const client = yield* api;
    yield* step(
      "seal",
      client.databases.seal({ params: { databaseId }, payload: { successors: [successor] } }),
    );
    const refused = yield* expectFailure("write after the seal", put(databaseId, "b", "2"));
    yield* expectThat(
      "refused as sealed",
      refused.message.startsWith("DatabaseSealedPublicError"),
      refused.message,
    );
    const status = yield* step("status", client.databases.status({ params: { databaseId } }));
    yield* expectEqual("sealed", status.sealed, true);
    yield* expectEqual("the sealed data", yield* read(databaseId, "a", "Latest"), "1");
    yield* assertTraces("the refused write never committed", {
      select: { databaseId, containing: { name: "DbWriter.write", error: true } },
      checks: [{ _tag: "NoSpan", span: { name: "MemTable.commit" } }],
    });
  }),
};

const changelog: Scenario = {
  name: "changelog",
  description:
    "The change feed delivers every change once, in order, across flushes (changelog SSTs) and the memtable.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-cdc"));
    yield* createDatabase(databaseId, { changelog: true });
    for (const key of ["a", "b", "c"]) {
      yield* put(databaseId, key, key);
      yield* flush(databaseId);
    }
    yield* put(databaseId, "d", "d");
    yield* Effect.sleep("1200 millis");
    const client = yield* api;
    const seen: Array<string> = [];
    let afterSeq = SeqNum.make(0);
    for (let page = 0; page < 10; page++) {
      const changes = yield* step(
        "read changes",
        client.databases.changes({ params: { databaseId }, query: { afterSeq, limit: 2 } }),
      );
      if (changes.rows.length === 0) break;
      seen.push(...changes.rows.map((row) => text(row.key)));
      afterSeq = changes.nextAfterSeq;
    }
    yield* expectEqual("every change once, in order", seen, ["a", "b", "c", "d"]);
    yield* assertTraces("flushed changes came from the changelog SSTs", {
      select: { databaseId, containing: { name: "DbReader.changelogRows" } },
      checks: [{ _tag: "NoErrorStatus" }],
    });
  }),
};

const transactions: Scenario = {
  name: "transactions",
  description:
    "Write skew commits under Snapshot but is refused under SerializableSnapshot (ReadConflict).",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-txn"));
    yield* createDatabase(databaseId);
    yield* put(databaseId, "x", "1");
    yield* put(databaseId, "y", "1");
    const client = yield* api;
    const skew = (isolation: "Snapshot" | "SerializableSnapshot") =>
      Effect.gen(function* () {
        const first = yield* step(
          "begin",
          client.databases.beginTransaction({ params: { databaseId } }),
        );
        const second = yield* step(
          "begin",
          client.databases.beginTransaction({ params: { databaseId } }),
        );
        const commit = (readSeq: SeqNum, key: string) =>
          client.databases.commitTransaction({
            params: { databaseId },
            payload: {
              readSeq,
              isolation,
              batch: {
                operations: [
                  { _tag: "Put", key: bytes(key), value: bytes("0"), ttlMillis: Option.none() },
                ],
              },
              readKeys: [bytes("x"), bytes("y")],
              readRanges: [],
            },
          });
        yield* step("first commit", commit(first.readSeq, "x"));
        return yield* Effect.result(commit(second.readSeq, "y"));
      });
    const snapshot = yield* skew("Snapshot");
    yield* expectEqual("Snapshot allows write skew", snapshot._tag, "Success");
    const serializable = yield* skew("SerializableSnapshot");
    yield* expectThat(
      "SerializableSnapshot refuses it",
      serializable._tag === "Failure" &&
        serializable.failure._tag === "TransactionConflictPublicError" &&
        serializable.failure.details.reason === "ReadConflict",
      "expected TransactionConflict (ReadConflict)",
    );
    yield* assertTraces("the conflict check ran in the writer", {
      select: { databaseId, containing: { name: "DbWriter.commitTransaction" } },
      checks: [{ _tag: "SpanExists", span: { name: "MemTable.writtenAfter" } }],
    });
  }),
};

const removal: Scenario = {
  name: "remove",
  description:
    "Removing a database deletes its objects and purges its cache tag; it is then not found.",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-remove"));
    yield* createDatabase(databaseId);
    yield* put(databaseId, "a", "1");
    yield* flush(databaseId);
    const client = yield* api;
    yield* step("remove", client.databases.removeDatabase({ params: { databaseId } }));
    const gone = yield* expectFailure(
      "read after removal",
      step("compaction status", client.databases.compaction({ params: { databaseId } })),
    );
    yield* expectThat(
      "not found",
      gone.message.startsWith("DatabaseNotFoundPublicError"),
      gone.message,
    );
    yield* assertTraces("removal purged the cache tag through BlockServer", {
      select: { databaseId, containing: { name: "DatabaseLifecycle.remove" } },
      checks: [
        { _tag: "SpanExists", span: { name: "BlockServer.purge" } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

const blockCache: Scenario = {
  name: "block-cache",
  description:
    "Reads one flushed block through two Workers: the second gets it from Workers Cache (Cf-Cache-Status HIT) on an unrestricted preview.",
  run: Effect.gen(function* () {
    const { workersCache } = yield* ScenarioStack;
    const group = ShardGroup.make(uniqueId("sc-cache"));
    const databaseId = DatabaseId.make(`${group}-0`);
    const shards = yield* shardsApi;
    yield* step(
      "create the group",
      shards.groups.create({ params: { group }, payload: { databaseId } }),
    );
    yield* step(
      "put k",
      shards.groups.put({ params: { group }, payload: { key: bytes("k"), value: bytes("V") } }),
    );
    yield* flush(databaseId);
    // The API Worker's isolate reads the block first: an L1 miss that fills Workers Cache.
    yield* expectEqual("value of k through the API", yield* read(databaseId, "k", "Flushed"), "V");
    // The shard router is another Worker with its own L1: its BlockServer call for the same block
    // URL is answered by Workers Cache without running the BlockServer.
    const got = yield* step(
      "get k through the shard router",
      shards.groups.get({ params: { group }, payload: { key: bytes("k") } }),
    );
    yield* expectEqual(
      "value of k through the router",
      Option.map(got.value, text),
      Option.some("V"),
    );
    yield* assertTraces("the second Worker's block came from Workers Cache", {
      select: { databaseId },
      checks: [
        { _tag: "SpanCount", span: { name: "BlockSource.block" }, count: { atLeast: 2 } },
        // Workers Cache is not emulated locally, and a Stack restricted to a jurisdiction turns it
        // off: there no block is served from it.
        workersCache
          ? { _tag: "SpanExists", span: { attributes: { "cache.l2.status": "HIT" } } }
          : {
              _tag: "SpanCount",
              span: { attributes: { "cache.l2.status": "HIT" } },
              count: { equals: 0 },
            },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

/** Concurrent creates of one database in `create-race`. */
const CREATE_RACERS = 16;

const createRace: Scenario = {
  name: "create-race",
  description: `${CREATE_RACERS} concurrent creates of one database: R2 create-if-absent (If-None-Match: * as Headers) lets exactly one win.`,
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-race"));
    const client = yield* api;
    const outcomes = yield* Effect.forEach(
      Array.from({ length: CREATE_RACERS }, (_, index) => index),
      () =>
        Effect.result(
          client.databases.createDatabase({
            payload: {
              databaseId,
              keyRange: Option.none(),
              mergeOperator: Option.none(),
              changelogRetentionMs: Option.none(),
              budgetGroup: Option.none(),
            },
          }),
        ),
      { concurrency: "unbounded" },
    );
    const created = outcomes.filter((outcome) => outcome._tag === "Success").length;
    const refused = outcomes.filter(
      (outcome) =>
        outcome._tag === "Failure" && outcome.failure._tag === "DatabaseAlreadyExistsPublicError",
    ).length;
    yield* expectEqual("creates that won", created, 1);
    yield* expectEqual("creates refused as DatabaseAlreadyExists", refused, CREATE_RACERS - 1);
    yield* assertTraces("one manifest created; every other create saw AlreadyExists", {
      select: { databaseId },
      checks: [
        {
          _tag: "SpanCount",
          span: {
            name: "ObjectStore.putIfAbsent",
            attributes: { "object.kind": "Manifest", "object.outcome": "Created" },
          },
          count: { equals: 1 },
        },
        {
          _tag: "SpanCount",
          span: {
            name: "ObjectStore.putIfAbsent",
            attributes: { "object.kind": "Manifest", "object.outcome": "AlreadyExists" },
          },
          count: { equals: CREATE_RACERS - 1 },
        },
      ],
    });
  }),
};

/** Inverts one byte of a stored object through the probe Worker (local and preview stages only). */
const damageObject = (key: string, offset: number) =>
  Effect.gen(function* () {
    const { probe } = yield* ScenarioStack;
    const http = yield* HttpClient.HttpClient;
    const response = yield* step(
      `damage ${key}`,
      http.execute(
        HttpClientRequest.post(`${probe}/v1/probe/damage`).pipe(
          HttpClientRequest.setUrlParams({ key, offset: String(offset) }),
        ),
      ),
    );
    yield* expectEqual(`damage ${key}`, response.status, 200);
  });

const repair: Scenario = {
  name: "repair",
  description:
    "A corrupted L0 SST stops L0 compaction and is reported; the operator's drop (confirm repeating the SST id) removes it, compaction resumes, and only its rows are gone (D62).",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-repair"));
    yield* createDatabase(databaseId);
    const client = yield* api;
    // A checkpoint before any data: created before every SST, so it could restore any of them.
    const checkpoint = yield* step(
      "checkpoint",
      client.databases.createCheckpoint({
        params: { databaseId },
        payload: { name: Option.some("before"), lifetimeMs: Option.some(600_000) },
      }),
    );
    yield* put(databaseId, "k0", "v0");
    yield* flush(databaseId);
    const first = yield* step(
      "compaction status",
      client.databases.compaction({ params: { databaseId } }),
    );
    const victim = first.l0SstIds[0];
    if (victim === undefined) {
      return yield* expectThat("one L0 SST", false, "the flush left no L0 SST");
    }
    // Byte 2 is inside the first data block: its checksum no longer matches.
    yield* damageObject(sstKey(databaseId, victim), 2);
    for (const round of [1, 2, 3]) {
      yield* put(databaseId, `k${round}`, `v${round}`);
      yield* flush(databaseId);
    }
    const reported = yield* eventually(
      "the compactor reports the corrupted SST",
      step("compaction status", client.databases.compaction({ params: { databaseId } })),
      (status) =>
        Option.exists(status.compactor, (compactor) =>
          compactor.corruptSsts.some((corrupt) => corrupt.sstId === victim),
        ),
    );
    yield* expectEqual("L0 is blocked behind it", reported.l0Ssts, 4);
    const corrupt = reported.corruptSsts.find((entry) => entry.sstId === victim);
    const listed = yield* step(
      "list checkpoints",
      client.databases.listCheckpoints({ params: { databaseId } }),
    );
    yield* expectThat(
      "the checkpoint predates the corrupted SST (restoring from it is possible)",
      corrupt !== undefined &&
        listed.checkpoints.some(
          (entry) => entry.id === checkpoint.id && entry.createdAt < corrupt.createdAt,
        ),
      JSON.stringify({ corrupt: corrupt?.createdAt, checkpoints: listed.checkpoints.length }),
    );
    // A fifth flush: still blocked (L0 merges oldest first); after the drop, four compact.
    yield* put(databaseId, "k4", "v4");
    yield* flush(databaseId);
    const mismatch = yield* expectFailure(
      "a drop without the confirmation",
      step(
        "drop without confirm",
        client.databases.dropSst({
          params: { databaseId, sstId: victim },
          payload: { confirm: "yes" },
        }),
      ),
    );
    yield* expectThat(
      "refused as a confirmation mismatch",
      mismatch.message.includes("SstDropRefused"),
      mismatch.message,
    );
    const report = yield* step(
      "drop the corrupted SST",
      client.databases.dropSst({
        params: { databaseId, sstId: victim },
        payload: { confirm: victim },
      }),
    );
    yield* expectEqual("dropped as corrupted", report.dropped.reason, "Corrupted");
    const settled = yield* compacted(databaseId);
    yield* expectEqual(
      "the drop is recorded",
      settled.droppedSsts.map((dropped) => dropped.sstId),
      [victim],
    );
    yield* expectEqual(
      "k0 lived only in the dropped SST",
      yield* read(databaseId, "k0", "Latest"),
      undefined,
    );
    for (const round of [1, 2, 3, 4]) {
      yield* expectEqual(`k${round}`, yield* read(databaseId, `k${round}`, "Latest"), `v${round}`);
    }
    yield* assertTraces("the compactor dropped it by one manifest commit", {
      select: { databaseId, containing: { name: "Compactor.dropSst" } },
      checks: [
        {
          _tag: "SpanExists",
          span: { name: "Compactor.dropSst", attributes: { "drop.reason": "Corrupted" } },
        },
        { _tag: "SpanCount", span: { name: "ManifestStore.create" }, count: { equals: 1 } },
        { _tag: "NoErrorStatus" },
      ],
    });
  }),
};

/** Span attributes that identify a caller or carry a query string: never exported (D63). */
const CALLER_ATTRIBUTES = ["client.address", "user_agent.original", "url.full", "url.query"];

const redaction: Scenario = {
  name: "redaction",
  description:
    "A request with a User-Agent and a query string: its server and client spans export the method, route, and status, and never the caller's address, user agent, full URL, or query (D63).",
  run: Effect.gen(function* () {
    const databaseId = DatabaseId.make(uniqueId("sc-redact"));
    yield* createDatabase(databaseId);
    yield* put(databaseId, "k", "v");
    const { api: apiUrl } = yield* ScenarioStack;
    const http = yield* HttpClient.HttpClient;
    const response = yield* step(
      "status with a query string and a user agent",
      http.execute(
        HttpClientRequest.get(`${apiUrl}/v1/databases/${databaseId}/status`).pipe(
          HttpClientRequest.setUrlParams({ probe: "redaction-secret" }),
          HttpClientRequest.setHeader("user-agent", "cf-slatedb-scenario/redaction"),
        ),
      ),
    );
    yield* expectEqual("status answered", response.status, 200);
    yield* assertTraces("server and client spans carry no caller attributes", {
      select: { databaseId, containing: { name: "WriterClient.status" } },
      checks: [
        {
          _tag: "Attribute",
          span: { name: "GET" },
          requiredKeys: ["http.request.method", "http.response.status_code"],
          forbiddenKeys: CALLER_ATTRIBUTES,
        },
        { _tag: "SpanExists", span: { name: "WriterClient.status" } },
      ],
    });
  }),
};

/** Every scenario, in the order `run all` runs them. */
export const SCENARIOS: ReadonlyArray<Scenario> = [
  writeRead,
  flushScenario,
  compaction,
  gc,
  forkSplit,
  fencing,
  changelog,
  transactions,
  removal,
  blockCache,
  createRace,
  repair,
  redaction,
];
