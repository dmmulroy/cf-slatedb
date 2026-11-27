import * as Cloudflare from "@alchemy.run/cloudflare";
import { CheckpointId, DatabaseId, SeqNum, SstId } from "@shaledb/lsm-core";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, type Layer, Option, type Scope } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";
import { describe, expect, it } from "vite-plus/test";

import { DbHttpApi } from "./db-http-api.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";

/**
 * Every public route, through the real router, its middleware, and a generated client, against
 * databases whose writers start cold on their first request, as a Durable Object does (D131). Each
 * answer is recorded as the route, its HTTP status, and for a failure its public code, so a route
 * that answers 503 where workerd answers 2xx or 404 fails here.
 */
type TestServices =
  | Layer.Success<ReturnType<typeof dbServicesTestLayer>>
  | Layer.Success<typeof lsmTestRuntimeContextLayer>
  | Scope.Scope;

interface Answer {
  readonly route: string;
  readonly status: number;
  readonly code: string | null;
}

const run = <A, E>(
  body: (
    client: HttpApiClient.ForApi<typeof DbHttpApi>,
    answers: Array<Answer>,
    route: (name: string) => void,
  ) => Effect.Effect<A, E, TestServices>,
) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
    const runtime = yield* RuntimeContext;
    const answers: Array<Answer> = [];
    let current = "";
    const client = yield* HttpApiClient.makeWith(DbHttpApi, {
      baseUrl: "http://api.internal",
      httpClient: Cloudflare.toHttpClient({
        fetch: (request: HttpServerRequest.HttpServerRequest) =>
          app.pipe(
            Effect.provideService(HttpServerRequest.HttpServerRequest, request),
            Effect.scoped,
            Effect.provideService(RuntimeContext, runtime),
            Effect.tap((response) =>
              Effect.sync(() =>
                answers.push({ route: current, status: response.status, code: null }),
              ),
            ),
          ),
      }),
    });
    return yield* body(client, answers, (name) => {
      current = name;
    });
  }).pipe(
    Effect.scoped,
    Effect.provide(dbServicesTestLayer()),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

/** Runs `call` as `name`, records its public error code if it fails, and never fails itself. */
const step = <A>(
  answers: Array<Answer>,
  route: (name: string) => void,
  name: string,
  call: Effect.Effect<A, unknown, TestServices>,
) =>
  Effect.gen(function* () {
    route(name);
    const result = yield* Effect.result(call);
    if (result._tag === "Failure") {
      const last = answers.at(-1);
      const failure = result.failure;
      const code =
        typeof failure === "object" && failure !== null && "code" in failure
          ? String(failure.code)
          : typeof failure === "object" && failure !== null && "_tag" in failure
            ? String(failure._tag)
            : "unknown";
      if (last !== undefined && last.route === name)
        answers[answers.length - 1] = { ...last, code };
      else answers.push({ route: name, status: 0, code });
      return Option.none<A>();
    }
    return Option.some(result.success);
  });

const bytes = (text: string) => new TextEncoder().encode(text);
const all = { start: new Uint8Array(0), end: Option.none<Uint8Array>() };

describe("every public route against cold databases (D131)", () => {
  it("answers as workerd does: 2xx on a database, 404 DatabaseNotFound on a missing one", async () => {
    const answers = await run((client, answers, route) =>
      Effect.gen(function* () {
        const call = <A>(name: string, effect: Effect.Effect<A, unknown, TestServices>) =>
          step(answers, route, name, effect);
        const databaseId = DatabaseId.make("orders");
        const params = { databaseId };
        yield* call(
          "createDatabase",
          client.databases.createDatabase({
            payload: {
              databaseId,
              keyRange: Option.none(),
              mergeOperator: Option.none(),
              changelogRetentionMs: Option.some(60_000),
              budgetGroup: Option.none(),
            },
          }),
        );
        for (const key of ["a", "b", "c"]) {
          yield* call(
            "write",
            client.databases.write({
              params,
              payload: {
                operations: [
                  { _tag: "Put", key: bytes(key), value: bytes(key), ttlMillis: Option.none() },
                ],
              },
            }),
          );
        }
        for (const consistency of [
          { _tag: "Latest" } as const,
          { _tag: "AtLeast", seq: SeqNum.make(1) } as const,
          { _tag: "Flushed" } as const,
        ]) {
          yield* call(
            `get ${consistency._tag}`,
            client.databases.get({
              params,
              payload: { key: bytes("a"), consistency, snapshot: Option.none() },
            }),
          );
        }
        const page = yield* call(
          "scan",
          client.databases.scan({
            params,
            payload: {
              range: all,
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              limit: 1,
            },
          }),
        );
        const resumeFrom = Option.flatMap(page, (answer) => answer.resumeFrom);
        if (Option.isSome(resumeFrom)) {
          yield* call(
            "scan resumeFrom",
            client.databases.scan({
              params,
              payload: {
                range: { start: resumeFrom.value, end: Option.none() },
                consistency: { _tag: "Latest" },
                snapshot: Option.none(),
                limit: 10,
              },
            }),
          );
        }
        const begun = yield* call(
          "beginTransaction",
          client.databases.beginTransaction({ params }),
        );
        if (Option.isSome(begun)) {
          const { readSeq, manifestId } = begun.value;
          yield* call(
            "transactionGet",
            client.databases.transactionGet({
              params,
              payload: { readSeq, manifestId, key: bytes("a") },
            }),
          );
          yield* call(
            "transactionScan",
            client.databases.transactionScan({
              params,
              payload: { readSeq, manifestId, range: all, limit: 10 },
            }),
          );
          yield* call(
            "commitTransaction",
            client.databases.commitTransaction({
              params,
              payload: {
                readSeq,
                isolation: "SerializableSnapshot",
                batch: {
                  operations: [
                    { _tag: "Put", key: bytes("t"), value: bytes("t"), ttlMillis: Option.none() },
                  ],
                },
                readKeys: [bytes("a")],
                readRanges: [],
              },
            }),
          );
        }
        yield* call(
          "changes",
          client.databases.changes({ params, query: { afterSeq: SeqNum.make(0), limit: 10 } }),
        );
        yield* call("flush", client.databases.flush({ params }));
        yield* call("status", client.databases.status({ params }));
        const compaction = yield* call("compaction", client.databases.compaction({ params }));
        const sstId = Option.flatMap(compaction, (answer) =>
          Option.fromUndefinedOr(answer.l0SstIds[0]),
        );
        const checkpoint = yield* call(
          "createCheckpoint",
          client.databases.createCheckpoint({
            params,
            payload: { name: Option.some("e2e"), lifetimeMs: Option.none() },
          }),
        );
        yield* call("listCheckpoints", client.databases.listCheckpoints({ params }));
        const snapshot = yield* call("snapshot", client.databases.snapshot({ params }));
        if (Option.isSome(snapshot)) {
          yield* call(
            "get at snapshot",
            client.databases.get({
              params,
              payload: {
                key: bytes("a"),
                consistency: { _tag: "Latest" },
                snapshot: Option.some(snapshot.value),
              },
            }),
          );
        }
        yield* call(
          "pinSnapshot",
          client.databases.pinSnapshot({ params, payload: { lifetimeMs: 60_000 } }),
        );
        if (Option.isSome(checkpoint)) {
          yield* call(
            "fork",
            client.databases.fork({
              params,
              payload: {
                forkId: DatabaseId.make("orders-fork"),
                checkpointId: checkpoint.value.id,
                range: all,
              },
            }),
          );
          yield* call(
            "get on the fork",
            client.databases.get({
              params: { databaseId: DatabaseId.make("orders-fork") },
              payload: {
                key: bytes("b"),
                consistency: { _tag: "Latest" },
                snapshot: Option.none(),
              },
            }),
          );
          yield* call(
            "deleteCheckpoint",
            client.databases.deleteCheckpoint({
              params: { databaseId, checkpointId: checkpoint.value.id },
            }),
          );
        }
        if (Option.isSome(sstId)) {
          yield* call(
            "dropSst",
            client.databases.dropSst({
              params: { databaseId, sstId: sstId.value },
              payload: { confirm: sstId.value, force: true },
            }),
          );
        }
        // Seal and remove a second database, each on its first request since creation.
        const sealed = DatabaseId.make("sealed");
        yield* call(
          "createDatabase (sealed)",
          client.databases.createDatabase({
            payload: {
              databaseId: sealed,
              keyRange: Option.none(),
              mergeOperator: Option.none(),
              changelogRetentionMs: Option.none(),
              budgetGroup: Option.none(),
            },
          }),
        );
        yield* call(
          "seal",
          client.databases.seal({
            params: { databaseId: sealed },
            payload: { successors: [DatabaseId.make("sealed-a")] },
          }),
        );
        const removed = DatabaseId.make("removed");
        yield* call(
          "createDatabase (removed)",
          client.databases.createDatabase({
            payload: {
              databaseId: removed,
              keyRange: Option.none(),
              mergeOperator: Option.none(),
              changelogRetentionMs: Option.none(),
              budgetGroup: Option.none(),
            },
          }),
        );
        yield* call(
          "removeDatabase",
          client.databases.removeDatabase({ params: { databaseId: removed } }),
        );

        // Every route on a database that never existed.
        const missing = { databaseId: DatabaseId.make("missing") };
        const checkpointId = CheckpointId.make("01JD0000000000000000000AAA");
        const missingSst = SstId.make("01JD0000000000000000000AAA");
        const onMissing: ReadonlyArray<
          readonly [string, Effect.Effect<unknown, unknown, TestServices>]
        > = [
          [
            "write",
            client.databases.write({
              params: missing,
              payload: { operations: [{ _tag: "Delete", key: bytes("a") }] },
            }),
          ],
          [
            "get Latest",
            client.databases.get({
              params: missing,
              payload: {
                key: bytes("a"),
                consistency: { _tag: "Latest" },
                snapshot: Option.none(),
              },
            }),
          ],
          [
            "get Flushed",
            client.databases.get({
              params: missing,
              payload: {
                key: bytes("a"),
                consistency: { _tag: "Flushed" },
                snapshot: Option.none(),
              },
            }),
          ],
          [
            "scan",
            client.databases.scan({
              params: missing,
              payload: {
                range: all,
                consistency: { _tag: "Latest" },
                snapshot: Option.none(),
                limit: 1,
              },
            }),
          ],
          ["beginTransaction", client.databases.beginTransaction({ params: missing })],
          [
            "transactionGet",
            client.databases.transactionGet({
              params: missing,
              payload: { readSeq: SeqNum.make(0), manifestId: Option.none(), key: bytes("a") },
            }),
          ],
          [
            "transactionScan",
            client.databases.transactionScan({
              params: missing,
              payload: { readSeq: SeqNum.make(0), manifestId: Option.none(), range: all, limit: 1 },
            }),
          ],
          [
            "commitTransaction",
            client.databases.commitTransaction({
              params: missing,
              payload: {
                readSeq: SeqNum.make(0),
                isolation: "Snapshot",
                batch: { operations: [{ _tag: "Delete", key: bytes("a") }] },
                readKeys: [],
                readRanges: [],
              },
            }),
          ],
          [
            "changes",
            client.databases.changes({
              params: missing,
              query: { afterSeq: SeqNum.make(0), limit: 1 },
            }),
          ],
          ["flush", client.databases.flush({ params: missing })],
          ["status", client.databases.status({ params: missing })],
          ["compaction", client.databases.compaction({ params: missing })],
          [
            "dropSst",
            client.databases.dropSst({
              params: { ...missing, sstId: missingSst },
              payload: { confirm: missingSst },
            }),
          ],
          [
            "createCheckpoint",
            client.databases.createCheckpoint({
              params: missing,
              payload: { name: Option.none(), lifetimeMs: Option.none() },
            }),
          ],
          ["listCheckpoints", client.databases.listCheckpoints({ params: missing })],
          [
            "deleteCheckpoint",
            client.databases.deleteCheckpoint({ params: { ...missing, checkpointId } }),
          ],
          ["snapshot", client.databases.snapshot({ params: missing })],
          [
            "pinSnapshot",
            client.databases.pinSnapshot({ params: missing, payload: { lifetimeMs: 1_000 } }),
          ],
          [
            "seal",
            client.databases.seal({
              params: missing,
              payload: { successors: [DatabaseId.make("x")] },
            }),
          ],
          [
            "fork",
            client.databases.fork({
              params: missing,
              payload: { forkId: DatabaseId.make("missing-fork"), checkpointId, range: all },
            }),
          ],
          ["removeDatabase", client.databases.removeDatabase({ params: missing })],
        ];
        for (const [name, effect] of onMissing) yield* call(`missing: ${name}`, effect);
        return answers;
      }),
    );
    // dropSst goes to the compactor, and the test compactor client is a stub that drops nothing
    // (`compactorClientTestLayer`): its answer here is the stub's, not the route's.
    const routes = answers
      .filter((answer) => !answer.route.endsWith("dropSst"))
      .map((answer) => [answer.route, answer.status, answer.code]);
    expect(routes).toEqual([
      ["createDatabase", 201, null],
      ["write", 200, null],
      ["write", 200, null],
      ["write", 200, null],
      ["get Latest", 200, null],
      ["get AtLeast", 200, null],
      ["get Flushed", 200, null],
      ["scan", 200, null],
      ["scan resumeFrom", 200, null],
      ["beginTransaction", 200, null],
      ["transactionGet", 200, null],
      ["transactionScan", 200, null],
      ["commitTransaction", 200, null],
      ["changes", 200, null],
      ["flush", 200, null],
      ["status", 200, null],
      ["compaction", 200, null],
      ["createCheckpoint", 201, null],
      ["listCheckpoints", 200, null],
      ["snapshot", 200, null],
      ["get at snapshot", 200, null],
      ["pinSnapshot", 201, null],
      ["fork", 201, null],
      ["get on the fork", 200, null],
      ["deleteCheckpoint", 204, null],
      ["createDatabase (sealed)", 201, null],
      ["seal", 200, null],
      ["createDatabase (removed)", 201, null],
      ["removeDatabase", 204, null],
      ["missing: write", 404, "DatabaseNotFound"],
      ["missing: get Latest", 404, "DatabaseNotFound"],
      ["missing: get Flushed", 404, "DatabaseNotFound"],
      ["missing: scan", 404, "DatabaseNotFound"],
      ["missing: beginTransaction", 404, "DatabaseNotFound"],
      ["missing: transactionGet", 404, "DatabaseNotFound"],
      ["missing: transactionScan", 404, "DatabaseNotFound"],
      ["missing: commitTransaction", 404, "DatabaseNotFound"],
      ["missing: changes", 404, "DatabaseNotFound"],
      ["missing: flush", 404, "DatabaseNotFound"],
      ["missing: status", 404, "DatabaseNotFound"],
      ["missing: compaction", 404, "DatabaseNotFound"],
      ["missing: createCheckpoint", 404, "DatabaseNotFound"],
      ["missing: listCheckpoints", 404, "DatabaseNotFound"],
      ["missing: deleteCheckpoint", 404, "DatabaseNotFound"],
      ["missing: snapshot", 404, "DatabaseNotFound"],
      ["missing: pinSnapshot", 404, "DatabaseNotFound"],
      ["missing: seal", 404, "DatabaseNotFound"],
      ["missing: fork", 404, "DatabaseNotFound"],
      ["missing: removeDatabase", 404, "DatabaseNotFound"],
    ]);
  }, 120_000);
});
