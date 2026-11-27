import { DatabaseId, SeqNum } from "@shaledb/lsm-core";
import { DbHttpApi } from "@shaledb/lsm-cloudflare";
import { Data, Duration, Effect, Option } from "effect";
import { HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";

/** A smoke check failed: which step, and what it saw. */
export class SmokeFailure extends Data.TaggedError("SmokeFailure")<{
  readonly step: string;
  readonly message: string;
}> {}

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);

const tagOf = (error: unknown) =>
  typeof error === "object" && error !== null && "_tag" in error && typeof error._tag === "string"
    ? error._tag
    : "unknown";

/** Longest one smoke request may take before the run fails naming it (D104). */
export const SMOKE_STEP_TIMEOUT = Duration.seconds(60);

/**
 * Runs a step, turning any failure into a `SmokeFailure` naming it; a step that does not answer
 * within {@link SMOKE_STEP_TIMEOUT} fails too, so a hung request cannot hang the run.
 */
const step = <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: SMOKE_STEP_TIMEOUT,
      orElse: () =>
        Effect.fail(
          new SmokeFailure({
            step: name,
            message: `no answer within ${Duration.format(SMOKE_STEP_TIMEOUT)}`,
          }),
        ),
    }),
    Effect.mapError((error) =>
      error instanceof SmokeFailure
        ? error
        : new SmokeFailure({ step: name, message: `${tagOf(error)}: ${String(error)}` }),
    ),
  );

const expect = (name: string, ok: boolean, message: string) =>
  ok ? Effect.void : Effect.fail(new SmokeFailure({ step: name, message }));

/**
 * The production smoke test (v1 workstream 10, D65): only operations safe on a live installation,
 * on databases it names itself (`smoke-<time>-…`) and removes at the end, whatever happened. It
 * makes no trace assertions, since production keeps a sample of traces.
 *
 * Checks, against `apiUrl`:
 * - Create, write a batch, and read it back at `Latest`, `AtLeast`, and `Flushed` (after a flush).
 * - Scan, a `SerializableSnapshot` transaction, and a conflicting one refused.
 * - A checkpoint created, listed, and deleted.
 * - Concurrent creates of one id: exactly one wins, the rest are `DatabaseAlreadyExists`.
 * - Removal: the databases answer `DatabaseNotFound` afterwards.
 *
 * @returns Each step's outcome, or `SmokeFailure` naming the step that failed.
 */
export const smoke = (apiUrl: string) =>
  Effect.gen(function* () {
    const api = yield* HttpApiClient.makeWith(DbHttpApi, {
      baseUrl: apiUrl,
      httpClient: yield* HttpClient.HttpClient,
    });
    // Unique per run: a removed id cannot be created again for gcMinAge (D90).
    const prefix = `smoke-${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
    const main = DatabaseId.make(`${prefix}-rw`);
    const raced = DatabaseId.make(`${prefix}-race`);
    const created: Array<DatabaseId> = [];
    const passed: Array<string> = [];
    const create = (databaseId: DatabaseId) =>
      api.databases.createDatabase({
        payload: {
          databaseId,
          keyRange: Option.none(),
          mergeOperator: Option.none(),
          changelogRetentionMs: Option.none(),
          budgetGroup: Option.none(),
        },
      });

    const checks = Effect.gen(function* () {
      // Recorded before the create: one whose answer is lost may still have created it.
      created.push(main);
      yield* step("create", create(main));
      passed.push("create");

      const receipt = yield* step(
        "write",
        api.databases.write({
          params: { databaseId: main },
          payload: {
            operations: ["a", "b", "c"].map((key) => ({
              _tag: "Put" as const,
              key: bytes(key),
              value: bytes(key.toUpperCase()),
              ttlMillis: Option.none(),
            })),
          },
        }),
      );
      passed.push("write");
      const read = (
        key: string,
        consistency: Parameters<typeof api.databases.get>[0]["payload"]["consistency"],
      ) =>
        step(
          `get ${key} ${consistency._tag}`,
          api.databases
            .get({
              params: { databaseId: main },
              payload: { key: bytes(key), consistency, snapshot: Option.none() },
            })
            .pipe(
              Effect.map((response) =>
                Option.flatMap(response.row, (row) =>
                  row.value._tag === "Value" ? Option.some(text(row.value.bytes)) : Option.none(),
                ),
              ),
            ),
        );
      for (const consistency of [
        { _tag: "Latest" } as const,
        { _tag: "AtLeast", seq: SeqNum.make(receipt.seq) } as const,
      ]) {
        const value = yield* read("b", consistency);
        yield* expect(
          `get b ${consistency._tag}`,
          Option.getOrNull(value) === "B",
          `read ${Option.getOrNull(value)}`,
        );
      }
      passed.push("read Latest, AtLeast");
      yield* step("flush", api.databases.flush({ params: { databaseId: main } }));
      const flushed = yield* read("c", { _tag: "Flushed" });
      yield* expect(
        "get c Flushed",
        Option.getOrNull(flushed) === "C",
        `read ${Option.getOrNull(flushed)}`,
      );
      passed.push("flush, read Flushed");

      const scanned = yield* step(
        "scan",
        api.databases.scan({
          params: { databaseId: main },
          payload: {
            range: { start: new Uint8Array(0), end: Option.none() },
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            limit: 10,
          },
        }),
      );
      yield* expect(
        "scan",
        scanned.rows.map((row) => text(row.key)).join("") === "abc",
        `scanned ${scanned.rows.map((row) => text(row.key)).join(",")}`,
      );
      passed.push("scan");

      const begin = () =>
        step("begin", api.databases.beginTransaction({ params: { databaseId: main } }));
      const first = yield* begin();
      const second = yield* begin();
      const commit = (begun: typeof first, value: string) =>
        api.databases.commitTransaction({
          params: { databaseId: main },
          payload: {
            readSeq: begun.readSeq,
            isolation: "SerializableSnapshot",
            batch: {
              operations: [
                { _tag: "Put", key: bytes("a"), value: bytes(value), ttlMillis: Option.none() },
              ],
            },
            readKeys: [bytes("a")],
            readRanges: [],
          },
        });
      yield* step("commit", commit(first, "A1"));
      const conflict = yield* Effect.flip(commit(second, "A2")).pipe(
        Effect.mapError(
          () => new SmokeFailure({ step: "conflict", message: "the second commit succeeded" }),
        ),
      );
      yield* expect(
        "conflict",
        tagOf(conflict) === "TransactionConflictPublicError",
        tagOf(conflict),
      );
      passed.push("transaction, conflict refused");

      const checkpoint = yield* step(
        "checkpoint",
        api.databases.createCheckpoint({
          params: { databaseId: main },
          payload: { name: Option.some("smoke"), lifetimeMs: Option.some(600_000) },
        }),
      );
      const listed = yield* step(
        "list checkpoints",
        api.databases.listCheckpoints({ params: { databaseId: main } }),
      );
      yield* expect(
        "list checkpoints",
        listed.checkpoints.some((entry) => entry.id === checkpoint.id),
        "the checkpoint is not listed",
      );
      yield* step(
        "delete checkpoint",
        api.databases.deleteCheckpoint({
          params: { databaseId: main, checkpointId: checkpoint.id },
        }),
      );
      passed.push("checkpoint created, listed, deleted");

      created.push(raced);
      const racers = yield* Effect.forEach(
        Array.from({ length: 8 }, (_, index) => index),
        () => Effect.result(create(raced)),
        { concurrency: "unbounded" },
      );
      const winners = racers.filter((result) => result._tag === "Success").length;
      const refused = racers.filter(
        (result) =>
          result._tag === "Failure" && tagOf(result.failure) === "DatabaseAlreadyExistsPublicError",
      ).length;
      yield* expect(
        "create race",
        winners === 1 && refused === 7,
        `${winners} created, ${refused} refused`,
      );
      passed.push("create race: one created, seven refused");
    });

    const removeAll = Effect.forEach(
      created,
      (databaseId) =>
        Effect.ignore(
          step(`remove ${databaseId}`, api.databases.removeDatabase({ params: { databaseId } })),
        ),
      { discard: true },
    );
    // Remove what the smoke test created whatever happened, an interruption included (D104).
    const outcome = yield* Effect.result(checks).pipe(Effect.onInterrupt(() => removeAll));
    yield* removeAll;
    const leftovers: Array<DatabaseId> = [];
    for (const databaseId of created) {
      const after = yield* Effect.result(
        step(`remove ${databaseId}`, api.databases.status({ params: { databaseId } })),
      );
      if (
        after._tag === "Success" ||
        !after.failure.message.includes("DatabaseNotFoundPublicError")
      ) {
        leftovers.push(databaseId);
      }
    }
    if (leftovers.length > 0) {
      return yield* new SmokeFailure({
        step: `remove ${leftovers.join(", ")}`,
        message: "they still answer; remove them with DELETE /v1/databases/<id>",
      });
    }
    passed.push("removed every database it created");
    if (outcome._tag === "Failure") return yield* outcome.failure;
    return { apiUrl, databases: created, passed };
  });
