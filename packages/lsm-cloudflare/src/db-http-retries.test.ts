import { DatabaseId, DEFAULT_DATABASE_OPTIONS } from "@shaledb/lsm-core";
import { Effect, Exit, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { DbClient } from "./db-client.ts";
import type { DbApiOperation } from "./db-public-error.ts";
import { type DbInternalError, toPublicError } from "./db-public-error-translation.ts";
import { DatabaseNotFoundError, WriterUnavailableError } from "./lsm-errors.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { WriterClient } from "./writer-client.ts";

const ID = DatabaseId.make("retry");
const put = (key: number) => ({
  operations: [
    {
      _tag: "Put" as const,
      key: Uint8Array.of(key),
      value: Uint8Array.of(1),
      ttlMillis: Option.none(),
    },
  ],
});

/** The public answer (code and message) a caller would get for an exit of `operation`. */
const answer =
  (operation: DbApiOperation) =>
  <A>(exit: Exit.Exit<A, DbInternalError>) =>
    Exit.isSuccess(exit)
      ? "ok"
      : Exit.findErrorOption(exit).pipe(
          Option.map((error) => {
            const translated = toPublicError(operation, "r")(error);
            return `${translated.code}: ${translated.message}`;
          }),
          Option.getOrElse(() => String(exit)),
        );

describe("what a retried call returns (D88)", () => {
  it("answers each retry truthfully: idempotent calls succeed again, the others say a completed first attempt answers this way", async () => {
    const out = await Effect.gen(function* () {
      const lifecycle = yield* DatabaseLifecycle;
      const writer = yield* WriterClient;
      const client = yield* DbClient;
      const create = () =>
        lifecycle.create(ID, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
      const results: Record<string, string> = {};
      results["create"] = answer("createDatabase")(yield* Effect.exit(create()));
      results["create again"] = answer("createDatabase")(yield* Effect.exit(create()));
      results["write"] = answer("write")(yield* Effect.exit(writer.write(ID, put(1))));
      results["flush"] = answer("flush")(yield* Effect.exit(writer.flush(ID)));
      results["flush again"] = answer("flush")(yield* Effect.exit(writer.flush(ID)));
      const begin = yield* client.beginTransaction(ID);
      const commit = () =>
        writer.commitTransaction(ID, {
          readSeq: begin.readSeq,
          isolation: "Snapshot",
          batch: put(2),
          readKeys: [],
          readRanges: [],
        });
      results["commit"] = answer("transaction")(yield* Effect.exit(commit()));
      results["commit again"] = answer("transaction")(yield* Effect.exit(commit()));
      const a = DatabaseId.make("retry-a");
      const b = DatabaseId.make("retry-b");
      results["seal"] = answer("seal")(yield* Effect.exit(lifecycle.seal(ID, [a, b], false)));
      results["seal again"] = answer("seal")(yield* Effect.exit(lifecycle.seal(ID, [a, b], false)));
      results["remove"] = answer("remove")(yield* Effect.exit(lifecycle.remove(ID)));
      results["remove again"] = answer("remove")(yield* Effect.exit(lifecycle.remove(ID)));
      return results;
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(out["create"]).toBe("ok");
    expect(out["create again"]).toContain("A retried create whose first attempt completed");
    expect(out["flush again"]).toBe("ok");
    expect(out["commit"]).toBe("ok");
    expect(out["commit again"]).toMatch(/^TransactionConflict: /);
    expect(out["commit again"]).toContain("it may have committed: read its keys");
    expect(out["seal again"]).toBe("ok");
    expect(out["remove"]).toBe("ok");
    // While the removal's tombstone stands (gcMinAge, D91), a retried remove succeeds again.
    expect(out["remove again"]).toBe("ok");
  });

  it("tells a caller removing a database that is gone that it is already removed", () => {
    // After the tombstone is collected, the database no longer exists for a retried remove.
    const gone = toPublicError(
      "remove",
      "r",
    )(new DatabaseNotFoundError({ databaseId: ID, message: `Database ${ID} does not exist.` }));
    expect(gone.code).toBe("DatabaseNotFound");
    expect(gone.message).toContain("already removed");
    expect(gone.message).not.toContain("Create it first");
  });
});

describe("a write whose answer was lost (D88)", () => {
  it("says the write may have committed, and that merges may apply twice", () => {
    const unreachable = toPublicError(
      "write",
      "r",
    )(
      new WriterUnavailableError({
        databaseId: ID,
        operation: "write",
        reason: "WriterUnreachable",
        message: "internal",
      }),
    );
    expect(unreachable.retryable).toBe(true);
    expect(unreachable.message).toContain("may or may not have committed");
    expect(unreachable.message).toContain("merges may then apply twice");
    const storage = toPublicError(
      "write",
      "r",
    )(
      new WriterUnavailableError({
        databaseId: ID,
        operation: "write",
        reason: "StorageUnavailable",
        message: "internal",
      }),
    );
    expect(storage.message).toContain("Retry the same request");
  });
});
