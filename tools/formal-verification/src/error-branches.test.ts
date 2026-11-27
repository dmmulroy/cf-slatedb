import { describe, expect, it } from "vite-plus/test";

import { checkErrorBranches, errorBranches, functionText, matrixRows } from "./error-branches.ts";

describe("error branches in model-covered code (D137)", () => {
  it("every branch has a row in docs/coverage-matrix.md naming a model action or a reason, and every row a branch", () => {
    const { unmapped, stale, empty, missingFunctions } = checkErrorBranches();
    expect({
      missingFunctions,
      unmapped: unmapped.map((branch) => `${branch.file} ${branch.name}: ${branch.branch}`),
      stale,
      empty,
    }).toEqual({ missingFunctions: [], unmapped: [], stale: [], empty: [] });
  });

  it("finds a function's branches: handled tags, constructed errors, failure helpers, and catch-alls", () => {
    const source = `
const other = Effect.fail(new UnrelatedError({}));

const forkRange = Effect.fn("DatabaseLifecycle.forkRange")(function* (request: Request) {
  // new CommentedOutError( is not a branch
  const head = yield* reader.findLatest(forkId).pipe(
    Effect.catchTags({ ManifestReadError: unavailable(forkId, "fork"), ...updateFailures(source) }),
  );
  if (head._tag === "Missing") return yield* checkpointNotFound(source, id);
  yield* Effect.ignore(background.run("warm", warm));
  return yield* initialize(fork).pipe(
    Effect.catchTag("DatabaseAlreadyExistsError", () => Effect.fail(new ForkTakenError({}))),
  );
});

const after = 1;
`;
    const text = functionText(source, "forkRange");
    expect(text).toBeDefined();
    expect(errorBranches(text ?? "")).toEqual([
      "catch DatabaseAlreadyExistsError",
      "catch ManifestReadError",
      "fail ForkTakenError",
      "ignore failures",
      "via checkpointNotFound",
      "via unavailable",
      "via updateFailures",
    ]);
  });

  it("reads an object property's extent up to the property that follows it", () => {
    const source = `
return MemTable.of({
    trim: Effect.fn("MemTable.trim")(function* () {
      yield* meta.pipe(Effect.orDie);
    }),
    stats: Effect.fail(new LaterError({})),
});`;
    expect(errorBranches(functionText(source, "trim") ?? "")).toEqual(["die on failure"]);
  });

  it("reads the matrix's rows between its markers", () => {
    const rows = matrixRows(`
<!-- error-branches:start -->
| Code | Branch | Model action, or why not modeled |
| --- | --- | --- |
| \`lsm-cloudflare/src/a.ts\` \`f\` | \`catch XError\` | Model \`Step\` |
<!-- error-branches:end -->
| \`lsm-cloudflare/src/b.ts\` \`g\` | \`catch YError\` | outside the markers |
`);
    expect([...rows]).toEqual([["lsm-cloudflare/src/a.ts#f|catch XError", "Model `Step`"]]);
  });
});
