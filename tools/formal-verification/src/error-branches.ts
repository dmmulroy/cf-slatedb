/**
 * Lists the error branches of the code the TLA+ models cover, and checks that each has a row in
 * `docs/coverage-matrix.md` ("Error branches in model-covered code") naming the model action it
 * maps to, or why no model covers it (D137).
 *
 * Usage: `node tools/formal-verification/src/error-branches.ts` prints every branch with no row
 * and every row with no branch, as rows ready to fill in, and exits 1 when there are any. The
 * workspace's test runs the same check, so `vp run ready` fails on an unmapped branch.
 *
 * A branch is found by text, not by a type checker: inside each covered function, every tag an
 * `Effect.catchTag`/`catchTags` handles, every `new …Error(` it constructs, and every call of a
 * helper whose name says it produces or translates a failure (`…NotFound`, `…Unavailable`,
 * `…Failures`, …). A new branch of any of these shapes appears here without anyone listing it.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..", "..", "..");

/** A function a TLA+ model describes, and the models (verify skills) that describe it. */
export interface ModelCoveredFunction {
  /** Path from the repository root. */
  readonly file: string;
  /** Its name: a `const`, an object property, or a `function`. */
  readonly name: string;
  /** The `verify-*` skills whose models describe it. */
  readonly models: ReadonlyArray<string>;
}

const cloudflare = (file: string) => `packages/lsm-cloudflare/src/${file}`;
const core = (file: string) => `packages/lsm-core/src/${file}`;
const sharding = (file: string) => `packages/lsm-sharding/src/${file}`;

/**
 * Every function a model covers, from each verify skill's "use after any change to" list. Add a
 * function here when a model starts to describe it.
 */
export const MODEL_COVERED_FUNCTIONS: ReadonlyArray<ModelCoveredFunction> = [
  // DbClient
  { file: cloudflare("db-client.ts"), name: "changes", models: ["change-feed"] },
  { file: cloudflare("db-client.ts"), name: "scanUnflushed", models: ["paged-scan"] },
  { file: cloudflare("db-client.ts"), name: "scanWithBudget", models: ["paged-scan"] },
  { file: cloudflare("db-client.ts"), name: "scanAtPage", models: ["paged-scan", "transactions"] },
  { file: cloudflare("db-client.ts"), name: "beginTransaction", models: ["transactions"] },
  { file: cloudflare("db-client.ts"), name: "getAt", models: ["transactions"] },
  { file: cloudflare("db-client.ts"), name: "scanAt", models: ["transactions"] },
  { file: cloudflare("db-client.ts"), name: "transaction", models: ["transactions"] },
  { file: cloudflare("db-client.ts"), name: "pointView", models: ["no-acked-write-lost"] },
  // DbReader
  { file: cloudflare("db-reader.ts"), name: "changelogRows", models: ["change-feed"] },
  { file: cloudflare("db-reader.ts"), name: "manifestFor", models: ["transactions", "paged-scan"] },
  { file: cloudflare("db-reader.ts"), name: "getRow", models: ["transactions", "paged-scan"] },
  { file: cloudflare("db-reader.ts"), name: "scan", models: ["paged-scan"] },
  // DbWriter
  {
    file: cloudflare("db-writer.ts"),
    name: "ensureReady",
    models: ["writer-object", "no-acked-write-lost"],
  },
  { file: cloudflare("db-writer.ts"), name: "startWriter", models: ["writer-object"] },
  { file: cloudflare("db-writer.ts"), name: "commitBatch", models: ["writer-object"] },
  {
    file: cloudflare("db-writer.ts"),
    name: "write",
    models: ["no-acked-write-lost", "seal-split"],
  },
  {
    file: cloudflare("db-writer.ts"),
    name: "commitTransaction",
    models: ["writer-object", "transactions", "paged-scan"],
  },
  { file: cloudflare("db-writer.ts"), name: "flushThen", models: ["writer-object", "change-feed"] },
  { file: cloudflare("db-writer.ts"), name: "flushNow", models: ["no-acked-write-lost"] },
  { file: cloudflare("db-writer.ts"), name: "refreshUnflushedCounters", models: ["writer-object"] },
  { file: cloudflare("db-writer.ts"), name: "scheduleFlush", models: ["writer-object"] },
  {
    file: cloudflare("db-writer.ts"),
    name: "seal",
    models: ["writer-object", "seal-split", "shard-split"],
  },
  { file: cloudflare("db-writer.ts"), name: "runAlarm", models: ["writer-object"] },
  { file: cloudflare("db-writer.ts"), name: "markFenced", models: ["writer-object"] },
  { file: cloudflare("db-writer.ts"), name: "probe", models: ["no-acked-write-lost"] },
  { file: cloudflare("db-writer.ts"), name: "scan", models: ["no-acked-write-lost"] },
  // MemTable
  { file: cloudflare("mem-table.ts"), name: "commit", models: ["no-acked-write-lost"] },
  { file: cloudflare("mem-table.ts"), name: "consistently", models: ["no-acked-write-lost"] },
  { file: cloudflare("mem-table.ts"), name: "markFlushed", models: ["writer-object"] },
  { file: cloudflare("mem-table.ts"), name: "trim", models: ["writer-object"] },
  { file: cloudflare("mem-table.ts"), name: "stats", models: ["writer-object"] },
  { file: cloudflare("mem-table.ts"), name: "writtenAfter", models: ["transactions"] },
  // Compactor
  { file: cloudflare("compactor.ts"), name: "updateState", models: ["compactor-object"] },
  { file: cloudflare("compactor.ts"), name: "ensureState", models: ["compactor-object"] },
  { file: cloudflare("compactor.ts"), name: "poll", models: ["compactor-object"] },
  { file: cloudflare("compactor.ts"), name: "runCollection", models: ["compactor-object"] },
  { file: cloudflare("compactor.ts"), name: "runAlarm", models: ["compactor-object"] },
  { file: cloudflare("compactor.ts"), name: "dropSst", models: ["compactor-object"] },
  { file: cloudflare("compaction-workflow.ts"), name: "start", models: ["compactor-object"] },
  { file: cloudflare("compaction-workflow.ts"), name: "progress", models: ["compactor-object"] },
  // GC
  {
    file: cloudflare("garbage-collector.ts"),
    name: "collect",
    models: ["gc-safety", "compactor-object", "lifecycle"],
  },
  // Manifests
  // ManifestStore.update and updateFrom are commitEdit; create is attemptCreate.
  { file: cloudflare("manifest-store.ts"), name: "commitEdit", models: ["manifest-cas-fencing"] },
  {
    file: cloudflare("manifest-store.ts"),
    name: "attemptCreate",
    models: ["manifest-cas-fencing"],
  },
  {
    file: cloudflare("manifest-store.ts"),
    name: "initialize",
    models: ["manifest-cas-fencing", "lifecycle"],
  },
  // ManifestReader.loadLatest is newestFromStorage, which reads the hint and probes forward.
  {
    file: cloudflare("manifest-reader.ts"),
    name: "newestFromStorage",
    models: ["manifest-cas-fencing"],
  },
  {
    file: cloudflare("manifest-reader.ts"),
    name: "newestFromHint",
    models: ["manifest-cas-fencing"],
  },
  {
    file: cloudflare("manifest-reader.ts"),
    name: "probeForward",
    models: ["manifest-cas-fencing"],
  },
  {
    file: cloudflare("manifest-reader.ts"),
    name: "findLatest",
    models: ["manifest-cas-fencing", "lifecycle"],
  },
  { file: cloudflare("object-store.ts"), name: "putIfAbsent", models: ["manifest-cas-fencing"] },
  // Lifecycle
  { file: cloudflare("database-lifecycle.ts"), name: "create", models: ["lifecycle"] },
  { file: cloudflare("database-lifecycle.ts"), name: "remove", models: ["lifecycle"] },
  { file: cloudflare("database-lifecycle.ts"), name: "removeCheckpointFor", models: ["lifecycle"] },
  { file: cloudflare("database-lifecycle.ts"), name: "deleteCheckpoint", models: ["lifecycle"] },
  { file: cloudflare("database-lifecycle.ts"), name: "seal", models: ["seal-split"] },
  { file: cloudflare("database-lifecycle.ts"), name: "createCheckpoint", models: ["seal-split"] },
  {
    file: cloudflare("database-lifecycle.ts"),
    name: "acquireForkHold",
    models: ["lifecycle", "shard-split"],
  },
  {
    file: cloudflare("database-lifecycle.ts"),
    name: "forkRange",
    models: ["lifecycle", "seal-split", "shard-split"],
  },
  { file: cloudflare("database-lifecycle.ts"), name: "exists", models: ["shard-split"] },
  // Pure manifest edits
  {
    file: core("manifest.ts"),
    name: "commitL0Flush",
    models: ["manifest-cas-fencing", "gc-safety", "change-feed"],
  },
  {
    file: core("manifest.ts"),
    name: "commitCompaction",
    models: ["manifest-cas-fencing", "gc-safety", "compactor-object"],
  },
  { file: core("manifest.ts"), name: "fenceWriter", models: ["manifest-cas-fencing"] },
  { file: core("manifest.ts"), name: "fenceCompactor", models: ["manifest-cas-fencing"] },
  { file: core("manifest.ts"), name: "forkManifest", models: ["lifecycle", "seal-split"] },
  { file: core("manifest.ts"), name: "releasableExternalRefs", models: ["lifecycle"] },
  { file: core("manifest.ts"), name: "pruneChangelog", models: ["change-feed"] },
  { file: core("manifest.ts"), name: "changelogHoldsChangesAfter", models: ["change-feed"] },
  // Sharding
  { file: sharding("shard-split.ts"), name: "split", models: ["shard-split", "seal-split"] },
  { file: sharding("shard-split.ts"), name: "resumePending", models: ["shard-split"] },
  { file: sharding("shard-router.ts"), name: "refresh", models: ["shard-split"] },
  { file: sharding("shard-router.ts"), name: "refreshForUnprobedRead", models: ["shard-split"] },
  { file: sharding("shard-router.ts"), name: "withRouting", models: ["shard-split"] },
];

/** `source` with comments blanked (same length, so offsets hold) and strings kept. */
const withoutComments = (source: string): string => {
  let out = "";
  let index = 0;
  while (index < source.length) {
    const two = source.slice(index, index + 2);
    const char = source[index] ?? "";
    if (two === "//") {
      const end = source.indexOf("\n", index);
      const stop = end === -1 ? source.length : end;
      out += " ".repeat(stop - index);
      index = stop;
    } else if (two === "/*") {
      const end = source.indexOf("*/", index + 2);
      const stop = end === -1 ? source.length : end + 2;
      out += source.slice(index, stop).replace(/[^\n]/g, " ");
      index = stop;
    } else if (char === '"' || char === "'" || char === "`") {
      const stop = stringEnd(source, index);
      out += source.slice(index, stop);
      index = stop;
    } else {
      out += char;
      index++;
    }
  }
  return out;
};

/** The index just past the string literal opening at `start` (template `${}` nesting included). */
const stringEnd = (source: string, start: number): number => {
  const quote = source[start];
  let index = start + 1;
  while (index < source.length) {
    const char = source[index];
    if (char === "\\") {
      index += 2;
      continue;
    }
    if (char === quote) return index + 1;
    if (quote === "`" && char === "$" && source[index + 1] === "{") {
      index = expressionEnd(source, index + 2, "}") + 1;
      continue;
    }
    index++;
  }
  return index;
};

/** The index of the `close` that ends an expression starting at `start` (depth 0). */
const expressionEnd = (source: string, start: number, close: string): number => {
  let depth = 0;
  let index = start;
  while (index < source.length) {
    const char = source[index] ?? "";
    if (char === '"' || char === "'" || char === "`") {
      index = stringEnd(source, index);
      continue;
    }
    if (char === "(" || char === "{" || char === "[") depth++;
    else if (char === ")" || char === "}" || char === "]") {
      if (depth === 0 && char === close) return index;
      if (depth === 0) return index;
      depth--;
    } else if (depth === 0 && close.includes(char)) return index;
    index++;
  }
  return index;
};

/** The text of `name`'s declaration in `source` (comments blanked), or undefined. */
export const functionText = (source: string, name: string): string | undefined => {
  const text = withoutComments(source);
  const patterns = [
    new RegExp(`\\bconst ${name}\\s*=`),
    new RegExp(`\\bfunction\\*?\\s+${name}\\s*\\(`),
    new RegExp(`\\n\\s+${name}\\s*:\\s*(?:Effect\\.fn|\\(|<|function|[a-zA-Z_.]+\\()`),
  ];
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match === null) continue;
    const start = match.index + match[0].length;
    // A const ends at its `;`, an object property at the `,` or `}` that closes it.
    const end = expressionEnd(text, start, match[0].startsWith("\n") ? ",}" : ";");
    return text.slice(match.index, end);
  }
  return undefined;
};

/** Helper names that produce or translate a failure. */
const FAILURE_HELPER =
  /\b((?:[a-z]\w*(?:NotFound|TooOld|Unavailable|Failures|Failure|Sealed|Fenced|Refused|Expired|Conflict|Missing|Removed)|unavailable|notFound|tooOld|conflict|refused)\w*)\(/g;

/** The error branches in one function's text: `catch Tag`, `fail ErrorClass`, `via helper`. */
export const errorBranches = (text: string): ReadonlyArray<string> => {
  const found = new Set<string>();
  for (const match of text.matchAll(/catchTags?\(\s*/g)) {
    const at = (match.index ?? 0) + match[0].length;
    if (text[at] === '"') {
      const tag = /^"(\w+)"/.exec(text.slice(at));
      if (tag?.[1] !== undefined) found.add(`catch ${tag[1]}`);
    } else if (text[at] === "{") {
      const end = expressionEnd(text, at + 1, "}");
      const body = text.slice(at + 1, end);
      // Keys at the object's top level only.
      let depth = 0;
      let line = "";
      for (const char of body + ",") {
        if (char === "(" || char === "{" || char === "[") depth++;
        if (char === ")" || char === "}" || char === "]") depth--;
        if (depth === 0 && char === ",") {
          const key = /^\s*(\w+)\s*:/.exec(line);
          if (key?.[1] !== undefined) found.add(`catch ${key[1]}`);
          const spread = /^\s*\.\.\.(\w+)/.exec(line);
          if (spread?.[1] !== undefined) found.add(`via ${spread[1]}`);
          line = "";
        } else line += char;
      }
    }
  }
  for (const match of text.matchAll(/_tag\s*[!=]==?\s*"(\w+Error)"/g)) {
    if (match[1] !== undefined) found.add(`catch ${match[1]}`);
  }
  // Handlers of every failure, and ways a failure ends the fiber or is retried.
  const general: ReadonlyArray<readonly [RegExp, string]> = [
    [/\.catch\(|Effect\.catch\(/, "catch any failure"],
    [/catchCause\(/, "catch any cause"],
    [/Effect\.ignore\b|\.ignore\(/, "ignore failures"],
    [/tapError\(|tapCause\(/, "observe failure"],
    [/orDie\b/, "die on failure"],
    [/Effect\.die\(/, "die"],
    [/Effect\.retry\(/, "retry"],
    [/Effect\.result\(|Effect\.exit\(/, "inspect failure"],
    [/matchEffect\(|Effect\.match\(/, "match failure"],
  ];
  for (const [pattern, branch] of general) if (pattern.test(text)) found.add(branch);
  for (const match of text.matchAll(/\bnew (\w+Error)\(/g)) {
    if (match[1] !== undefined) found.add(`fail ${match[1]}`);
  }
  for (const match of text.matchAll(FAILURE_HELPER)) {
    if (match[1] !== undefined) found.add(`via ${match[1]}`);
  }
  return [...found].sort();
};

/** One error branch: where it is and what it is. */
export interface ErrorBranch {
  readonly file: string;
  readonly name: string;
  readonly branch: string;
  readonly models: ReadonlyArray<string>;
}

/** Every error branch in the model-covered functions, or the functions that could not be found. */
export const scanErrorBranches = (): {
  readonly branches: ReadonlyArray<ErrorBranch>;
  readonly missingFunctions: ReadonlyArray<string>;
} => {
  const branches: Array<ErrorBranch> = [];
  const missingFunctions: Array<string> = [];
  for (const covered of MODEL_COVERED_FUNCTIONS) {
    const source = readFileSync(resolve(ROOT, covered.file), "utf8");
    const text = functionText(source, covered.name);
    if (text === undefined) {
      missingFunctions.push(`${covered.file} ${covered.name}`);
      continue;
    }
    for (const branch of errorBranches(text)) {
      branches.push({ file: covered.file, name: covered.name, branch, models: covered.models });
    }
  }
  return { branches, missingFunctions };
};

const MATRIX = resolve(ROOT, "docs", "coverage-matrix.md");
const START = "<!-- error-branches:start -->";
const END = "<!-- error-branches:end -->";

/** A row's key: the function and the branch. */
const keyOf = (file: string, name: string, branch: string) =>
  `${file.replace(/^packages\//, "")}#${name}|${branch}`;

/** The rows of the matrix's error-branch table, by key, with what each says. */
export const matrixRows = (matrix: string): ReadonlyMap<string, string> => {
  const start = matrix.indexOf(START);
  const end = matrix.indexOf(END);
  const rows = new Map<string, string>();
  if (start === -1 || end === -1) return rows;
  for (const line of matrix.slice(start, end).split("\n")) {
    const cells = line.split("|").map((cell) => cell.trim());
    // | `file` `name` | `branch` | mapping |
    const where = /^`([^`]+)` `([^`]+)`$/.exec(cells[1] ?? "");
    const branch = /^`([^`]+)`$/.exec(cells[2] ?? "");
    if (where?.[1] === undefined || where[2] === undefined || branch?.[1] === undefined) continue;
    rows.set(`${where[1]}#${where[2]}|${branch[1]}`, cells[3] ?? "");
  }
  return rows;
};

/** The branches with no row, the rows with no branch, and rows that map to nothing. */
export const checkErrorBranches = () => {
  const { branches, missingFunctions } = scanErrorBranches();
  const rows = matrixRows(readFileSync(MATRIX, "utf8"));
  const keys = new Set(branches.map((b) => keyOf(b.file, b.name, b.branch)));
  const unmapped = branches.filter((b) => !rows.has(keyOf(b.file, b.name, b.branch)));
  const stale = [...rows.keys()].filter((key) => !keys.has(key));
  const empty = [...rows].filter(([, mapping]) => mapping === "").map(([key]) => key);
  return { unmapped, stale, empty, missingFunctions };
};

/** A table row for a branch, to fill in. */
export const rowFor = (branch: ErrorBranch): string =>
  `| \`${branch.file.replace(/^packages\//, "")}\` \`${branch.name}\` | \`${branch.branch}\` | TODO (${branch.models.join(", ")}) |`;

if (import.meta.main) {
  const { unmapped, stale, empty, missingFunctions } = checkErrorBranches();
  for (const missing of missingFunctions) console.log(`function not found: ${missing}`);
  for (const branch of unmapped) console.log(rowFor(branch));
  for (const key of stale) console.log(`stale row (no such branch): ${key}`);
  for (const key of empty) console.log(`row maps to nothing: ${key}`);
  const problems = unmapped.length + stale.length + empty.length + missingFunctions.length;
  if (problems > 0) {
    console.log(
      `\n${problems} problem(s). Add each branch to "Error branches in model-covered code" in docs/coverage-matrix.md with the model action it maps to, or why no model covers it; remove stale rows.`,
    );
    process.exit(1);
  }
  console.log("Every error branch in model-covered code maps to a model action or a reason.");
}
