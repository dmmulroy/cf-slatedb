/**
 * Runs every formal check with TLC and compares each run to its expected outcome.
 *
 * Usage: `node tools/formal-verification/src/verify.ts [check-name ...] [--json]`
 * (`vp run verify` runs all of them).
 *
 * The first run downloads a pinned JDK (Temurin 21) and `tla2tools.jar` 1.7.4 into `.tlc/`
 * (gitignored) and checks their SHA-256 digests. Set `JAVA` and `TLA2TOOLS` to use existing ones.
 * Every TLC run has a hard time limit, so an oversized bound fails fast instead of hanging.
 * `TLC_WORKERS` sets TLC's worker threads (default `auto`, one per core).
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { FORMAL_CHECKS, type FormalCheck, type FormalCheckRun } from "./formal-checks.ts";

const ROOT = resolve(import.meta.dirname, "..", "..", "..");
const SPECS = resolve(import.meta.dirname, "..", "specs");
const TOOL_CACHE = join(ROOT, ".tlc");

const JDK = {
  url: "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13.1%2B2/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13.1_2.tar.gz",
  sha256: "48067f2b7c958ed104a9d8a5749be7d25220b0a356b4d2d1bcfaab3e16ad0b14",
};
const TLA2TOOLS = {
  url: "https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar",
  sha256: "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88",
};

const sha256 = (path: string) => createHash("sha256").update(readFileSync(path)).digest("hex");

const download = (url: string, path: string, digest: string) => {
  if (existsSync(path) && sha256(path) === digest) return;
  const result = spawnSync("curl", ["-sSfL", "-o", path, url], { stdio: "inherit" });
  if (result.status !== 0 || sha256(path) !== digest) {
    throw new Error(
      `Downloading ${url} failed or its SHA-256 did not match ${digest}. Check the network and retry.`,
    );
  }
};

/** Resolves `java` and `tla2tools.jar`, downloading the pinned versions when needed. */
const resolveTools = (): { readonly java: string; readonly jar: string } => {
  const envJava = process.env["JAVA"];
  const envJar = process.env["TLA2TOOLS"];
  if (envJava !== undefined && envJar !== undefined) return { java: envJava, jar: envJar };
  const downloads = join(TOOL_CACHE, "downloads");
  mkdirSync(downloads, { recursive: true });
  const jar = join(downloads, "tla2tools.jar");
  download(TLA2TOOLS.url, jar, TLA2TOOLS.sha256);
  const jdkDir = join(TOOL_CACHE, "jdk");
  const findJava = () =>
    existsSync(jdkDir)
      ? readdirSync(jdkDir)
          .map((name) => join(jdkDir, name, "Contents", "Home", "bin", "java"))
          .find((candidate) => existsSync(candidate))
      : undefined;
  let java = envJava ?? findJava();
  if (java === undefined) {
    if (process.platform !== "darwin" || process.arch !== "arm64") {
      throw new Error("No pinned JDK for this platform. Set JAVA to a Java 11+ binary and rerun.");
    }
    const archive = join(downloads, "jdk.tar.gz");
    download(JDK.url, archive, JDK.sha256);
    mkdirSync(jdkDir, { recursive: true });
    spawnSync("tar", ["-xzf", archive, "-C", jdkDir], { stdio: "inherit" });
    java = findJava();
    if (java === undefined)
      throw new Error(`The JDK archive did not unpack a java binary under ${jdkDir}.`);
  }
  return { java, jar: envJar ?? jar };
};

/** Outcome of one TLC run. */
interface RunOutcome {
  readonly check: string;
  readonly config: string;
  readonly broken: string;
  readonly expected: string;
  readonly actual: string;
  readonly ok: boolean;
  readonly distinctStates: number | null;
  readonly seconds: number;
}

/** The first lines that explain a TLC failure: semantic errors, parse errors, or `Error` lines. */
const tlcErrorSummary = (output: string): string => {
  const lines = output.split("\n").map((line) => line.trim());
  const detail = lines.filter(
    (line) =>
      line.includes("already defined") ||
      line.includes("Unknown operator") ||
      line.startsWith("Encountered") ||
      line.startsWith("line ") ||
      line.startsWith("Error"),
  );
  return detail.slice(0, 4).join(" | ") || "no result";
};

const runTlc = (
  tools: { readonly java: string; readonly jar: string },
  check: FormalCheck,
  run: FormalCheckRun,
): Promise<RunOutcome> =>
  new Promise((done) => {
    const work = mkdtempSync(join(tmpdir(), `tlc-${check.name}-`));
    copyFileSync(join(SPECS, check.spec), join(work, check.spec));
    const config = readFileSync(join(SPECS, run.config), "utf8").replace(
      /Broken = "[^"]*"/,
      `Broken = "${run.broken}"`,
    );
    writeFileSync(join(work, "run.cfg"), config);
    const started = Date.now();
    const child = spawn(
      tools.java,
      [
        "-XX:+UseParallelGC",
        "-cp",
        tools.jar,
        "tlc2.TLC",
        "-workers",
        process.env["TLC_WORKERS"] ?? "auto",
        "-config",
        "run.cfg",
        check.spec,
      ],
      { cwd: work },
    );
    let output = "";
    child.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
    child.stderr.on("data", (chunk: Buffer) => (output += chunk.toString()));
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, run.timeLimitSeconds * 1000);
    child.on("close", () => {
      clearTimeout(timer);
      // TLC names a violated invariant, but reports a liveness failure only as "Temporal
      // properties were violated": that is the config's one property (a config whose broken run
      // must fail a liveness property lists exactly one).
      const properties = /PROPERTIES?\s+((?:\s*\w+)+?)\s*(?:\n[A-Z_]+|$)/.exec(config)?.[1];
      const singleProperty =
        properties?.trim().split(/\s+/).length === 1 ? properties.trim() : undefined;
      const violated =
        /(?:Invariant|Temporal property|Action property) (\w+) is violated/.exec(output)?.[1] ??
        (output.includes("Temporal properties were violated")
          ? (singleProperty ?? "an unnamed temporal property")
          : undefined);
      const passed = output.includes("Model checking completed. No error has been found.");
      const states = /(\d[\d,]*) distinct states found/.exec(
        output.split("\n").reverse().join("\n"),
      )?.[1];
      const actual = timedOut
        ? `timed out after ${run.timeLimitSeconds}s`
        : violated !== undefined
          ? violated
          : passed
            ? "pass"
            : `TLC error: ${tlcErrorSummary(output)}`;
      if (actual !== run.expect) writeFileSync(join(work, "tlc.out"), output);
      else rmSync(work, { recursive: true, force: true });
      done({
        check: check.name,
        config: run.config,
        broken: run.broken,
        expected: run.expect,
        actual,
        ok: actual === run.expect,
        distinctStates: states === undefined ? null : Number(states.replaceAll(",", "")),
        seconds: Math.round((Date.now() - started) / 100) / 10,
      });
    });
  });

const main = async () => {
  const args = process.argv.slice(2);
  const json = args.includes("--json");
  const names = args.filter((arg) => !arg.startsWith("--"));
  const checks = FORMAL_CHECKS.filter((check) => names.length === 0 || names.includes(check.name));
  const tools = resolveTools();
  const outcomes: Array<RunOutcome> = [];
  for (const check of checks) {
    for (const run of check.runs) {
      const outcome = await runTlc(tools, check, run);
      outcomes.push(outcome);
      if (!json) {
        console.log(
          `${outcome.ok ? "✓" : "✗"} ${check.name} ${run.config} Broken=${run.broken}: expected ${run.expect}, got ${outcome.actual}` +
            `${outcome.distinctStates === null ? "" : ` (${outcome.distinctStates} states)`} in ${outcome.seconds}s`,
        );
      }
    }
  }
  if (json) console.log(JSON.stringify(outcomes, null, 2));
  const failed = outcomes.filter((outcome) => !outcome.ok);
  if (failed.length > 0) {
    console.error(
      `${failed.length} formal check run(s) did not have the expected outcome; TLC output is kept in the temp directory for each.`,
    );
    process.exit(1);
  }
};

await main();
