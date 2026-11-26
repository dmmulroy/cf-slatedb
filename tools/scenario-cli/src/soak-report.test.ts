import { NodeServices } from "@effect/platform-node";
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect, FileSystem, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { soakPassed, type SoakPhase, writeReportFile } from "./soak-report.ts";

const directory = () => mkdtempSync(join(tmpdir(), "soak-report-"));

const run = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
  effect.pipe(Effect.provide(NodeServices.layer), Effect.runPromise);

describe("the report file", () => {
  it("holds the last phase written, and only the report file is left in its directory", async () => {
    const dir = directory();
    const path = join(dir, "soak.json");
    const phases: ReadonlyArray<SoakPhase> = [
      "workload",
      "final-reads",
      "storage",
      "telemetry",
      "done",
    ];
    for (const phase of phases) {
      await run(writeReportFile(path, { phase, passed: phase === "done" }));
      expect(JSON.parse(readFileSync(path, "utf8"))).toEqual({ phase, passed: phase === "done" });
    }
    expect(readdirSync(dir)).toEqual(["soak.json"]);
  });

  it("is replaced by a rename of a complete temporary file, never written in place", async () => {
    const dir = directory();
    const path = join(dir, "soak.json");
    const calls: Array<string> = [];
    await run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const recording = FileSystem.FileSystem.of({
          ...fs,
          writeFileString: (target, data, options) => {
            calls.push(`write ${target === path ? "report" : "temporary"}`);
            return fs.writeFileString(target, data, options);
          },
          rename: (from, to) => {
            calls.push(`rename ${from === path ? "report" : "temporary"} to ${to === path ? "report" : "other"}`);
            expect(JSON.parse(readFileSync(from, "utf8"))).toEqual({ phase: "workload" });
            return fs.rename(from, to);
          },
        });
        yield* writeReportFile(path, { phase: "workload" }).pipe(
          Effect.provideService(FileSystem.FileSystem, recording),
        );
      }),
    );
    expect(calls).toEqual(["write temporary", "rename temporary to report"]);
  });

  it("keeps the previous report when a write is stopped before the rename", async () => {
    const dir = directory();
    const path = join(dir, "soak.json");
    await run(writeReportFile(path, { phase: "workload", checks: { write: 10 } }));
    const exit = await Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dying = FileSystem.FileSystem.of({ ...fs, rename: () => Effect.die("killed") });
      return yield* writeReportFile(path, { phase: "final-reads" }).pipe(
        Effect.provideService(FileSystem.FileSystem, dying),
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.runPromiseExit);
    expect(exit._tag).toBe("Failure");
    expect(JSON.parse(readFileSync(path, "utf8"))).toEqual({
      phase: "workload",
      checks: { write: 10 },
    });
  });
});

describe("the verdict", () => {
  const clean = {
    mismatches: [],
    failures: [],
    unexpectedErrors: [],
    healthIssues: Option.some([]),
  };

  it("passes a clean soak only once it is done", () => {
    expect(soakPassed({ ...clean, phase: "done" })).toBe(true);
    for (const phase of ["workload", "final-reads", "storage", "telemetry"] as const)
      expect(soakPassed({ ...clean, phase })).toBe(false);
  });

  it("fails a soak that could not check its telemetry", () => {
    expect(
      soakPassed({
        ...clean,
        phase: "done",
        failures: ["telemetry: the last tail records were not all read within 10m"],
      }),
    ).toBe(false);
    expect(soakPassed({ ...clean, phase: "done", healthIssues: Option.none() })).toBe(false);
  });
});
