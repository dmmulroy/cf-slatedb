import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { runNodeProcess } from "./scenario.ts";

const isRunning = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

describe("child processes", () => {
  it("returns a finished process's status and output", async () => {
    const result = await Effect.runPromise(
      runNodeProcess(["-e", "process.stdout.write('spans'); process.exit(3)"], true),
    );
    expect(result).toEqual({ status: 3, stdout: "spans", stderr: "" });
  });

  it("kills a process that never ends when interrupted, so a time budget stops it", async () => {
    const pidFile = join(mkdtempSync(join(tmpdir(), "scenario-process-")), "pid");
    const started = Date.now();
    const result = await Effect.runPromise(
      runNodeProcess(
        [
          "-e",
          `require("node:fs").writeFileSync(${JSON.stringify(pidFile)}, String(process.pid)); setInterval(() => {}, 1000)`,
        ],
        false,
      ).pipe(Effect.timeoutOption("1 second")),
    );
    expect(result._tag).toBe("None");
    expect(Date.now() - started).toBeLessThan(4_000);
    expect(existsSync(pidFile)).toBe(true);
    // The interruption waited for the process to exit.
    expect(isRunning(Number(readFileSync(pidFile, "utf8")))).toBe(false);
  });
});
