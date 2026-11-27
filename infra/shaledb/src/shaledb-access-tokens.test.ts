import { Effect, Fiber, Option, Redacted, Ref } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import shaledbViteConfig from "../vite.config.ts";
import {
  awaitSecretActive,
  inactiveSecretsFailure,
  missingAccessTokenSlots,
  SECRET_ACTIVE_POLL,
} from "./access-token-store.ts";
import { accessTokenSecretName, isShaledbPreviewStage } from "./shaledb-access-tokens.ts";
import { deploysTelemetryProbe } from "./telemetry-probe-worker.ts";

describe("access tokens and stage-only resources (D62, D64)", () => {
  it("names one Secrets Store secret per stage and slot", () => {
    expect(accessTokenSecretName("production", "A")).toBe("SHALEDB_PRODUCTION_ACCESS_TOKEN_A");
    expect(accessTokenSecretName("preview-1", "B")).toBe("SHALEDB_PREVIEW_1_ACCESS_TOKEN_B");
  });

  it("keeps the state-held token and the probe Worker out of production", () => {
    expect(isShaledbPreviewStage("production")).toBe(false);
    expect(isShaledbPreviewStage("preview-2")).toBe(true);
    expect(deploysTelemetryProbe("production")).toBe(false);
    expect(deploysTelemetryProbe("staging")).toBe(false);
    expect(deploysTelemetryProbe("preview-3")).toBe(true);
    expect(deploysTelemetryProbe("dev")).toBe(true);
  });
});

describe("the production token check (D104)", () => {
  it("names the slots whose own secret is missing, never satisfied by another stage's", () => {
    const production = (slot: "A" | "B") => accessTokenSecretName("production", slot);
    expect(missingAccessTokenSlots("production", new Set())).toEqual(["A", "B"]);
    expect(missingAccessTokenSlots("production", new Set([production("A")]))).toEqual(["B"]);
    expect(
      missingAccessTokenSlots("production", new Set([production("A"), production("B")])),
    ).toEqual([]);
    // A preview's secrets, or a longer name, never stand in for production's.
    expect(
      missingAccessTokenSlots(
        "production",
        new Set([
          accessTokenSecretName("preview-1", "A"),
          accessTokenSecretName("preview-1", "B"),
          `${production("A")}_OLD`,
        ]),
      ),
    ).toEqual(["A", "B"]);
  });

  it("runs the token check before the production deploy, and the deploy only if it passes", () => {
    const tasks = shaledbViteConfig.run?.tasks ?? {};
    expect(JSON.stringify(tasks["deploy:production"])).toContain(
      JSON.stringify(
        "node src/access-tokens-cli.ts check production && alchemy deploy --stage production --yes",
      ),
    );
  });
});

describe("a written secret that never turns active (D121)", () => {
  /** Reads statuses from `statuses` in turn (the last repeats; `fail` fails the read), counting the reads. */
  const statusesOf = (statuses: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      const reads = yield* Ref.make(0);
      const read = Effect.gen(function* () {
        const index = yield* Ref.getAndUpdate(reads, (count) => count + 1);
        const status = statuses[Math.min(index, statuses.length - 1)] ?? "pending";
        return status === "fail" ? yield* Effect.fail("unreadable") : status;
      });
      return { read, reads };
    });

  const waitFor = (statuses: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      const { read, reads } = yield* statusesOf(statuses);
      const fiber = yield* Effect.forkChild(awaitSecretActive(read));
      yield* TestClock.adjust("30 seconds");
      return { active: yield* Fiber.join(fiber), reads: yield* Ref.get(reads) };
    }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);

  it("is reported active as soon as a read says so, after a failed read or two", async () => {
    expect(await waitFor(["pending", "fail", "active"])).toEqual({ active: true, reads: 3 });
  });

  it("is reported not active after its 20 reads, and the command then fails after showing the token", async () => {
    expect(await waitFor(["pending"])).toEqual({
      active: false,
      reads: SECRET_ACTIVE_POLL.reads,
    });
    const written = (active: boolean) => ({
      stage: "production",
      slot: "A" as const,
      secretName: accessTokenSecretName("production", "A"),
      token: Redacted.make("not-a-real-token"),
      active,
    });
    expect(inactiveSecretsFailure([written(true)])).toEqual(Option.none());
    const failure = inactiveSecretsFailure([written(true), written(false)]);
    expect(Option.map(failure, (error) => error.reason)).toEqual(Option.some("SecretNotActive"));
    expect(Option.getOrThrow(failure).message).toContain("SHALEDB_PRODUCTION_ACCESS_TOKEN_A");
    expect(Option.getOrThrow(failure).message).not.toContain("not-a-real-token");
  });
});
