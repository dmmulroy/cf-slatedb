import { DEFAULT_LSM_SETTINGS } from "@shaledb/lsm-core";
import { Effect, Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  deployEnvironment,
  deployJurisdiction,
  describeBoundLsmSettings,
  destroyCommand,
  LIVE_STAGES,
  PREVIEW_DEV_TIMINGS,
  PREVIEW_STAGES,
  PreviewStage,
  parseStateRead,
  previewOutputs,
  previewSecret,
  previewWorkerBindings,
  traceCollectorToken,
  traceTokenRefusal,
} from "./preview-stage.ts";

const outputs = {
  apiUrl: "https://shaledb-dbapiworker-preview-1-abc.example.workers.dev",
  shardsUrl: "https://shaledb-shardrouterworker-preview-1-abc.example.workers.dev",
  gcUrl: "https://shaledb-gcworker-preview-1-abc.example.workers.dev",
  blockServerWorkerName: "shaledb-blockserverworker-preview-1-abc",
  probeUrl: "https://shaledb-telemetryprobeworker-abc.example.workers.dev",
};

// What `vp run @shaledb/infra#state:read -r Shaledb/preview-1` prints: the task banner, then
// the records as JSON.
const stateRead = [
  "~/infra/shaledb$ alchemy state read --backend cloudflare -r Shaledb/preview-1 ⊘ cache disabled",
  JSON.stringify(
    {
      "Shaledb/preview-1/output": outputs,
      "Shaledb/preview-1/LsmAccessToken": {
        resourceType: "Alchemy.Random",
        attr: { text: { __redacted__: "f".repeat(64) } },
      },
    },
    null,
    2,
  ),
].join("\n");

describe("preview state", () => {
  it("reads a deployed stage's outputs and access token from `alchemy state read`", async () => {
    const parsed = await Effect.gen(function* () {
      const records = yield* parseStateRead(stateRead, "Shaledb/preview-1");
      return {
        outputs: yield* previewOutputs(records, "preview-1"),
        token: yield* previewSecret(records, "preview-1", "LsmAccessToken"),
        collectorToken: yield* previewSecret(records, "preview-1", "TraceCollectorToken"),
        other: yield* previewOutputs(records, "preview-2"),
      };
    }).pipe(Effect.runPromise);
    expect(parsed.outputs).toEqual(
      Option.some({ ...outputs, traceCollectorUrl: Option.none(), jurisdiction: Option.none() }),
    );
    expect(parsed.token).toEqual(Option.some("f".repeat(64)));
    expect(parsed.collectorToken).toEqual(Option.none());
    expect(parsed.other).toEqual(Option.none());
  });

  it("treats a missing path as nothing deployed and anything else unreadable as an error", async () => {
    const missing = await parseStateRead(
      "~/infra/shaledb$ alchemy state read ...\nerror: Shaledb: path does not exist\n",
      "Shaledb",
    ).pipe(Effect.runPromise);
    expect(missing.size).toBe(0);
    const failure = await parseStateRead("error: unauthorized", "Shaledb").pipe(
      Effect.flip,
      Effect.runPromise,
    );
    expect(failure.reason).toBe("StateUnreadable");
  });

  it("refuses an output record that is not this Stack's", async () => {
    const failure = await Effect.gen(function* () {
      const records = yield* parseStateRead(
        JSON.stringify({ "Shaledb/preview-2/output": { apiUrl: 1 } }),
        "Shaledb",
      );
      return yield* Effect.flip(previewOutputs(records, "preview-2"));
    }).pipe(Effect.runPromise);
    expect(failure.reason).toBe("StateRecordInvalid");
  });

  it("prints a destroy command for Dillon that goes through the stage's vp task", () => {
    expect(destroyCommand("preview-3")).toBe("vp run @shaledb/infra#destroy:preview-3");
  });
});

describe("deployJurisdiction", () => {
  const decoded = {
    ...outputs,
    traceCollectorUrl: Option.none<string>(),
    jurisdiction: Option.none<"none" | "eu" | "fedramp">(),
  };
  const deployed = (jurisdiction: "none" | "eu" | "fedramp") =>
    Option.some({ ...decoded, jurisdiction: Option.some(jurisdiction) });

  it("keeps a stage's jurisdiction unless the deploy asks for another, so a redeploy never moves it", () => {
    expect(deployJurisdiction(Option.none(), Option.none())).toBe("none");
    expect(deployJurisdiction(Option.none(), deployed("eu"))).toBe("eu");
    expect(deployJurisdiction(Option.some("none"), deployed("eu"))).toBe("none");
    expect(deployJurisdiction(Option.some("fedramp"), Option.some(decoded))).toBe("fedramp");
    expect(deployJurisdiction(Option.none(), Option.some(decoded))).toBe("none");
  });
});

describe("deployEnvironment", () => {
  it("decides the Stack's variables from the flags alone, whatever the shell holds", () => {
    const shell = {
      PATH: "/bin",
      SHALEDB_LSM_SETTINGS: '{"gcMinAge":1}',
      SHALEDB_JURISDICTION: "eu",
      SHALEDB_TRACE_SAMPLE_RATE: "0.5",
      SHALEDB_BUCKET_LOCATION: "weur",
    };
    expect(
      deployEnvironment(shell, {
        devTimings: false,
        jurisdiction: "none",
        settings: Option.none(),
        traceSampleRate: Option.none(),
      }),
    ).toEqual({
      PATH: "/bin",
      SHALEDB_JURISDICTION: "none",
    });
    expect(
      deployEnvironment(shell, {
        devTimings: true,
        jurisdiction: "fedramp",
        settings: Option.none(),
        traceSampleRate: Option.none(),
      }),
    ).toEqual({
      PATH: "/bin",
      SHALEDB_LSM_SETTINGS:
        '{"gcMinAge":10000,"compactorPollInterval":1000,"transactionRetentionMs":5000}',
      SHALEDB_JURISDICTION: "fedramp",
    });
  });
});

describe("deployEnvironment settings", () => {
  it("applies measurement overrides over the dev timings", () => {
    const environment = deployEnvironment(
      {},
      {
        devTimings: true,
        jurisdiction: "none",
        settings: Option.some({ l0SstSize: 268_435_456, gcMinAge: 20_000 }),
        traceSampleRate: Option.some(0.25),
      },
    );
    expect(JSON.parse(environment["SHALEDB_LSM_SETTINGS"] ?? "{}")).toEqual({
      ...PREVIEW_DEV_TIMINGS,
      l0SstSize: 268_435_456,
      gcMinAge: 20_000,
    });
    expect(environment["SHALEDB_TRACE_SAMPLE_RATE"]).toBe("0.25");
  });
});

describe("previewWorkerBindings", () => {
  it("reads each Worker's bound settings and jurisdiction from its state record", () => {
    const bound = (value: string) => ({
      __redacted__: JSON.stringify({ _tag: "Redacted", value }),
    });
    const complete = JSON.stringify(DEFAULT_LSM_SETTINGS);
    const records = new Map<string, unknown>([
      [
        "Shaledb/preview-1/DbApiWorker",
        {
          resourceType: "Cloudflare.Worker",
          props: {
            env: {
              SHALEDB_LSM_SETTINGS: bound(complete),
              SHALEDB_JURISDICTION: bound("none"),
              SHALEDB_ACCESS_TOKEN: bound("secret"),
            },
          },
        },
      ],
      [
        "Shaledb/preview-1/BlockServerWorker",
        {
          resourceType: "Cloudflare.Worker",
          props: { env: { SHALEDB_JURISDICTION: bound("eu") } },
        },
      ],
      ["Shaledb/preview-1/CpuTailWorker", { resourceType: "Cloudflare.Worker", props: {} }],
      ["Shaledb/preview-1/LsmBucket", { resourceType: "Cloudflare.R2.Bucket", props: {} }],
    ]);
    expect(previewWorkerBindings(records, "preview-1")).toEqual([
      { worker: "BlockServerWorker", lsmSettings: Option.none(), jurisdiction: Option.some("eu") },
      {
        worker: "DbApiWorker",
        lsmSettings: Option.some(complete),
        jurisdiction: Option.some("none"),
      },
    ]);
  });

  it("tells production settings from dev timings and from a partial binding", () => {
    expect(describeBoundLsmSettings(JSON.stringify(DEFAULT_LSM_SETTINGS))).toBe("production");
    expect(
      describeBoundLsmSettings(JSON.stringify({ ...DEFAULT_LSM_SETTINGS, gcMinAge: 10_000 })),
    ).toBe("complete, differs: gcMinAge=10000");
    expect(describeBoundLsmSettings("{}")).toBe("partial: {}");
  });
});

describe("trace-token", () => {
  // What `vp run @shaledb/infra#state:read -r Shaledb/production` holds on a deployed production:
  // the collector token, and no state-held access token (D64).
  const productionRecords = new Map<string, unknown>([
    [
      "Shaledb/production/TraceCollectorToken",
      { resourceType: "Alchemy.Random", attr: { text: { __redacted__: "c".repeat(64) } } },
    ],
  ]);

  it("reads production's trace collector token, which deploy's stage list never accepts", async () => {
    expect(LIVE_STAGES).toContain("production");
    expect(PREVIEW_STAGES).not.toContain("production");
    // `deploy`'s argument is the preview literals alone.
    expect(Schema.is(PreviewStage)("production")).toBe(false);
    const token = await traceCollectorToken(productionRecords, "production").pipe(
      Effect.runPromise,
    );
    expect(token).toBe("c".repeat(64));
  });

  it("names how to put a missing token right for production and for a preview", async () => {
    const missing = (stage: "production" | "preview-2") =>
      traceCollectorToken(new Map(), stage).pipe(Effect.flip, Effect.runPromise);
    const production = await missing("production");
    expect(production.reason).toBe("StateRecordMissing");
    expect(production.message).toContain("Shaledb/production/TraceCollectorToken");
    expect(production.message).toContain('docs/operations.md, "Deploying"');
    expect(production.message).not.toContain("preview-cli deploy");
    const preview = await missing("preview-2");
    expect(preview.message).toContain("preview-cli deploy preview-2");
  });

  it("writes the token only where it does not stay on a screen", () => {
    expect(traceTokenRefusal("production", false)).toEqual(Option.none());
    const refusal = traceTokenRefusal("production", true);
    expect(Option.isSome(refusal)).toBe(true);
    expect(Option.getOrElse(refusal, () => "")).toContain(
      'export SHALEDB_TRACE_TOKEN="$(node tools/preview-cli/src/preview-cli.ts trace-token production)"',
    );
  });
});
