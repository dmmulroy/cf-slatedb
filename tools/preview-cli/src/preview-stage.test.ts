import { DEFAULT_LSM_SETTINGS } from "@cf-slatedb/lsm-core";
import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  deployEnvironment,
  deployJurisdiction,
  describeBoundLsmSettings,
  destroyCommand,
  parseStateRead,
  previewOutputs,
  previewSecret,
  previewWorkerBindings,
} from "./preview-stage.ts";

const outputs = {
  apiUrl: "https://cfslatedb-dbapiworker-preview-1-abc.example.workers.dev",
  shardsUrl: "https://cfslatedb-shardrouterworker-preview-1-abc.example.workers.dev",
  gcUrl: "https://cfslatedb-gcworker-preview-1-abc.example.workers.dev",
  blockServerWorkerName: "cfslatedb-blockserverworker-preview-1-abc",
  probeUrl: "https://cfslatedb-telemetryprobeworker-abc.example.workers.dev",
};

// What `vp run @cf-slatedb/infra#state:read -r CfSlatedb/preview-1` prints: the task banner, then
// the records as JSON.
const stateRead = [
  "~/infra/cf-slatedb$ alchemy state read --backend cloudflare -r CfSlatedb/preview-1 ⊘ cache disabled",
  JSON.stringify(
    {
      "CfSlatedb/preview-1/output": outputs,
      "CfSlatedb/preview-1/LsmAccessToken": {
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
      const records = yield* parseStateRead(stateRead, "CfSlatedb/preview-1");
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
      "~/infra/cf-slatedb$ alchemy state read ...\nerror: CfSlatedb: path does not exist\n",
      "CfSlatedb",
    ).pipe(Effect.runPromise);
    expect(missing.size).toBe(0);
    const failure = await parseStateRead("error: unauthorized", "CfSlatedb").pipe(
      Effect.flip,
      Effect.runPromise,
    );
    expect(failure.reason).toBe("StateUnreadable");
  });

  it("refuses an output record that is not this Stack's", async () => {
    const failure = await Effect.gen(function* () {
      const records = yield* parseStateRead(
        JSON.stringify({ "CfSlatedb/preview-2/output": { apiUrl: 1 } }),
        "CfSlatedb",
      );
      return yield* Effect.flip(previewOutputs(records, "preview-2"));
    }).pipe(Effect.runPromise);
    expect(failure.reason).toBe("StateRecordInvalid");
  });

  it("prints a destroy command for Dillon that goes through the stage's vp task", () => {
    expect(destroyCommand("preview-3")).toBe("vp run @cf-slatedb/infra#destroy:preview-3");
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
      CF_SLATEDB_LSM_SETTINGS: '{"gcMinAge":1}',
      CF_SLATEDB_JURISDICTION: "eu",
      CF_SLATEDB_TRACE_SAMPLE_RATE: "0.5",
      CF_SLATEDB_BUCKET_LOCATION: "weur",
    };
    expect(deployEnvironment(shell, { devTimings: false, jurisdiction: "none" })).toEqual({
      PATH: "/bin",
      CF_SLATEDB_JURISDICTION: "none",
    });
    expect(deployEnvironment(shell, { devTimings: true, jurisdiction: "fedramp" })).toEqual({
      PATH: "/bin",
      CF_SLATEDB_LSM_SETTINGS:
        '{"gcMinAge":10000,"compactorPollInterval":1000,"transactionRetentionMs":5000}',
      CF_SLATEDB_JURISDICTION: "fedramp",
    });
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
        "CfSlatedb/preview-1/DbApiWorker",
        {
          resourceType: "Cloudflare.Worker",
          props: {
            env: {
              CF_SLATEDB_LSM_SETTINGS: bound(complete),
              CF_SLATEDB_JURISDICTION: bound("none"),
              CF_SLATEDB_ACCESS_TOKEN: bound("secret"),
            },
          },
        },
      ],
      [
        "CfSlatedb/preview-1/BlockServerWorker",
        {
          resourceType: "Cloudflare.Worker",
          props: { env: { CF_SLATEDB_JURISDICTION: bound("eu") } },
        },
      ],
      ["CfSlatedb/preview-1/CpuTailWorker", { resourceType: "Cloudflare.Worker", props: {} }],
      ["CfSlatedb/preview-1/LsmBucket", { resourceType: "Cloudflare.R2.Bucket", props: {} }],
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
