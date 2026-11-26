import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  requireTraceCollector,
  selectTraceCollector,
  type TraceCollectorOptions,
} from "./trace-collector-option.ts";

const COLLECTOR_URL = "https://shaledb-tracecollectorworker-production-abc.example.workers.dev";
const TOKEN = "t".repeat(64);

const options = (stage: Option.Option<string>, collectorUrl: Option.Option<string>) =>
  ({ stage, collectorUrl }) satisfies TraceCollectorOptions;

const failureOf = (
  given: TraceCollectorOptions,
  environment: Readonly<Record<string, string | undefined>>,
) => requireTraceCollector(given, environment).pipe(Effect.flip, Effect.runPromise);

describe("selectTraceCollector", () => {
  it("names a collector by URL with the token from SHALEDB_TRACE_TOKEN", async () => {
    const selection = await requireTraceCollector(
      options(Option.none(), Option.some(COLLECTOR_URL)),
      { SHALEDB_TRACE_TOKEN: TOKEN },
    ).pipe(Effect.runPromise);
    expect(selection).toEqual({
      _tag: "Url",
      collector: {
        name: "shaledb-tracecollectorworker-production-abc.example.workers.dev",
        url: `${COLLECTOR_URL}/`,
        token: TOKEN,
        stage: Option.none(),
      },
    });
  });

  it("names a preview collector by stage, to be resolved through the preview CLI", async () => {
    const selection = await requireTraceCollector(
      options(Option.some("preview-3"), Option.none()),
      {
        SHALEDB_TRACE_TOKEN: TOKEN,
      },
    ).pipe(Effect.runPromise);
    expect(selection).toEqual({ _tag: "Stage", stage: "preview-3" });
  });

  it("gives a URL with a port a name that is a safe file name", async () => {
    const selection = await requireTraceCollector(
      options(Option.none(), Option.some("http://127.0.0.1:8787")),
      { SHALEDB_TRACE_TOKEN: TOKEN },
    ).pipe(Effect.runPromise);
    expect(selection._tag === "Url" ? selection.collector.name : "").toBe("127.0.0.1_8787");
  });

  it("refuses --collector-url without a token, or with an empty one, saying what to set", async () => {
    for (const environment of [{}, { SHALEDB_TRACE_TOKEN: "" }, { SHALEDB_TRACE_TOKEN: "  \n" }]) {
      const failure = await failureOf(
        options(Option.none(), Option.some(COLLECTOR_URL)),
        environment,
      );
      expect(failure.reason).toBe("TraceTokenMissing");
      expect(failure.message).toContain("SHALEDB_TRACE_TOKEN");
      expect(failure.message).toContain("trace-token <stage>");
    }
  });

  it("refuses both --stage and --collector-url, and neither for a command that needs one", async () => {
    const both = await failureOf(options(Option.some("preview-1"), Option.some(COLLECTOR_URL)), {
      SHALEDB_TRACE_TOKEN: TOKEN,
    });
    expect(both.reason).toBe("CollectorNamedTwice");
    const neither = await failureOf(options(Option.none(), Option.none()), {
      SHALEDB_TRACE_TOKEN: TOKEN,
    });
    expect(neither.reason).toBe("CollectorNotNamed");
    expect(neither.message).toContain("--collector-url");
    // `tail` without either follows the local store instead.
    const local = await selectTraceCollector(options(Option.none(), Option.none()), {}).pipe(
      Effect.runPromise,
    );
    expect(local).toEqual(Option.none());
  });

  it("refuses a --collector-url that is not http or https without echoing it", async () => {
    for (const text of [TOKEN, "ftp://collector.example"]) {
      const failure = await failureOf(options(Option.none(), Option.some(text)), {
        SHALEDB_TRACE_TOKEN: TOKEN,
      });
      expect(failure.reason).toBe("CollectorUrlInvalid");
      expect(failure.message).not.toContain(text);
    }
  });
});
