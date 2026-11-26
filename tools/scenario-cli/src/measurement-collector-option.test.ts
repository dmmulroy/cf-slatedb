import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  type MeasurementCollectorOptions,
  selectMeasurementCollector,
} from "./measurement-collector-option.ts";

const COLLECTOR_URL = "https://shaledb-tracecollectorworker-production-abc.example.workers.dev";
const TOKEN = "t".repeat(64);
const PREVIEWS = ["preview-1", "preview-2", "preview-3"];

const options = (
  stage: Option.Option<string>,
  collectorUrl: Option.Option<string>,
): MeasurementCollectorOptions => ({ stage, collectorUrl });

const failureOf = (
  given: MeasurementCollectorOptions,
  environment: Readonly<Record<string, string | undefined>>,
) => selectMeasurementCollector(given, environment).pipe(Effect.flip, Effect.runPromise);

describe("selectMeasurementCollector", () => {
  it("reads a collector by URL with the token from SHALEDB_TRACE_TOKEN, in a store no preview uses", async () => {
    const selection = await selectMeasurementCollector(
      options(Option.none(), Option.some(COLLECTOR_URL)),
      { SHALEDB_TRACE_TOKEN: TOKEN },
    ).pipe(Effect.runPromise);
    expect(selection).toEqual({
      _tag: "Url",
      collector: {
        stage: "shaledb-tracecollectorworker-production-abc.example.workers.dev",
        url: `${COLLECTOR_URL}/`,
        token: TOKEN,
      },
    });
    // The name picks `.traces/measure/<name>`: a URL's never equals a preview's.
    const name = selection._tag === "Url" ? selection.collector.stage : "";
    expect(PREVIEWS).not.toContain(name);
  });

  it("keeps --stage for a preview, resolved through the preview CLI", async () => {
    const selection = await selectMeasurementCollector(
      options(Option.some("preview-3"), Option.none()),
      {},
    ).pipe(Effect.runPromise);
    expect(selection).toEqual({ _tag: "Stage", stage: "preview-3" });
  });

  it("refuses --collector-url without a token, or with an empty one, saying what to set", async () => {
    for (const environment of [{}, { SHALEDB_TRACE_TOKEN: "" }, { SHALEDB_TRACE_TOKEN: " " }]) {
      const failure = await failureOf(
        options(Option.none(), Option.some(COLLECTOR_URL)),
        environment,
      );
      expect(failure.reason).toBe("TraceTokenMissing");
      expect(failure.message).toContain("SHALEDB_TRACE_TOKEN");
      expect(failure.message).toContain("trace-token <stage>");
    }
  });

  it("refuses both --stage and --collector-url, and neither", async () => {
    const environment = { SHALEDB_TRACE_TOKEN: TOKEN };
    const both = await failureOf(
      options(Option.some("preview-1"), Option.some(COLLECTOR_URL)),
      environment,
    );
    expect(both.reason).toBe("CollectorNamedTwice");
    const neither = await failureOf(options(Option.none(), Option.none()), environment);
    expect(neither.reason).toBe("CollectorNotNamed");
    expect(neither.message).toContain("--collector-url");
  });

  it("refuses a --collector-url that is not http or https without echoing it", async () => {
    const failure = await failureOf(options(Option.none(), Option.some(TOKEN)), {
      SHALEDB_TRACE_TOKEN: TOKEN,
    });
    expect(failure.reason).toBe("CollectorUrlInvalid");
    expect(failure.message).not.toContain(TOKEN);
  });
});
