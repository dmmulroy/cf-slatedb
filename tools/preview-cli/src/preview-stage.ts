import { Data, Effect, Option, Schema } from "effect";

/** Schema for one of the three reusable live preview stages. */
export const PreviewStage = Schema.Literals(["preview-1", "preview-2", "preview-3"]);

/** A reusable live preview stage. */
export type PreviewStage = typeof PreviewStage.Type;

/** Every preview stage, in order. */
export const PREVIEW_STAGES: ReadonlyArray<PreviewStage> = PreviewStage.literals;

/** Name of the Alchemy Stack every preview deploys (`infra/cf-slatedb/alchemy.run.ts`). */
export const CF_SLATEDB_STACK_NAME = "CfSlatedb";

/**
 * Engine settings for `deploy --dev-timings`: the same short GC and compactor timings as `vp run
 * dev`, so scenarios see compaction and collection within seconds. Without the flag a preview runs
 * the production defaults, so its measurements are honest.
 */
export const PREVIEW_DEV_TIMINGS = { gcMinAge: 10_000, compactorPollInterval: 1_000 } as const;

/** Schema for a deployed preview's Stack outputs (`CfSlatedb/<stage>/output` in Alchemy state). */
export const PreviewOutputs = Schema.Struct({
  /** Public database API (`/v1`). */
  apiUrl: Schema.String,
  /** The sharding package's example router. */
  shardsUrl: Schema.String,
  /** GC operator API. */
  gcUrl: Schema.String,
  /** The `BlockServer` Worker; it has no URL, only a service binding from the API Worker. */
  blockServerWorkerName: Schema.String,
  /** Telemetry probe (Worker plus Durable Object with an alarm). */
  probeUrl: Schema.String,
  /** OTLP receiver of the stage's spans (`TraceCollector`), once the Stack has one. */
  traceCollectorUrl: Schema.optionalKey(Schema.String),
});

/** A deployed preview's Stack outputs. */
export type PreviewOutputs = typeof PreviewOutputs.Type;

/** What `status` knows about one preview stage. */
export interface PreviewStatus {
  readonly stage: PreviewStage;
  /** The Stack outputs, or `None` when nothing is deployed to the stage. */
  readonly outputs: Option.Option<PreviewOutputs>;
}

/** Records of the Alchemy state store, keyed by path (`CfSlatedb/<stage>/<resource>`). */
export type StateRecords = ReadonlyMap<string, unknown>;

/**
 * The Alchemy state store could not be read, or a record in it does not have the expected shape.
 * `reason` is `StateUnreadable` when `alchemy state read` failed and `StateRecordInvalid` when a
 * record did not parse.
 */
export class PreviewStateError extends Data.TaggedError("PreviewStateError")<{
  readonly reason: "StateUnreadable" | "StateRecordInvalid";
  readonly message: string;
}> {}

const decodeStateJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodePreviewOutputs = Schema.decodeUnknownEffect(PreviewOutputs);
const decodeRedactedText = Schema.decodeUnknownEffect(
  Schema.Struct({ attr: Schema.Struct({ text: Schema.Struct({ __redacted__: Schema.String }) }) }),
);

/**
 * Parses the output of `alchemy state read -r <path>` (run through `vp run`, so it may start with
 * the task's banner line): the records under the path, or none when the path does not exist.
 */
export const parseStateRead = Effect.fn("PreviewStage.parseStateRead")(function* (
  output: string,
  path: string,
) {
  if (output.includes(`${path}: path does not exist`)) return new Map<string, unknown>();
  const start = output.indexOf("\n{");
  const json = output.startsWith("{") ? output : start === -1 ? "" : output.slice(start + 1);
  const records = yield* decodeStateJson(json.trim()).pipe(
    Effect.mapError(
      () =>
        new PreviewStateError({
          reason: "StateUnreadable",
          message: `The Alchemy state store's records under ${path} could not be read:\n${output.trim()}`,
        }),
    ),
  );
  return new Map(Object.entries(records));
});

/** The Stack outputs of a stage in `records`, or `None` when nothing is deployed there. */
export const previewOutputs = Effect.fn("PreviewStage.previewOutputs")(function* (
  records: StateRecords,
  stage: PreviewStage,
) {
  const path = `${CF_SLATEDB_STACK_NAME}/${stage}/output`;
  const output = records.get(path);
  if (output === undefined) return Option.none<PreviewOutputs>();
  return Option.some(
    yield* decodePreviewOutputs(output).pipe(
      Effect.mapError(
        (error) =>
          new PreviewStateError({
            reason: "StateRecordInvalid",
            message: `${path} is not this Stack's output: ${error.message}. Redeploy ${stage} with \`preview-cli deploy ${stage}\`.`,
          }),
      ),
    ),
  );
});

/** The access token of a stage in `records`, or `None` when the stage has none. */
export const previewAccessToken = Effect.fn("PreviewStage.previewAccessToken")(function* (
  records: StateRecords,
  stage: PreviewStage,
) {
  const path = `${CF_SLATEDB_STACK_NAME}/${stage}/LsmAccessToken`;
  const record = records.get(path);
  if (record === undefined) return Option.none<string>();
  const decoded = yield* decodeRedactedText(record).pipe(
    Effect.mapError(
      () =>
        new PreviewStateError({
          reason: "StateRecordInvalid",
          message: `${path} does not hold a generated token. Redeploy ${stage} with \`preview-cli deploy ${stage}\`.`,
        }),
    ),
  );
  return Option.some(decoded.attr.text.__redacted__);
});

/** The `vp run` task that deploys a stage in place. */
export const deployTask = (stage: PreviewStage) => `@cf-slatedb/infra#deploy:${stage}`;

/**
 * The command Dillon runs to tear a preview down. Agents never run it: the deployment guard blocks
 * `alchemy destroy`, and previews are reused by redeploying them.
 */
export const destroyCommand = (stage: PreviewStage) => `vp run @cf-slatedb/infra#destroy:${stage}`;
