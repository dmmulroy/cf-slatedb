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

/** Schema for a preview's data-residency jurisdiction: `none` leaves it unrestricted. */
export const PreviewJurisdiction = Schema.Literals(["none", "eu", "fedramp"]);

/** A preview's data-residency jurisdiction. */
export type PreviewJurisdiction = typeof PreviewJurisdiction.Type;

/**
 * The jurisdiction a deploy uses: the one asked for, or else the stage's current one (so a redeploy
 * never moves an installation by accident), or else none.
 */
export const deployJurisdiction = (
  requested: Option.Option<PreviewJurisdiction>,
  current: Option.Option<PreviewOutputs>,
): PreviewJurisdiction =>
  Option.getOrElse(requested, () =>
    Option.getOrElse(
      Option.flatMap(current, (outputs) => outputs.jurisdiction),
      () => "none" as const,
    ),
  );

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
  traceCollectorUrl: Schema.OptionFromOptionalKey(Schema.String),
  /** Data-residency jurisdiction (`eu`, `fedramp`, `none`), once the Stack reports one. */
  jurisdiction: Schema.OptionFromOptionalKey(PreviewJurisdiction),
});

/** A deployed preview's Stack outputs. */
export type PreviewOutputs = typeof PreviewOutputs.Type;

/** Encodes outputs back to the Stack's JSON form, for printing (optional outputs omitted when absent). */
export const encodePreviewOutputs = Schema.encodeSync(PreviewOutputs);

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
  /** The state path read (`CfSlatedb[/<stage>[/<record>]]`). */
  readonly path: string;
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
 *
 * @returns The records by path, or `PreviewStateError` (`StateUnreadable`) when the output is
 *   neither records nor "path does not exist" (an Alchemy login or network failure, printed whole).
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
          path,
          message: `The Alchemy state store's records under ${path} could not be read:\n${output.trim()}`,
        }),
    ),
  );
  return new Map(Object.entries(records));
});

/**
 * The Stack outputs of a stage in `records`, or `None` when nothing is deployed there.
 *
 * @returns The outputs, or `PreviewStateError` (`StateRecordInvalid`) when the record is not this
 *   Stack's output; redeploying the stage rewrites it.
 */
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
            path,
            message: `${path} is not this Stack's output: ${error.message}. Redeploy ${stage} with \`preview-cli deploy ${stage}\`.`,
          }),
      ),
    ),
  );
});

/**
 * A stage's generated secrets: `LsmAccessToken` gates its public Workers; `TraceCollectorToken`
 * authenticates span exports to, and reads from, its trace collector.
 */
export type PreviewSecret = "LsmAccessToken" | "TraceCollectorToken";

/**
 * A stage's generated secret in `records`, or `None` when the stage has none.
 *
 * @returns The secret, or `PreviewStateError` (`StateRecordInvalid`) when the record holds no
 *   generated value; redeploying the stage regenerates it.
 */
export const previewSecret = Effect.fn("PreviewStage.previewSecret")(function* (
  records: StateRecords,
  stage: PreviewStage,
  secret: PreviewSecret,
) {
  const path = `${CF_SLATEDB_STACK_NAME}/${stage}/${secret}`;
  const record = records.get(path);
  if (record === undefined) return Option.none<string>();
  const decoded = yield* decodeRedactedText(record).pipe(
    Effect.mapError(
      () =>
        new PreviewStateError({
          reason: "StateRecordInvalid",
          path,
          message: `${path} does not hold a generated token. Redeploy ${stage} with \`preview-cli deploy ${stage}\`.`,
        }),
    ),
  );
  return Option.some(decoded.attr.text.__redacted__);
});

/**
 * The environment a preview deploy runs with: the inherited one, with the engine settings and the
 * jurisdiction always set explicitly. A value a deploy stops setting stays bound on the Workers
 * (Cloudflare keeps a script's secrets across uploads, D54), so "no dev timings" is `{}` and "no
 * jurisdiction" is `none`, never an absent variable.
 */
export const deployEnvironment = (
  inherited: Readonly<Record<string, string | undefined>>,
  options: { readonly devTimings: boolean; readonly jurisdiction: PreviewJurisdiction },
): Record<string, string | undefined> => ({
  ...inherited,
  CF_SLATEDB_LSM_SETTINGS: options.devTimings ? JSON.stringify(PREVIEW_DEV_TIMINGS) : "{}",
  CF_SLATEDB_JURISDICTION: options.jurisdiction,
});

/** The `vp run` task that deploys a stage in place. */
export const deployTask = (stage: PreviewStage) => `@cf-slatedb/infra#deploy:${stage}`;

/**
 * The command Dillon runs to tear a preview down. Agents never run it: the deployment guard blocks
 * `alchemy destroy`, and previews are reused by redeploying them.
 */
export const destroyCommand = (stage: PreviewStage) => `vp run @cf-slatedb/infra#destroy:${stage}`;
