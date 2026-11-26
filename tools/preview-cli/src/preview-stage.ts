import { DEFAULT_LSM_SETTINGS } from "@cf-slatedb/lsm-core";
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
export const PREVIEW_DEV_TIMINGS = {
  gcMinAge: 10_000,
  compactorPollInterval: 1_000,
  transactionRetentionMs: 5_000,
} as const;

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
 * Deploy variables the preview CLI decides from its flags. The Stack resolves the rest of its
 * configuration from them (`CfSlatedbStackConfig`, D55); values left in the shell must not change a
 * preview, so a variable a flag does not set is removed rather than inherited.
 */
const PREVIEW_DECIDED_VARIABLES = [
  "CF_SLATEDB_LSM_SETTINGS",
  "CF_SLATEDB_JURISDICTION",
  // Previews let R2 place their bucket: a hint left in the shell would replace it.
  "CF_SLATEDB_BUCKET_LOCATION",
  "CF_SLATEDB_TRACE_SAMPLE_RATE",
  "CF_SLATEDB_TRACE_RETENTION_DAYS",
] as const;

/**
 * The environment a preview deploy runs with: the inherited one, with the variables the flags
 * decide replaced. It only translates flags; the Stack resolves the complete configuration and binds
 * every Worker its full settings and jurisdiction on each deploy, whatever this environment holds.
 */
export const deployEnvironment = (
  inherited: Readonly<Record<string, string | undefined>>,
  options: { readonly devTimings: boolean; readonly jurisdiction: PreviewJurisdiction },
): Record<string, string> => {
  const environment: Record<string, string> = {};
  for (const [name, value] of Object.entries(inherited)) {
    if (value !== undefined && !PREVIEW_DECIDED_VARIABLES.some((decided) => decided === name)) {
      environment[name] = value;
    }
  }
  if (options.devTimings)
    environment["CF_SLATEDB_LSM_SETTINGS"] = JSON.stringify(PREVIEW_DEV_TIMINGS);
  environment["CF_SLATEDB_JURISDICTION"] = options.jurisdiction;
  return environment;
};

/** The engine settings and jurisdiction one Worker of a stage is bound, as recorded in state. */
export interface PreviewWorkerBindings {
  /** The Worker's logical id (`DbApiWorker`, ...). */
  readonly worker: string;
  /** The bound engine settings JSON, or `None` when the Worker has no such binding. */
  readonly lsmSettings: Option.Option<string>;
  /** The bound jurisdiction, or `None` when the Worker has no such binding. */
  readonly jurisdiction: Option.Option<string>;
}

const decodeWorkerRecord = Schema.decodeUnknownOption(
  Schema.Struct({
    resourceType: Schema.Literal("Cloudflare.Worker"),
    props: Schema.Struct({
      env: Schema.OptionFromOptionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  }),
);
// A bound `Config` value is recorded as a redacted, packed `Redacted` marker; a literal as text.
const decodeBoundValue = Schema.decodeUnknownOption(
  Schema.Union([
    Schema.String,
    Schema.Struct({
      __redacted__: Schema.fromJsonString(
        Schema.Struct({ _tag: Schema.Literal("Redacted"), value: Schema.String }),
      ),
    }),
  ]),
);

const boundText = (value: unknown): Option.Option<string> =>
  Option.map(decodeBoundValue(value), (bound) =>
    typeof bound === "string" ? bound : bound.__redacted__.value,
  );

/**
 * The engine settings and jurisdiction every Worker of `stage` is bound, read from the Workers'
 * records in `records` (the values the last deploy uploaded). Workers that bind neither are left
 * out.
 */
export const previewWorkerBindings = (
  records: StateRecords,
  stage: PreviewStage,
): ReadonlyArray<PreviewWorkerBindings> => {
  const prefix = `${CF_SLATEDB_STACK_NAME}/${stage}/`;
  const bindings: Array<PreviewWorkerBindings> = [];
  for (const [path, record] of records) {
    if (!path.startsWith(prefix)) continue;
    const worker = decodeWorkerRecord(record);
    if (Option.isNone(worker)) continue;
    const env: Readonly<Record<string, unknown>> = Option.getOrElse(
      worker.value.props.env,
      () => ({}),
    );
    const bound = (name: string) => Option.flatMap(Option.fromUndefinedOr(env[name]), boundText);
    const lsmSettings = bound("CF_SLATEDB_LSM_SETTINGS");
    const jurisdiction = bound("CF_SLATEDB_JURISDICTION");
    if (Option.isNone(lsmSettings) && Option.isNone(jurisdiction)) continue;
    bindings.push({ worker: path.slice(prefix.length), lsmSettings, jurisdiction });
  }
  return bindings.sort((left, right) => left.worker.localeCompare(right.worker));
};

const decodeSettingsJson = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);

/**
 * Describes bound engine settings for a person: `production` when they are exactly the defaults,
 * `partial: …` when they name only some settings (a deploy before D55), otherwise the settings that
 * differ from the defaults.
 */
export const describeBoundLsmSettings = (json: string): string => {
  const decoded = decodeSettingsJson(json);
  if (Option.isNone(decoded)) return `unreadable: ${json}`;
  const bound = decoded.value;
  const names = Object.keys(DEFAULT_LSM_SETTINGS);
  if (!names.every((name) => name in bound)) return `partial: ${json}`;
  const differing = Object.entries(DEFAULT_LSM_SETTINGS).filter(
    ([name, value]) => bound[name] !== value,
  );
  return differing.length === 0
    ? "production"
    : `complete, differs: ${differing.map(([name]) => `${name}=${String(bound[name])}`).join(", ")}`;
};

/** The `vp run` task that deploys a stage in place. */
export const deployTask = (stage: PreviewStage) => `@cf-slatedb/infra#deploy:${stage}`;

/**
 * The command Dillon runs to tear a preview down. Agents never run it: the deployment guard blocks
 * `alchemy destroy`, and previews are reused by redeploying them.
 */
export const destroyCommand = (stage: PreviewStage) => `vp run @cf-slatedb/infra#destroy:${stage}`;
