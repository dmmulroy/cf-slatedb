import { DEFAULT_LSM_SETTINGS, LsmSettings } from "@cf-slatedb/lsm-core";
import {
  isLocalLsmStage,
  LSM_BUCKET_LOCATION_SETTING,
  LSM_JURISDICTION_SETTING,
  LSM_SETTINGS_BINDING,
  LsmBucketLocation,
  type LsmWorkerSettings,
  type LsmWorkerSettingsSource,
} from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { Stage } from "alchemy/Stage";
import { Data, Effect, Option, Schema } from "effect";

import { CF_SLATEDB_PRODUCTION_STAGE } from "./cf-slatedb-stage.ts";

/**
 * Name of the deploy variable that sets the fraction of requests whose traces are kept (0 to 1).
 * Default: 1 on local and preview stages, {@link PRODUCTION_TRACE_SAMPLE_RATE} in production.
 */
export const CF_SLATEDB_TRACE_SAMPLE_RATE_VARIABLE = "CF_SLATEDB_TRACE_SAMPLE_RATE";

/**
 * Name of the deploy variable that sets how many days a live stage keeps exported traces and tail
 * records. Default: {@link DEFAULT_TRACE_RETENTION_DAYS}.
 */
export const CF_SLATEDB_TRACE_RETENTION_DAYS_VARIABLE = "CF_SLATEDB_TRACE_RETENTION_DAYS";

/** Name of the deploy variable that sets the API Worker's CPU limit per invocation, in ms. */
export const CF_SLATEDB_API_CPU_MS_VARIABLE = "CF_SLATEDB_API_CPU_MS";

/** Name of the deploy variable that sets the compactor Worker's CPU limit per invocation, in ms. */
export const CF_SLATEDB_COMPACTOR_CPU_MS_VARIABLE = "CF_SLATEDB_COMPACTOR_CPU_MS";

/**
 * Default CPU limit of the API Worker and the writers it hosts (P5, D61): a 60 MiB flush peaked at
 * 3.5 s of CPU on `preview-1`, so twice that is well inside Workers Paid's 30 s default, kept
 * explicit.
 */
export const DEFAULT_API_CPU_MS = 30_000;

/**
 * Default CPU limit of the compactor Worker (P5, D61): a compaction step writing a 64 MiB output
 * peaked at 17.7 s of CPU on `preview-1`; twice that exceeds the 30 s default, so it is raised.
 */
export const DEFAULT_COMPACTOR_CPU_MS = 60_000;

/** Default fraction of production requests whose traces are kept (P4). */
export const PRODUCTION_TRACE_SAMPLE_RATE = 0.1;

/** Default number of days a live stage keeps its traces and tail records (P4). */
export const DEFAULT_TRACE_RETENTION_DAYS = 14;

/** Schema for the fraction of requests whose traces are kept: 0 (none) to 1 (every request). */
const TraceSampleRate = Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

/** Schema for a Worker's CPU limit per invocation: 10 ms to Workers Paid's 5-minute maximum. */
const CpuLimitMs = Schema.Int.check(Schema.isBetween({ minimum: 10, maximum: 300_000 }));

/** Schema for how many whole days a live stage keeps traces and tail records. */
const TraceRetentionDays = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Schema for where an unrestricted Stack's LSM bucket is created: an R2 location hint, or `auto`
 * (R2 places it near the deploy that creates it).
 */
export const CfSlatedbBucketLocation = Schema.Literals(["auto", ...LsmBucketLocation.literals]);

/** Where an unrestricted Stack's LSM bucket is created. */
export type CfSlatedbBucketLocation = typeof CfSlatedbBucketLocation.Type;

/** Schema for the Stack's data residency: `eu`, `fedramp`, or `none` (unrestricted). */
export const CfSlatedbJurisdiction = Schema.Literals(["eu", "fedramp", "none"]);

/** The Stack's data residency. */
export type CfSlatedbJurisdiction = typeof CfSlatedbJurisdiction.Type;

/**
 * Schema for how callers authenticate to the Stack's public Workers: `Open` on local stages, which
 * are not reachable from outside the machine; `BearerToken` on every live stage (the access gate).
 */
export const CfSlatedbAccessMode = Schema.Literals(["Open", "BearerToken"]);

/** How callers authenticate to the Stack's public Workers. */
export type CfSlatedbAccessMode = typeof CfSlatedbAccessMode.Type;

/**
 * Schema for the complete configuration of one cf-slatedb installation, resolved at plan time.
 *
 * **Details**
 *
 * - `stage`: the Alchemy stage being planned.
 * - `jurisdiction`: data residency of the buckets, Durable Objects, and Workflow; fixed per
 *   installation (D49).
 * - `bucketLocation`: the LSM bucket's R2 location hint, near the writers; fixed per installation
 *   (R2 cannot move a bucket) and only for an unrestricted Stack (D56).
 * - `lsmSettings`: the complete engine settings, the defaults merged with the deploy's overrides.
 * - `telemetry.traceSampleRate`: fraction of requests whose traces are kept.
 * - `telemetry.traceRetentionDays`: days a live stage keeps traces and tail records.
 * - `access.mode`: how callers authenticate to the public Workers.
 * - `limits`: CPU per invocation of the API Worker (and its writers) and the compactor Worker.
 */
export const CfSlatedbStackConfig = Schema.Struct({
  stage: Schema.String,
  jurisdiction: CfSlatedbJurisdiction,
  bucketLocation: CfSlatedbBucketLocation,
  lsmSettings: LsmSettings,
  telemetry: Schema.Struct({
    traceSampleRate: TraceSampleRate,
    traceRetentionDays: TraceRetentionDays,
  }),
  access: Schema.Struct({ mode: CfSlatedbAccessMode }),
  limits: Schema.Struct({ apiCpuMs: CpuLimitMs, compactorCpuMs: CpuLimitMs }),
});

/** The complete configuration of one cf-slatedb installation. */
export type CfSlatedbStackConfig = typeof CfSlatedbStackConfig.Type;

/** The deploy's environment: variable name to value, as `process.env` holds it. */
export type CfSlatedbDeployEnvironment = Readonly<Record<string, string | undefined>>;

/**
 * The deploy's environment could not be resolved into a Stack configuration: a variable does not
 * parse, or the settings it yields contradict each other. The plan stops before anything deploys.
 */
export class CfSlatedbStackConfigError extends Data.TaggedError("CfSlatedbStackConfigError")<{
  /** The deploy variable whose value was refused. */
  readonly variable: string;
  readonly message: string;
}> {}

// The deploy's environment is the boundary: its values are untrusted text.
const decodeJurisdiction = Schema.decodeUnknownEffect(CfSlatedbJurisdiction);
const decodeBucketLocation = Schema.decodeUnknownEffect(CfSlatedbBucketLocation);
const decodeSettingsOverrides = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);
// Overrides name settings by their field names; an unknown name is a typo that must not pass
// silently, so excess keys are refused.
const decodeLsmSettings = Schema.decodeUnknownEffect(LsmSettings, { onExcessProperty: "error" });
const decodeTraceSampleRate = Schema.decodeEffect(
  Schema.NumberFromString.pipe(Schema.decodeTo(TraceSampleRate)),
);
const decodeCpuLimitMs = Schema.decodeEffect(
  Schema.NumberFromString.pipe(Schema.decodeTo(CpuLimitMs)),
);
const decodeTraceRetentionDays = Schema.decodeEffect(
  Schema.NumberFromString.pipe(Schema.decodeTo(TraceRetentionDays)),
);

/** A variable's value, or `None` when it is unset or blank. */
const variable = (environment: CfSlatedbDeployEnvironment, name: string) =>
  Option.filter(Option.fromUndefinedOr(environment[name]), (value) => value.trim() !== "");

const refused = (name: string, reason: string) =>
  new CfSlatedbStackConfigError({
    variable: name,
    message: `${name} was refused: ${reason}. Fix it in the deploy's environment and plan again.`,
  });

/**
 * Resolves the Stack configuration of `stage` from the deploy's environment, with complete
 * defaults: every value an installation needs is decided here, so nothing downstream falls back to
 * a default or to a value an earlier deploy left bound.
 *
 * **Details**
 *
 * - `CF_SLATEDB_JURISDICTION`: `eu`, `fedramp`, or `none` (default `none`).
 * - `CF_SLATEDB_BUCKET_LOCATION`: `wnam`, `enam`, `weur`, `eeur`, `apac`, `oc`, or `auto` (default
 *   `auto`); refused together with a jurisdiction.
 * - `CF_SLATEDB_LSM_SETTINGS`: a JSON object of setting overrides by field name (default `{}`).
 *   Unknown names and contradictory combinations are refused.
 * - `CF_SLATEDB_TRACE_SAMPLE_RATE`: 0 to 1 (default 1; 0.1 in production).
 * - `CF_SLATEDB_TRACE_RETENTION_DAYS`: whole days (default 14).
 * - `CF_SLATEDB_API_CPU_MS`, `CF_SLATEDB_COMPACTOR_CPU_MS`: CPU limits per invocation (defaults
 *   30 s and 60 s, from measurements).
 *
 * @returns The configuration, or `CfSlatedbStackConfigError` naming the refused variable.
 */
export const resolveCfSlatedbStackConfig = Effect.fn("CfSlatedbStackConfig.resolve")(function* (
  stage: string,
  environment: CfSlatedbDeployEnvironment,
) {
  const jurisdiction = yield* Option.match(variable(environment, LSM_JURISDICTION_SETTING), {
    onNone: () => Effect.succeed<CfSlatedbJurisdiction>("none"),
    onSome: (text) =>
      decodeJurisdiction(text.trim()).pipe(
        Effect.mapError(() =>
          refused(LSM_JURISDICTION_SETTING, `"${text}" is not one of eu, fedramp, or none`),
        ),
      ),
  });
  const bucketLocation = yield* Option.match(variable(environment, LSM_BUCKET_LOCATION_SETTING), {
    onNone: () => Effect.succeed<CfSlatedbBucketLocation>("auto"),
    onSome: (text) =>
      decodeBucketLocation(text.trim()).pipe(
        Effect.mapError(() =>
          refused(
            LSM_BUCKET_LOCATION_SETTING,
            `"${text}" is not one of wnam, enam, weur, eeur, apac, oc, or auto`,
          ),
        ),
      ),
  });
  if (bucketLocation !== "auto" && jurisdiction !== "none") {
    return yield* refused(
      LSM_BUCKET_LOCATION_SETTING,
      `a location hint does not apply to a bucket restricted to the ${jurisdiction} jurisdiction; use auto`,
    );
  }
  const overrides = yield* Option.match(variable(environment, LSM_SETTINGS_BINDING), {
    onNone: () => Effect.succeed<Readonly<Record<string, number>>>({}),
    onSome: (text) =>
      decodeSettingsOverrides(text).pipe(
        Effect.mapError(() =>
          refused(LSM_SETTINGS_BINDING, "it is not a JSON object of numeric settings"),
        ),
      ),
  });
  const lsmSettings = yield* decodeLsmSettings({ ...DEFAULT_LSM_SETTINGS, ...overrides }).pipe(
    Effect.mapError((error) => refused(LSM_SETTINGS_BINDING, error.message)),
  );
  const traceSampleRate = yield* Option.match(
    variable(environment, CF_SLATEDB_TRACE_SAMPLE_RATE_VARIABLE),
    {
      onNone: () =>
        Effect.succeed(stage === CF_SLATEDB_PRODUCTION_STAGE ? PRODUCTION_TRACE_SAMPLE_RATE : 1),
      onSome: (text) =>
        decodeTraceSampleRate(text.trim()).pipe(
          Effect.mapError(() =>
            refused(CF_SLATEDB_TRACE_SAMPLE_RATE_VARIABLE, `"${text}" is not a number from 0 to 1`),
          ),
        ),
    },
  );
  const traceRetentionDays = yield* Option.match(
    variable(environment, CF_SLATEDB_TRACE_RETENTION_DAYS_VARIABLE),
    {
      onNone: () => Effect.succeed(DEFAULT_TRACE_RETENTION_DAYS),
      onSome: (text) =>
        decodeTraceRetentionDays(text.trim()).pipe(
          Effect.mapError(() =>
            refused(
              CF_SLATEDB_TRACE_RETENTION_DAYS_VARIABLE,
              `"${text}" is not a whole number of days above 0`,
            ),
          ),
        ),
    },
  );
  const cpuLimit = (name: string, fallback: number) =>
    Option.match(variable(environment, name), {
      onNone: () => Effect.succeed(fallback),
      onSome: (text) =>
        decodeCpuLimitMs(text.trim()).pipe(
          Effect.mapError(() =>
            refused(name, `"${text}" is not a whole number of milliseconds from 10 to 300000`),
          ),
        ),
    });
  const apiCpuMs = yield* cpuLimit(CF_SLATEDB_API_CPU_MS_VARIABLE, DEFAULT_API_CPU_MS);
  const compactorCpuMs = yield* cpuLimit(
    CF_SLATEDB_COMPACTOR_CPU_MS_VARIABLE,
    DEFAULT_COMPACTOR_CPU_MS,
  );
  const accessMode: CfSlatedbAccessMode = isLocalLsmStage(stage) ? "Open" : "BearerToken";
  // Every part was decoded by its own schema above.
  return {
    stage,
    jurisdiction,
    bucketLocation,
    lsmSettings,
    telemetry: { traceSampleRate, traceRetentionDays },
    access: { mode: accessMode },
    limits: { apiCpuMs, compactorCpuMs },
  } satisfies CfSlatedbStackConfig;
});

/**
 * The Stack configuration of the stage being planned, resolved from this process's environment
 * (see {@link resolveCfSlatedbStackConfig}). The Stack resolves it before it declares anything, so
 * an invalid configuration stops the plan.
 */
export const cfSlatedbStackConfig = Effect.gen(function* () {
  const stage = yield* Stage;
  return yield* resolveCfSlatedbStackConfig(stage, process.env);
});

/** The part of a Stack configuration every engine Worker is bound (`bindLsmWorkerSettings`). */
export const cfSlatedbWorkerSettingsOf = (config: CfSlatedbStackConfig): LsmWorkerSettings => ({
  lsmSettings: config.lsmSettings,
  jurisdiction: config.jurisdiction === "none" ? Option.none() : Option.some(config.jurisdiction),
  bucketLocation:
    config.bucketLocation === "auto" ? Option.none() : Option.some(config.bucketLocation),
  traceSampleRate: config.telemetry.traceSampleRate,
});

/**
 * Every Worker's settings source: the Stack configuration's engine settings and jurisdiction. The
 * Stack has already resolved the same configuration before any Worker is declared, so a failure
 * here cannot happen after a successful resolve and is a defect.
 */
export const cfSlatedbWorkerSettings: LsmWorkerSettingsSource<Stage> = cfSlatedbStackConfig.pipe(
  Effect.map(cfSlatedbWorkerSettingsOf),
  Effect.orDie,
);

/**
 * A value of the Stack configuration for a Worker's props. Props Effects also run inside the deployed
 * Worker, where there is no deploy environment to resolve (its `process.env` holds packed bindings),
 * so outside planning this yields `atRuntime`, which nothing reads there.
 */
const atPlanTime = <A>(
  select: (config: CfSlatedbStackConfig) => A,
  atRuntime: A,
): Effect.Effect<A, never, Stage> =>
  Effect.gen(function* () {
    if ((yield* Alchemy.ALCHEMY_PHASE) !== "plan") return atRuntime;
    return select(yield* cfSlatedbStackConfig.pipe(Effect.orDie));
  });

/** Days the Stack configuration keeps traces and tail records (the trace bucket's lifecycle). */
export const cfSlatedbTraceRetentionDays: Effect.Effect<number, never, Stage> = atPlanTime(
  (config) => config.telemetry.traceRetentionDays,
  DEFAULT_TRACE_RETENTION_DAYS,
);

/** The CPU limit (ms per invocation) the Stack configuration gives the API Worker. */
export const cfSlatedbApiCpuLimit: Effect.Effect<number, never, Stage> = atPlanTime(
  (config) => config.limits.apiCpuMs,
  DEFAULT_API_CPU_MS,
);

/** The CPU limit (ms per invocation) the Stack configuration gives the compactor Worker. */
export const cfSlatedbCompactorCpuLimit: Effect.Effect<number, never, Stage> = atPlanTime(
  (config) => config.limits.compactorCpuMs,
  DEFAULT_COMPACTOR_CPU_MS,
);
