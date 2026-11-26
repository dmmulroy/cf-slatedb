import { LsmSettings } from "@shaledb/lsm-core";
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import { CurrentRuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Option, Redacted, Schema } from "effect";

import { LSM_BUCKET_LOCATION_SETTING, type LsmBucketLocation } from "./lsm-bucket.ts";
import { LSM_SETTINGS_BINDING } from "./lsm-configuration.ts";
import { LSM_JURISDICTION_SETTING, type LsmJurisdiction } from "./lsm-jurisdiction.ts";
import { LSM_TRACE_SAMPLE_RATE_SETTING } from "./lsm-trace-policy.ts";

/**
 * Everything a deployment decides for the engine that every shaledb Worker must agree on: the
 * complete engine settings and the Stack's jurisdiction.
 */
export interface LsmWorkerSettings {
  /** The complete engine settings, defaults already merged with the deployment's overrides. */
  readonly lsmSettings: LsmSettings;
  /** The Stack's data-residency jurisdiction, or `None` for an unrestricted Stack. */
  readonly jurisdiction: Option.Option<LsmJurisdiction>;
  /** The LSM bucket's location hint, or `None` to let R2 place it. */
  readonly bucketLocation: Option.Option<LsmBucketLocation>;
  /** Fraction of traces kept, decided at each trace's root (D63). */
  readonly traceSampleRate: number;
}

/**
 * Yields a Worker's {@link LsmWorkerSettings} at plan time; the composition root (the Stack) owns
 * where they come from. `R` is what producing them needs at plan time. It is never run inside a
 * deployed Worker.
 */
export type LsmWorkerSettingsSource<R = never> = Effect.Effect<LsmWorkerSettings, never, R>;

const encodeLsmSettingsJson = Schema.encodeSync(Schema.fromJsonString(LsmSettings));

/**
 * The bindings that carry `settings` to a Worker, by binding name: the complete settings as JSON
 * under `SHALEDB_LSM_SETTINGS`, the jurisdiction (`eu`, `fedramp`, or `none`) under
 * `SHALEDB_JURISDICTION`, the bucket's location hint (or `auto`) under
 * `SHALEDB_BUCKET_LOCATION`, and the trace sample rate under `SHALEDB_TRACE_SAMPLE_RATE`. None is ever absent, so a deploy can never leave an earlier value
 * bound (D55).
 */
export const lsmWorkerSettingsBindings = (
  settings: LsmWorkerSettings,
): Readonly<Record<string, string>> => ({
  [LSM_SETTINGS_BINDING]: encodeLsmSettingsJson(settings.lsmSettings),
  [LSM_JURISDICTION_SETTING]: Option.getOrElse(settings.jurisdiction, () => "none"),
  [LSM_BUCKET_LOCATION_SETTING]: Option.getOrElse(settings.bucketLocation, () => "auto"),
  [LSM_TRACE_SAMPLE_RATE_SETTING]: String(settings.traceSampleRate),
});

/**
 * Binds the deployment's settings onto the Worker being initialized, at plan time: every key of
 * {@link lsmWorkerSettingsBindings}, always, whatever the deploy's environment holds. At runtime it
 * does nothing; the Worker reads the bindings back through `Config` (`lsmConfigurationLayer`,
 * `lsmJurisdiction`).
 *
 * **Details**
 *
 * Call it from a Worker's init Effect. It runs after the init's Layers, so it replaces whatever
 * Alchemy's `Config` interception bound from the environment while they were built. The values are
 * bound as `Redacted`, the form that interception uses, so a Stack deployed before D55 keeps the
 * same binding type (`secret_text`) instead of switching it on a live Worker. They are not secret.
 */
export const bindLsmWorkerSettings = <R>(source: LsmWorkerSettingsSource<R>) =>
  Effect.gen(function* () {
    if ((yield* Alchemy.ALCHEMY_PHASE) !== "plan") return;
    const runtimeContext = yield* CurrentRuntimeContext;
    if (runtimeContext === undefined) return;
    const bindings = lsmWorkerSettingsBindings(yield* source);
    for (const [name, value] of Object.entries(bindings)) {
      yield* runtimeContext.set(name, Output.literal(Redacted.make(value)));
    }
  });
