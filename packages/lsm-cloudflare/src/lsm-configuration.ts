import { DEFAULT_LSM_SETTINGS, LsmSettings } from "@shaledb/lsm-core";
import { Config, Context, Effect, Layer, Option, Schema } from "effect";

/** The engine's settings for the current Worker or Durable Object. */
export class LsmConfiguration extends Context.Service<LsmConfiguration, LsmSettings>()(
  "@shaledb/LsmConfiguration",
) {}

/**
 * Name of the setting that carries the engine's settings to a Worker: a JSON object of settings.
 * Deployments bind the complete settings (`bindLsmWorkerSettings`); a partial object, as tests and
 * deploys before D55 bound, overrides the defaults.
 */
export const LSM_SETTINGS_BINDING = "SHALEDB_LSM_SETTINGS";

const decodeLsmSettingsOverrides = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);
const decodeLsmSettings = Schema.decodeUnknownEffect(LsmSettings);

/**
 * Reads the settings once at the composition root: the defaults, overridden by the JSON object in
 * the {@link LSM_SETTINGS_BINDING} configuration value when it is set (deployments bind every
 * setting, so nothing falls back to a default there).
 *
 * @returns The settings; fails with a `ConfigError` when the value cannot be read.
 * @throws {SchemaError} As a startup defect when the override is not a JSON object of numbers that
 *   yields valid `LsmSettings`: a misconfigured Worker must not start.
 */
export const makeLsmConfiguration = Effect.gen(function* () {
  const text = yield* Config.String(LSM_SETTINGS_BINDING).pipe(Config.option);
  const overrides: Readonly<Record<string, number>> = Option.isSome(text)
    ? yield* decodeLsmSettingsOverrides(text.value).pipe(Effect.orDie)
    : {};
  return yield* decodeLsmSettings({ ...DEFAULT_LSM_SETTINGS, ...overrides }).pipe(Effect.orDie);
});

/** Provides the settings from configuration (see {@link makeLsmConfiguration}). */
export const lsmConfigurationLayerWithoutDependencies = Layer.effect(
  LsmConfiguration,
  makeLsmConfiguration,
);

/** Provides the settings from configuration. */
export const lsmConfigurationLayer = lsmConfigurationLayerWithoutDependencies;

/** Provides the default settings with `overrides` applied, for tests. */
export const lsmConfigurationTestLayer = (overrides: Partial<LsmSettings> = {}) =>
  Layer.succeed(LsmConfiguration, { ...DEFAULT_LSM_SETTINGS, ...overrides });
