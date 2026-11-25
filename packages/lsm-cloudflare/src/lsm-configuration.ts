import { DEFAULT_LSM_SETTINGS, LsmSettings } from "@cf-slatedb/lsm-core";
import { Config, Context, Effect, Layer, Option, Schema } from "effect";

/** The engine's settings for the current Worker or Durable Object. */
export class LsmConfiguration extends Context.Service<LsmConfiguration, LsmSettings>()(
  "@cf-slatedb/LsmConfiguration",
) {}

const decodeLsmSettingsOverrides = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);
const decodeLsmSettings = Schema.decodeUnknownEffect(LsmSettings);

/**
 * Reads the settings once at the composition root: the defaults, overridden by the JSON object in
 * the `CF_SLATEDB_LSM_SETTINGS` configuration value when it is set.
 *
 * @returns The settings; fails with a `ConfigError` when the value cannot be read, or a
 *   `SchemaError` when the override is not a JSON object of numbers that yields valid `LsmSettings`.
 */
export const makeLsmConfiguration = Effect.gen(function* () {
  const text = yield* Config.String("CF_SLATEDB_LSM_SETTINGS").pipe(Config.option);
  const overrides: Readonly<Record<string, number>> = Option.isSome(text)
    ? yield* decodeLsmSettingsOverrides(text.value)
    : {};
  return yield* decodeLsmSettings({ ...DEFAULT_LSM_SETTINGS, ...overrides });
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
