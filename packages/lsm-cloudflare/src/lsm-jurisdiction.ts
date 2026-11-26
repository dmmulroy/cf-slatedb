import { Config, Effect, Option, Schema } from "effect";

/**
 * Schema for a shaledb Stack's data-residency jurisdiction: `eu` (the European Union) or
 * `fedramp` (FedRAMP-compliant data centers). R2, Durable Objects, and Workflows all support both.
 */
export const LsmJurisdiction = Schema.Literals(["eu", "fedramp"]);

/** A Stack's data-residency jurisdiction. */
export type LsmJurisdiction = typeof LsmJurisdiction.Type;

/** Schema for the jurisdiction setting's value: a jurisdiction, or `none` for no restriction. */
const LsmJurisdictionSetting = Schema.Literals(["eu", "fedramp", "none"]);

const jurisdictionOfSetting = (
  setting: typeof LsmJurisdictionSetting.Type,
): Option.Option<LsmJurisdiction> => (setting === "none" ? Option.none() : Option.some(setting));

/**
 * Name of the setting that carries the Stack's jurisdiction: `eu`, `fedramp`, or `none`. Set it in
 * the environment of the deploy; Alchemy binds it onto every Worker that reads it. Unset also means
 * no restriction, but a deploy that stops setting it leaves the previous value bound on the Workers
 * (Cloudflare keeps a script's secrets across uploads, D54), so deploys set `none` explicitly.
 */
export const LSM_JURISDICTION_SETTING = "SHALEDB_JURISDICTION";

/**
 * The Stack's jurisdiction (design §12, "Data residency"), one for the whole installation: it is
 * applied to the R2 buckets, every Durable Object namespace, and the compaction Workflow.
 *
 * **Details**
 *
 * Read with `Config`: at plan time from the deploy's environment, which binds it onto the Worker
 * whose init reads it, and at runtime from that binding, so planning and running agree.
 *
 * @returns The jurisdiction, or `None` when unset or `none`. Any other value is a defect that stops
 *   the Worker: a misconfigured residency must not start.
 */
export const lsmJurisdiction: Effect.Effect<Option.Option<LsmJurisdiction>> = Config.option(
  Config.schema(LsmJurisdictionSetting, LSM_JURISDICTION_SETTING),
).pipe(
  Effect.map((setting) => Option.flatMap(setting, jurisdictionOfSetting)),
  Effect.orDie,
);

const decodeJurisdictionText = Schema.decodeUnknownOption(LsmJurisdictionSetting);

/**
 * The jurisdiction in the process environment when this module loads, for props that must be known
 * at declaration time (the compaction Workflow's). At plan time this is the deploy's environment,
 * the same source {@link lsmJurisdiction} binds from; inside a deployed Worker it is unused.
 */
export const lsmJurisdictionAtDeclaration: Option.Option<LsmJurisdiction> = Option.flatMap(
  Option.flatMap(
    Option.fromUndefinedOr(globalThis.process?.env?.[LSM_JURISDICTION_SETTING]),
    decodeJurisdictionText,
  ),
  jurisdictionOfSetting,
);

/**
 * A Durable Object namespace narrowed to the Stack's jurisdiction, or the namespace itself when the
 * Stack has none. Every Durable Object a shaledb Worker addresses goes through this, so the same
 * name always reaches the same object; an object addressed in one jurisdiction is a different
 * object in another.
 */
export const inLsmJurisdiction = <
  N extends { readonly jurisdiction: (jurisdiction: LsmJurisdiction) => N },
>(
  namespace: N,
  jurisdiction: Option.Option<LsmJurisdiction>,
): N =>
  Option.match(jurisdiction, {
    onNone: () => namespace,
    onSome: (restricted) => namespace.jurisdiction(restricted),
  });
