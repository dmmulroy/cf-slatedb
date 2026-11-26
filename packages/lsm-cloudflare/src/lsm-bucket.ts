import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import { Config, Context, Effect, Layer, Option, Schema } from "effect";

import { lsmJurisdiction } from "./lsm-jurisdiction.ts";

/**
 * Schema for an R2 location hint: where an unrestricted Stack's bucket is created (western or
 * eastern North America, western or eastern Europe, Asia-Pacific, Oceania).
 */
export const LsmBucketLocation = Schema.Literals(["wnam", "enam", "weur", "eeur", "apac", "oc"]);

/** An R2 location hint for the LSM bucket. */
export type LsmBucketLocation = typeof LsmBucketLocation.Type;

/**
 * Name of the setting that carries the LSM bucket's location hint: a {@link LsmBucketLocation}, or
 * `auto` to let R2 place it (near the deploy that creates it). Fixed per installation: R2 cannot
 * move a bucket, so a change replaces it.
 */
export const LSM_BUCKET_LOCATION_SETTING = "CF_SLATEDB_BUCKET_LOCATION";

const LsmBucketLocationSetting = Schema.Literals(["auto", ...LsmBucketLocation.literals]);

/**
 * The LSM bucket's location hint, read with `Config` like the jurisdiction: `None` when unset or
 * `auto`. Any other value is a defect that stops the Worker.
 */
export const lsmBucketLocation: Effect.Effect<Option.Option<LsmBucketLocation>> = Config.option(
  Config.schema(LsmBucketLocationSetting, LSM_BUCKET_LOCATION_SETTING),
).pipe(
  Effect.map((setting) =>
    Option.flatMap(setting, (value) => (value === "auto" ? Option.none() : Option.some(value))),
  ),
  Effect.orDie,
);

/** Alchemy stage that pins the bucket's production name and retains it. */
export const LSM_PRODUCTION_STAGE = "production";

/**
 * The R2 bucket that stores every database's manifests, SSTs, changelog, and GC files under
 * `db/<id>/`.
 *
 * **Details**
 *
 * Production would use the name `cf-slatedb` and be retained; every other stage gets a generated
 * name and a bucket that destroying the stage empties and deletes. It is created in the Stack's jurisdiction (`lsmJurisdiction`), if any. Under
 * `alchemy dev` it is Alchemy's local R2 simulator.
 */
export const LsmBucket = Effect.gen(function* () {
  const stage = yield* Alchemy.Stage;
  const production = stage === LSM_PRODUCTION_STAGE;
  const jurisdiction = yield* lsmJurisdiction;
  const location = yield* lsmBucketLocation;
  return yield* Cloudflare.R2.Bucket("LsmBucket", {
    ...(production ? { name: "cf-slatedb" } : {}),
    // The Stack's jurisdiction (D49). Changing it replaces the bucket: it is fixed per installation.
    ...(Option.isSome(jurisdiction) ? { jurisdiction: jurisdiction.value } : {}),
    // Where an unrestricted Stack's writers are (D56). Changing it replaces the bucket.
    ...(Option.isSome(location) ? { locationHint: location.value } : {}),
    // Every other stage's data is disposable: destroying or replacing its bucket empties it first,
    // so a preview stays destroyable once it holds databases. Production keeps R2's refusal.
    forceDestroy: !production,
  }).pipe(RemovalPolicy.retain(production));
});

/** The read-write client of {@link LsmBucket} for the current Worker or Durable Object. */
export class LsmBucketClient extends Context.Service<
  LsmBucketClient,
  Cloudflare.R2.ReadWriteBucketClient
>()("@cf-slatedb/LsmBucketClient") {}

/**
 * Binds {@link LsmBucket} read-write to the Worker being initialized and provides its client.
 * Provide it in a Worker's init Effect, where Alchemy supplies the host Worker.
 */
export const lsmBucketClientLayer = Layer.effect(
  LsmBucketClient,
  Effect.gen(function* () {
    const bucket = yield* LsmBucket;
    return yield* Cloudflare.R2.ReadWriteBucket(bucket);
  }),
).pipe(Layer.provide(Cloudflare.R2.ReadWriteBucketBinding));
