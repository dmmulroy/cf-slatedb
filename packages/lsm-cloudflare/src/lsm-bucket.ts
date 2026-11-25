import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import { Context, Effect, Layer } from "effect";

/** Alchemy stage that pins the bucket's production name and retains it. */
export const LSM_PRODUCTION_STAGE = "production";

/**
 * The R2 bucket that stores every database's manifests, SSTs, changelog, and GC files under
 * `db/<id>/`.
 *
 * **Details**
 *
 * Production would use the name `cf-slatedb` and be retained; every other stage gets a generated,
 * destroyable name. Under `alchemy dev` it is Alchemy's local R2 simulator.
 */
export const LsmBucket = Effect.gen(function* () {
  const stage = yield* Alchemy.Stage;
  const production = stage === LSM_PRODUCTION_STAGE;
  return yield* Cloudflare.R2.Bucket("LsmBucket", production ? { name: "cf-slatedb" } : {}).pipe(
    RemovalPolicy.retain(production),
  );
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
