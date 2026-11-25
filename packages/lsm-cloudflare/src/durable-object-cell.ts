import type * as cf from "@cloudflare/workers-types";
import { Effect, Option, Ref, Schema } from "effect";

/**
 * One value a Durable Object keeps in its storage under a fixed key, encoded with a Schema.
 *
 * **Details**
 *
 * Reads and writes go through the object's transactional key-value storage, so within one event
 * they are serialized by the Durable Object's input and output gates.
 */
export interface DurableObjectCell<A> {
  /** The stored value; none before the first `set`. */
  readonly get: Effect.Effect<Option.Option<A>>;
  /** Replaces the stored value. */
  readonly set: (value: A) => Effect.Effect<void>;
}

/**
 * A cell in a Durable Object's storage.
 *
 * @throws {SchemaError} As a defect when the stored value does not decode: only this code writes
 *   it, so a mismatch is a bug or a broken migration, not a runtime condition.
 */
export const durableObjectCell = <A, I>(
  storage: cf.DurableObjectStorage,
  key: string,
  schema: Schema.Codec<A, I>,
): DurableObjectCell<A> => {
  const decode = Schema.decodeUnknownEffect(schema);
  const encode = Schema.encodeEffect(schema);
  return {
    get: Effect.promise(() => storage.get(key)).pipe(
      Effect.flatMap((stored) =>
        stored === undefined
          ? Effect.succeed(Option.none<A>())
          : Effect.map(decode(stored), Option.some),
      ),
      Effect.orDie,
    ),
    set: (value) =>
      encode(value).pipe(
        Effect.flatMap((encoded) => Effect.promise(() => storage.put(key, encoded))),
        Effect.orDie,
      ),
  };
};

/** A cell held in memory, for tests. */
export const inMemoryCell = <A>(): Effect.Effect<DurableObjectCell<A>> =>
  Effect.map(Ref.make(Option.none<A>()), (ref) => ({
    get: Ref.get(ref),
    set: (value: A) => Ref.set(ref, Option.some(value)),
  }));
