import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { HttpClient } from "effect/http";

import { workerUrl } from "../telemetry/delayed-otlp-collector.ts";
import { objectStoreProbeStack } from "./object-store-probe-worker.ts";

const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "test-object-store",
});

const PutIfAbsentResult = Schema.Struct({
  outcomes: Schema.Array(Schema.String),
  stored: Schema.NullOr(Schema.Array(Schema.Number)),
});
const RaceResult = Schema.Struct({ id: Schema.Number, l0: Schema.Number });

test.provider(
  "local R2 honors If-None-Match: * from a Headers object, and manifest CAS loses no edit",
  (stack) =>
    Effect.gen(function* () {
      const deployed = yield* stack.deploy(objectStoreProbeStack);
      const client = yield* HttpClient.HttpClient;
      const base = workerUrl(deployed.url);

      const putResponse = yield* client.get(`${base}/put-if-absent/db%2Fprobe%2Fmanifest%2F1`);
      const put = yield* Schema.decodeUnknownEffect(PutIfAbsentResult)(yield* putResponse.json);
      expect(put.outcomes).toEqual(["Created", "AlreadyExists"]);
      expect(put.stored).toEqual([1]);

      const raceResponse = yield* client.get(`${base}/manifest-race/race-db/16`);
      const race = yield* Schema.decodeUnknownEffect(RaceResult)(yield* raceResponse.json);
      expect(race).toEqual({ id: 17, l0: 16 });
      yield* stack.destroy();
    }),
  { timeout: 120_000 },
);
