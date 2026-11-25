import { Arbitrary, Effect, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CheckpointId, SstId } from "./ids.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";
import { encodeUlid, generateCheckpointId, generateSstId, ulidTimeMillis } from "./ulid.ts";

const time = Arbitrary.schema(
  Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 2 ** 48 - 1 })),
);
const symbols = Arbitrary.array(
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 31 }))),
  {
    minLength: 16,
    maxLength: 16,
  },
);

describe("ULIDs", () => {
  it("round-trip their time and are valid SST ids", () => {
    assertProperty(Arbitrary.all({ time, symbols }), ({ time, symbols }) => {
      const ulid = encodeUlid(time, symbols);
      return ulid.length === 26 && ulidTimeMillis(ulid) === time && Schema.is(SstId)(ulid);
    });
  });

  it("sort by time: a later millisecond always sorts after, whatever the random part", () => {
    assertProperty(
      Arbitrary.all({ earlier: time, gap: time, left: symbols, right: symbols }),
      ({ earlier, gap, left, right }) => {
        const later = Math.min(2 ** 48 - 1, earlier + Math.max(1, gap));
        return later === earlier || encodeUlid(earlier, left) < encodeUlid(later, right);
      },
    );
  });

  it("come from the contextual Clock and Random", async () => {
    const ids = await Effect.gen(function* () {
      yield* TestClock.setTime(1_700_000_000_000);
      const first = yield* generateSstId;
      const checkpoint = yield* generateCheckpointId;
      yield* TestClock.adjust("1 second");
      const second = yield* generateSstId;
      return { first, checkpoint, second };
    }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);
    expect(ulidTimeMillis(ids.first)).toBe(1_700_000_000_000);
    expect(ulidTimeMillis(ids.second)).toBe(1_700_000_001_000);
    expect(ids.first < ids.second).toBe(true);
    expect(Schema.is(CheckpointId)(ids.checkpoint)).toBe(true);
  });
});
