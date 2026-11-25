import { ConfigProvider, Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { inLsmJurisdiction, type LsmJurisdiction, lsmJurisdiction } from "./lsm-jurisdiction.ts";

/** A namespace that records the jurisdiction it was narrowed to. */
interface RecordingNamespace {
  readonly restrictedTo: Option.Option<LsmJurisdiction>;
  readonly jurisdiction: (jurisdiction: LsmJurisdiction) => RecordingNamespace;
}

const namespace = (restrictedTo: Option.Option<LsmJurisdiction>): RecordingNamespace => ({
  restrictedTo,
  jurisdiction: (jurisdiction) => namespace(Option.some(jurisdiction)),
});

const readJurisdiction = (environment: Record<string, string>) =>
  lsmJurisdiction.pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown(environment)),
    Effect.exit,
    Effect.runPromise,
  );

describe("Stack jurisdiction", () => {
  it("narrows every Durable Object namespace to the Stack's jurisdiction, or leaves it unrestricted", () => {
    expect(inLsmJurisdiction(namespace(Option.none()), Option.none()).restrictedTo).toEqual(
      Option.none(),
    );
    expect(inLsmJurisdiction(namespace(Option.none()), Option.some("eu")).restrictedTo).toEqual(
      Option.some("eu"),
    );
  });

  it("reads the setting, and refuses to start on a value that is not a jurisdiction", async () => {
    const unset = await readJurisdiction({});
    expect(unset._tag === "Success" ? unset.value : "failed").toEqual(Option.none());
    const eu = await readJurisdiction({ CF_SLATEDB_JURISDICTION: "eu" });
    expect(eu._tag === "Success" ? eu.value : "failed").toEqual(Option.some("eu"));
    const invalid = await readJurisdiction({ CF_SLATEDB_JURISDICTION: "mars" });
    expect(invalid._tag).toBe("Failure");
  });
});
