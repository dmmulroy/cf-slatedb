/**
 * Every formal check `vp run verify` runs: a TLA+ spec, a TLC config, and the expected outcome
 * for the code as written ("pass") and for each deliberately broken variant (the invariant that
 * must fail). A check's verify skill in `.agents/skills/verify-*` explains what it covers.
 */

/** One TLC run and the outcome it must have. */
export interface FormalCheckRun {
  /** Config file in `specs/`. */
  readonly config: string;
  /** Value of the spec's `Broken` constant: `"none"` for the code as written. */
  readonly broken: string;
  /** `"pass"`, or the name of the invariant or property that must be violated. */
  readonly expect: string;
  /** Hard time limit in seconds; a run that exceeds it fails the verification. */
  readonly timeLimitSeconds: number;
}

/** A spec and its runs. */
export interface FormalCheck {
  /** Short name, matching `.agents/skills/verify-<name>`. */
  readonly name: string;
  /** Spec file in `specs/`. */
  readonly spec: string;
  readonly runs: ReadonlyArray<FormalCheckRun>;
}

/** Every formal check, in the order `vp run verify` runs them. */
export const FORMAL_CHECKS: ReadonlyArray<FormalCheck> = [
  {
    name: "manifest-cas-fencing",
    spec: "ManifestCasFencing.tla",
    runs: [
      { config: "ManifestCasFencing.cfg", broken: "none", expect: "pass", timeLimitSeconds: 150 },
      {
        config: "ManifestCasFencing.cfg",
        broken: "noWriterEpochCheck",
        expect: "NoStaleCommit",
        timeLimitSeconds: 60,
      },
      {
        config: "ManifestCasFencing.cfg",
        broken: "blindCreate",
        expect: "CreateOnce",
        timeLimitSeconds: 60,
      },
      {
        config: "ManifestCasFencingRace.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 120,
      },
      {
        config: "ManifestCasFencingRace.cfg",
        broken: "noInputsCheck",
        expect: "AckedDataVisible",
        timeLimitSeconds: 60,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 150,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "noBoundaryReread",
        expect: "CommitOnce",
        timeLimitSeconds: 60,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "hintOnlyProbe",
        expect: "LoadFindsHead",
        timeLimitSeconds: 60,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "knownIgnoresBoundary",
        expect: "LoadFromKnownFindsHead",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    name: "no-acked-write-lost",
    spec: "NoAckedWriteLost.tla",
    runs: [
      { config: "NoAckedWriteLost.cfg", broken: "none", expect: "pass", timeLimitSeconds: 120 },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "truncateBeforeCommit",
        expect: "NoAckedWriteLost",
        timeLimitSeconds: 60,
      },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "nonAtomicProbe",
        expect: "LatestSeesAcked",
        timeLimitSeconds: 60,
      },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "trimUnflushed",
        expect: "NoAckedWriteLost",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    name: "gc-safety",
    spec: "GcSafety.tla",
    runs: [
      { config: "GcSafety.cfg", broken: "none", expect: "pass", timeLimitSeconds: 150 },
      {
        config: "GcSafety.cfg",
        broken: "noBoundaryCheck",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 60,
      },
      {
        config: "GcSafety.cfg",
        broken: "ownUploadAge",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 60,
      },
      {
        config: "GcSafety.cfg",
        broken: "ignorePins",
        expect: "PinnedManifestsComplete",
        timeLimitSeconds: 60,
      },
      {
        config: "GcSafety.cfg",
        broken: "dropDeletesObject",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    name: "seal-split",
    spec: "SealSplit.tla",
    runs: [
      { config: "SealSplit.cfg", broken: "none", expect: "pass", timeLimitSeconds: 60 },
      {
        config: "SealSplit.cfg",
        broken: "sealWithoutLock",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 60,
      },
      {
        config: "SealSplit.cfg",
        broken: "writerNotFenced",
        expect: "NoCommitAfterSeal",
        timeLimitSeconds: 60,
      },
      {
        config: "SealSplit.cfg",
        broken: "checkpointBeforeSeal",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 60,
      },
      {
        config: "SealSplit.cfg",
        broken: "overlappingRanges",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    name: "transactions",
    spec: "Transactions.tla",
    runs: [
      { config: "Transactions.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "Transactions.cfg",
        broken: "snapshotOnly",
        expect: "Serializable",
        timeLimitSeconds: 60,
      },
      {
        config: "Transactions.cfg",
        broken: "trimWithoutAbort",
        expect: "Serializable",
        timeLimitSeconds: 60,
      },
      {
        config: "Transactions.cfg",
        broken: "noReadHorizonCheck",
        expect: "Serializable",
        timeLimitSeconds: 60,
      },
    ],
  },
];
