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
      {
        config: "ManifestCasFencingRemove.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 150,
      },
      {
        config: "ManifestCasFencingRemove.cfg",
        broken: "removalDeletesHead",
        expect: "NoResurrection",
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
      {
        config: "NoAckedWriteLost.cfg",
        broken: "noSnapshotUpperBound",
        expect: "LatestSeesAcked",
        timeLimitSeconds: 60,
      },
      { config: "NoAckedWriteLostLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 120 },
      {
        config: "NoAckedWriteLostLive.cfg",
        broken: "noWake",
        expect: "EventuallyFlushed",
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
      { config: "GcSafetyFork.cfg", broken: "none", expect: "pass", timeLimitSeconds: 120 },
      {
        config: "GcSafetyFork.cfg",
        broken: "earlyForkRelease",
        expect: "ForkReadsComplete",
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
      { config: "Transactions.cfg", broken: "none", expect: "pass", timeLimitSeconds: 900 },
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
        expect: "ReadsAtBegin",
        timeLimitSeconds: 60,
      },
      {
        config: "Transactions.cfg",
        broken: "noReadUpperBound",
        expect: "ReadsAtBegin",
        timeLimitSeconds: 60,
      },
      { config: "TransactionsLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 120 },
      {
        config: "TransactionsLive.cfg",
        broken: "beforeD76",
        expect: "EveryTxnCommits",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    // D110: the compactor Durable Object at await granularity, with its jobs, lease, GC, eviction.
    name: "compactor-object",
    spec: "CompactorObject.tla",
    runs: [
      { config: "CompactorObject.cfg", broken: "none", expect: "pass", timeLimitSeconds: 300 },
      {
        config: "CompactorObject.cfg",
        broken: "twoFenceIns",
        expect: "EpochNeverRegresses",
        timeLimitSeconds: 120,
      },
      {
        config: "CompactorObject.cfg",
        broken: "noJobRecord",
        expect: "OutcomeTrue",
        timeLimitSeconds: 120,
      },
      { config: "CompactorObjectL0.cfg", broken: "none", expect: "pass", timeLimitSeconds: 300 },
      {
        config: "CompactorObjectL0.cfg",
        broken: "beforeD78",
        expect: "L0Drains",
        timeLimitSeconds: 300,
      },
      {
        config: "CompactorObjectL0.cfg",
        broken: "gcIgnoresRunning",
        expect: "L0Drains",
        timeLimitSeconds: 300,
      },
      { config: "CompactorObjectJobs.cfg", broken: "none", expect: "pass", timeLimitSeconds: 300 },
      {
        config: "CompactorObjectJobs.cfg",
        broken: "noUnseenBound",
        expect: "RunningJobsLeave",
        timeLimitSeconds: 300,
      },
    ],
  },
  {
    // D112: a shard split end to end, with crashes, stale routers, and the change feed.
    name: "shard-split",
    spec: "ShardSplit.tla",
    runs: [
      { config: "ShardSplit.cfg", broken: "none", expect: "pass", timeLimitSeconds: 60 },
      {
        config: "ShardSplit.cfg",
        broken: "noFlushedRefresh",
        expect: "NoStaleBeyondBound",
        timeLimitSeconds: 60,
      },
      {
        config: "ShardSplit.cfg",
        broken: "resealOthers",
        expect: "HoldsMatchSeal",
        timeLimitSeconds: 60,
      },
      {
        config: "ShardSplit.cfg",
        broken: "feedGap",
        expect: "ConsumerMissesNothing",
        timeLimitSeconds: 60,
      },
      { config: "ShardSplitLiveness.cfg", broken: "none", expect: "pass", timeLimitSeconds: 60 },
      {
        config: "ShardSplitLiveness.cfg",
        broken: "noResume",
        expect: "WaitingWritesLand",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    // D114: create, remove, recreate, checkpoints and fork holds across three databases.
    name: "lifecycle",
    spec: "Lifecycle.tla",
    runs: [
      {
        config: "Lifecycle.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "releaseNewestOnly",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "releaseIgnoresPins",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "deleteHold",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "deleteHead",
        expect: "RemovedNeverReopens",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "recreateAtOnce",
        expect: "NoOldIncarnationServed",
        timeLimitSeconds: 120,
      },
      {
        config: "Lifecycle.cfg",
        broken: "beforeD116",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 120,
      },
      {
        config: "LifecycleLiveness.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 120,
      },
      {
        config: "LifecyclePrefix.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 120,
      },
    ],
  },
  {
    name: "writer-object",
    spec: "WriterObject.tla",
    runs: [
      { config: "WriterObjectSeal.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "WriterObjectSeal.cfg",
        broken: "sealKeepsPhase",
        expect: "SealNotHalf",
        timeLimitSeconds: 60,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "beforeD115",
        expect: "NeverSelfFenced",
        timeLimitSeconds: 60,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "markFencedAnyPhase",
        expect: "NeverSelfFenced",
        timeLimitSeconds: 90,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "noAlarmRetry",
        expect: "AlarmCovers",
        timeLimitSeconds: 60,
      },
      { config: "WriterObjectTxn.cfg", broken: "none", expect: "pass", timeLimitSeconds: 180 },
      {
        config: "WriterObjectTxn.cfg",
        broken: "noHorizonReread",
        expect: "Serializable",
        timeLimitSeconds: 90,
      },
      {
        config: "WriterObjectTxn.cfg",
        broken: "noGeneration",
        expect: "CountersCover",
        timeLimitSeconds: 60,
      },
      {
        config: "WriterObjectTxn.cfg",
        broken: "interruptedWrite",
        expect: "CountersCover",
        timeLimitSeconds: 60,
      },
      { config: "WriterObjectLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 120 },
      {
        config: "WriterObjectLive.cfg",
        broken: "noAlarmRetry",
        expect: "AckedFlushed",
        timeLimitSeconds: 60,
      },
      {
        config: "WriterObjectAlarmLoop.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 120,
      },
      {
        config: "WriterObjectAlarmLoop.cfg",
        broken: "noSettle",
        expect: "AlarmQuiets",
        timeLimitSeconds: 60,
      },
    ],
  },
  {
    name: "change-feed",
    spec: "ChangeFeed.tla",
    runs: [
      { config: "ChangeFeed.cfg", broken: "none", expect: "pass", timeLimitSeconds: 60 },
      {
        config: "ChangeFeed.cfg",
        broken: "lostResponseReexport",
        expect: "NoOverlappingEntries",
        timeLimitSeconds: 60,
      },
      {
        config: "ChangeFeed.cfg",
        broken: "noExpiredCheck",
        expect: "ExactlyOnce",
        timeLimitSeconds: 60,
      },
      {
        config: "ChangeFeed.cfg",
        broken: "appendAfterCut",
        expect: "ExactlyOnce",
        timeLimitSeconds: 60,
      },
      {
        config: "ChangeFeedOldData.cfg",
        broken: "beforeD113",
        expect: "ExactlyOnce",
        timeLimitSeconds: 60,
      },
      {
        config: "ChangeFeedOldData.cfg",
        broken: "lostResponseReexport",
        expect: "pass",
        timeLimitSeconds: 60,
      },
    ],
  },
];
