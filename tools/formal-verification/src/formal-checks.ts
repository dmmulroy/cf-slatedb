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
      { config: "ManifestCasFencing.cfg", broken: "none", expect: "pass", timeLimitSeconds: 600 },
      {
        config: "ManifestCasFencing.cfg",
        broken: "noWriterEpochCheck",
        expect: "NoStaleCommit",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencing.cfg",
        broken: "blindCreate",
        expect: "CreateOnce",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencingRace.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 480,
      },
      {
        config: "ManifestCasFencingRace.cfg",
        broken: "noInputsCheck",
        expect: "AckedDataVisible",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 600,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "noBoundaryReread",
        expect: "CommitOnce",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "hintOnlyProbe",
        expect: "LoadFindsHead",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencingGc.cfg",
        broken: "knownIgnoresBoundary",
        expect: "LoadFromKnownFindsHead",
        timeLimitSeconds: 240,
      },
      {
        config: "ManifestCasFencingRemove.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 600,
      },
      {
        config: "ManifestCasFencingRemove.cfg",
        broken: "removalDeletesHead",
        expect: "NoResurrection",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    name: "no-acked-write-lost",
    spec: "NoAckedWriteLost.tla",
    runs: [
      { config: "NoAckedWriteLost.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "truncateBeforeCommit",
        expect: "NoAckedWriteLost",
        timeLimitSeconds: 240,
      },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "nonAtomicProbe",
        expect: "LatestSeesAcked",
        timeLimitSeconds: 240,
      },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "trimUnflushed",
        expect: "NoAckedWriteLost",
        timeLimitSeconds: 240,
      },
      {
        config: "NoAckedWriteLost.cfg",
        broken: "noSnapshotUpperBound",
        expect: "LatestSeesAcked",
        timeLimitSeconds: 240,
      },
      { config: "NoAckedWriteLostLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "NoAckedWriteLostLive.cfg",
        broken: "noWake",
        expect: "EventuallyFlushed",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    name: "gc-safety",
    spec: "GcSafety.tla",
    runs: [
      { config: "GcSafety.cfg", broken: "none", expect: "pass", timeLimitSeconds: 600 },
      {
        config: "GcSafety.cfg",
        broken: "noBoundaryCheck",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 240,
      },
      {
        config: "GcSafety.cfg",
        broken: "ownUploadAge",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 240,
      },
      {
        config: "GcSafety.cfg",
        broken: "ignorePins",
        expect: "PinnedManifestsComplete",
        timeLimitSeconds: 240,
      },
      {
        config: "GcSafety.cfg",
        broken: "dropDeletesObject",
        expect: "ReadableManifestsComplete",
        timeLimitSeconds: 240,
      },
      // D116: two collections at once (D83); no variant fails here that passes with one.
      {
        config: "GcSafetyTwoCollectors.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 1200,
      },
      { config: "GcSafetyFork.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "GcSafetyFork.cfg",
        broken: "earlyForkRelease",
        expect: "ForkReadsComplete",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    name: "seal-split",
    spec: "SealSplit.tla",
    runs: [
      { config: "SealSplit.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "SealSplit.cfg",
        broken: "sealWithoutLock",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 240,
      },
      {
        config: "SealSplit.cfg",
        broken: "writerNotFenced",
        expect: "NoCommitAfterSeal",
        timeLimitSeconds: 240,
      },
      {
        config: "SealSplit.cfg",
        broken: "checkpointBeforeSeal",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 240,
      },
      {
        config: "SealSplit.cfg",
        broken: "overlappingRanges",
        expect: "EveryAckedWriteInExactlyOneSuccessor",
        timeLimitSeconds: 240,
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
        timeLimitSeconds: 240,
      },
      {
        config: "Transactions.cfg",
        broken: "trimWithoutAbort",
        expect: "Serializable",
        timeLimitSeconds: 240,
      },
      {
        config: "Transactions.cfg",
        broken: "noReadHorizonCheck",
        expect: "ReadsAtBegin",
        timeLimitSeconds: 240,
      },
      {
        config: "Transactions.cfg",
        broken: "noReadUpperBound",
        expect: "ReadsAtBegin",
        timeLimitSeconds: 240,
      },
      // D135: the writer's rows taken beside a manifest, as getAt and scanAt were.
      { config: "TransactionsMerge.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "TransactionsMerge.cfg",
        broken: "noPartition",
        expect: "ReadsAtBegin",
        timeLimitSeconds: 240,
      },
      {
        config: "TransactionsMerge.cfg",
        broken: "dedupeBySeq",
        expect: "ReadsAtBegin",
        timeLimitSeconds: 240,
      },
      { config: "TransactionsLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "TransactionsLive.cfg",
        broken: "beforeD76",
        expect: "EveryTxnCommits",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    // D110: the compactor Durable Object at await granularity, with its jobs, lease, GC, eviction.
    name: "compactor-object",
    spec: "CompactorObject.tla",
    runs: [
      { config: "CompactorObject.cfg", broken: "none", expect: "pass", timeLimitSeconds: 1200 },
      {
        config: "CompactorObject.cfg",
        broken: "twoFenceIns",
        expect: "EpochNeverRegresses",
        timeLimitSeconds: 480,
      },
      {
        config: "CompactorObject.cfg",
        broken: "noJobRecord",
        expect: "OutcomeTrue",
        timeLimitSeconds: 480,
      },
      {
        config: "CompactorObject.cfg",
        broken: "noStateLock",
        expect: "RunningStaysRecorded",
        timeLimitSeconds: 480,
      },
      { config: "CompactorObjectDrop.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "CompactorObjectDrop.cfg",
        broken: "noPlanLock",
        expect: "DroppedStaysDropped",
        timeLimitSeconds: 480,
      },
      {
        config: "CompactorObjectDrop.cfg",
        broken: "launchBeforeRecord",
        expect: "DroppedStaysDropped",
        timeLimitSeconds: 480,
      },
      { config: "CompactorObjectL0.cfg", broken: "none", expect: "pass", timeLimitSeconds: 1200 },
      {
        config: "CompactorObjectL0.cfg",
        broken: "beforeD78",
        expect: "L0Drains",
        timeLimitSeconds: 1200,
      },
      {
        config: "CompactorObjectL0.cfg",
        broken: "gcIgnoresRunning",
        expect: "L0Drains",
        timeLimitSeconds: 1200,
      },
      { config: "CompactorObjectJobs.cfg", broken: "none", expect: "pass", timeLimitSeconds: 1200 },
      {
        config: "CompactorObjectJobs.cfg",
        broken: "noUnseenBound",
        expect: "RunningJobsLeave",
        timeLimitSeconds: 1200,
      },
    ],
  },
  {
    // D112: a shard split end to end, with crashes, stale routers, and the change feed.
    name: "shard-split",
    spec: "ShardSplit.tla",
    runs: [
      { config: "ShardSplit.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "ShardSplit.cfg",
        broken: "noFlushedRefresh",
        expect: "NoStaleBeyondBound",
        timeLimitSeconds: 240,
      },
      {
        config: "ShardSplit.cfg",
        broken: "resealOthers",
        expect: "HoldsMatchSeal",
        timeLimitSeconds: 240,
      },
      {
        config: "ShardSplit.cfg",
        broken: "feedGap",
        expect: "ConsumerMissesNothing",
        timeLimitSeconds: 240,
      },
      {
        config: "ShardSplit.cfg",
        broken: "noDirectoryLock",
        expect: "PublishedIsSealed",
        timeLimitSeconds: 480,
      },
      {
        // D136: a repeated forkRange of a half that exists, as the code was before.
        config: "ShardSplit.cfg",
        broken: "forkRetryUndoesHold",
        expect: "ForkKeepsHold",
        timeLimitSeconds: 240,
      },
      { config: "ShardSplitLiveness.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "ShardSplitLiveness.cfg",
        broken: "noResume",
        expect: "WaitingWritesLand",
        timeLimitSeconds: 240,
      },
      {
        // D138: a split resumed after a half released its hold, as the code was before.
        config: "ShardSplit.cfg",
        broken: "resealTakesHolds",
        expect: "NoOrphanHold",
        timeLimitSeconds: 240,
      },
      {
        config: "ShardSplitLiveness.cfg",
        broken: "noSkipReleased",
        expect: "WaitingWritesLand",
        timeLimitSeconds: 240,
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
        timeLimitSeconds: 960,
      },
      {
        config: "Lifecycle.cfg",
        broken: "releaseNewestOnly",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "Lifecycle.cfg",
        broken: "releaseIgnoresPins",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "Lifecycle.cfg",
        broken: "deleteHold",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "Lifecycle.cfg",
        broken: "deleteHead",
        expect: "RemovedNeverReopens",
        timeLimitSeconds: 480,
      },
      {
        config: "Lifecycle.cfg",
        broken: "recreateAtOnce",
        expect: "RemovedNeverReopens",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecycleServed.cfg",
        broken: "recreateAtOnce",
        expect: "NoOldIncarnationServed",
        timeLimitSeconds: 480,
      },
      { config: "LifecycleRemovers.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "LifecycleRemovers.cfg",
        broken: "deleteByPrefix",
        expect: "NewIncarnationIntact",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecycleRemovers.cfg",
        broken: "staleViewOutlives",
        expect: "RemovedNeverReopens",
        timeLimitSeconds: 480,
      },
      {
        config: "Lifecycle.cfg",
        broken: "beforeD116",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecycleLiveness.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecyclePrefix.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 480,
      },
      // D136: forkRange called again after the fork moved on.
      { config: "LifecycleRetry.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "LifecycleRetry.cfg",
        broken: "compensateAnyHold",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecycleRetry.cfg",
        broken: "retryTrustsFirstManifest",
        expect: "NoReaderNeedsDeleted",
        timeLimitSeconds: 480,
      },
      {
        config: "LifecycleRetryLiveness.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 480,
      },
    ],
  },
  {
    name: "writer-object",
    spec: "WriterObject.tla",
    runs: [
      { config: "WriterObjectSeal.cfg", broken: "none", expect: "pass", timeLimitSeconds: 1440 },
      {
        config: "WriterObjectSeal.cfg",
        broken: "alarmAfterCommit",
        expect: "AlarmCovers",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "alarmBeforeMark",
        expect: "AlarmCovers",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "schedulerIgnoresCommit",
        expect: "AlarmCovers",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "sealKeepsPhase",
        expect: "SealNotHalf",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "beforeD115",
        expect: "NeverSelfFenced",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "markFencedAnyPhase",
        expect: "NeverSelfFenced",
        timeLimitSeconds: 360,
      },
      {
        config: "WriterObjectSeal.cfg",
        broken: "noAlarmRetry",
        expect: "AlarmCovers",
        timeLimitSeconds: 240,
      },
      { config: "WriterObjectTxn.cfg", broken: "none", expect: "pass", timeLimitSeconds: 720 },
      {
        config: "WriterObjectTxn.cfg",
        broken: "noHorizonReread",
        expect: "Serializable",
        timeLimitSeconds: 360,
      },
      {
        config: "WriterObjectTxn.cfg",
        broken: "noGeneration",
        expect: "CountersCover",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectTxn.cfg",
        broken: "interruptedWrite",
        expect: "CountersCover",
        timeLimitSeconds: 240,
      },
      { config: "WriterObjectLive.cfg", broken: "none", expect: "pass", timeLimitSeconds: 480 },
      {
        config: "WriterObjectLive.cfg",
        broken: "noAlarmRetry",
        expect: "AckedFlushed",
        timeLimitSeconds: 240,
      },
      {
        config: "WriterObjectAlarmLoop.cfg",
        broken: "none",
        expect: "pass",
        timeLimitSeconds: 480,
      },
      {
        config: "WriterObjectAlarmLoop.cfg",
        broken: "noSettle",
        expect: "AlarmQuiets",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    name: "change-feed",
    spec: "ChangeFeed.tla",
    runs: [
      { config: "ChangeFeed.cfg", broken: "none", expect: "pass", timeLimitSeconds: 240 },
      {
        config: "ChangeFeed.cfg",
        broken: "lostResponseReexport",
        expect: "NoOverlappingEntries",
        timeLimitSeconds: 240,
      },
      {
        config: "ChangeFeed.cfg",
        broken: "noExpiredCheck",
        expect: "ExactlyOnce",
        timeLimitSeconds: 240,
      },
      {
        config: "ChangeFeed.cfg",
        broken: "appendAfterCut",
        expect: "ExactlyOnce",
        timeLimitSeconds: 240,
      },
      {
        config: "ChangeFeedOldData.cfg",
        broken: "beforeD113",
        expect: "ExactlyOnce",
        timeLimitSeconds: 240,
      },
      {
        config: "ChangeFeedOldData.cfg",
        broken: "lostResponseReexport",
        expect: "pass",
        timeLimitSeconds: 240,
      },
    ],
  },
  {
    name: "paged-scan",
    spec: "PagedScan.tla",
    runs: [
      { config: "PagedScanLatest.cfg", broken: "none", expect: "pass", timeLimitSeconds: 720 },
      {
        config: "PagedScanLatest.cfg",
        broken: "restartAfterRows",
        expect: "LatestPageAtSnapshot",
        timeLimitSeconds: 240,
      },
      { config: "PagedScanTxn.cfg", broken: "none", expect: "pass", timeLimitSeconds: 720 },
      {
        config: "PagedScanTxn.cfg",
        broken: "pageRangesOnly",
        expect: "PhantomRefused",
        timeLimitSeconds: 360,
      },
      // D135: the writer's rows taken beside a manifest, as the point read and the scan were.
      {
        config: "PagedScanLatest.cfg",
        broken: "noPartition",
        expect: "LatestPageAtSnapshot",
        timeLimitSeconds: 480,
      },
      {
        config: "PagedScanLatest.cfg",
        broken: "dedupeBySeq",
        expect: "LatestPageAtSnapshot",
        timeLimitSeconds: 480,
      },
      {
        config: "PagedScanTxn.cfg",
        broken: "noPartition",
        expect: "TxnPagesAtReadSeq",
        timeLimitSeconds: 480,
      },
      {
        config: "PagedScanTxn.cfg",
        broken: "dedupeBySeq",
        expect: "TxnPagesAtReadSeq",
        timeLimitSeconds: 480,
      },
    ],
  },
];
