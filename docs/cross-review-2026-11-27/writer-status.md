2026-11-27T05:49:32Z started: reading BRIEF.md
2026-11-27T05:53:05Z read db-writer.ts in full, mem-table diff, alarm, writer-server; checking a candidate: a split resumed after its forks released their holds (seal re-takes holds; forkRange then fails)
2026-11-27T05:54:00Z FOUND: defect (a split resumed after its halves released their holds never completes, and leaks a hold). Three fixes, each correct alone, combine badly:

- D57: a seal with holdForSuccessors takes "a hold for each successor that has none yet".
- D77: each half releases its hold gcMinAge after its rewrite.
- D101: a routed request finishes an abandoned split by running the same steps again.
  Sequence: the split seals and forks both halves; its publish fails; the group is idle for more than gcMinAge, so both halves rewrite and release their holds. A request then resumes the split. The seal takes two NEW holds (a new commit pinning a new manifest). forkRange(left, new hold) finds LEFT existing with a different first-manifest ref, removes that hold, and fails DatabaseAlreadyExists, so the resume fails, every later resume too, and the sealed shard's range stays unwritable. The right half's new hold is left on the source forever: its holder has no reference to it, so nothing releases it, and the source can never be removed.
  Test: tests/split-resume-after-release.test-snippet.ts (append to database-lifecycle.test.ts). On fc9b7bb: resumed DatabaseAlreadyExistsError, 1 hold left behind; expected resumed, 0.
  Smallest fix (not committed): (1) DbWriter.seal's sealEdit takes successor holds only in the commit that seals (not when alreadySealed). (2) ShardSplit.split skips forking a half that already exists when the sealed manifest holds no hold for it, then publishes. Files: db-writer.ts, lsm-sharding shard-split.ts.
  2026-11-27T05:56:55Z checked: a defect in ensureAlarmBeforeCommit leaves committing=true; the next commit clears it (test passes): not a finding
