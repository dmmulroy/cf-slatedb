# Cross-review of `f3b2ad2..fc9b7bb`, 2026-11-27: open findings

Five reviewers each read files they did not write, at `fc9b7bb`, after about forty fixes
(D75 to D134) had landed in one night. The review was stopped before any fix was made. Each
finding below is **open on `main`**. The reviewers' status logs, reports, and failing tests are
in this directory as they left them (tests are stored as `.txt` so they are not run or linted;
each says where to copy it).

| Class                               | Finding                                                                                                                                                                                                                  | Evidence                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- |
| defect: lost data                   | A compaction job declared finished (D94, D103) can still commit later, after the scheduler has reused its sorted-run ids: `commitCompaction` checks run ids, not the input SSTs, so the late commit replaces newer runs. | `compaction-compaction-late-commit.test.ts.txt`         |
| defect: unbounded work              | A scan page's "examined at least one key" flag is per `DbReader.scan` call, and D93 makes one call per segment, so a limit-1 page over keys with more than 1,000 unflushed versions never ends on its budget (D125).     | `api-xreview-scan-budget-segments.test.ts.txt`          |
| defect: wrong answer                | A routed scan returns duplicate rows when a split lands between writer pages: the routing-error retry rescans the range from its start (D93 with D59).                                                                   | `api-xreview-routed-scan-split.test.ts.txt`             |
| defect: unavailability, leaked hold | A split resumed (D101) after its halves released their holds (D77) never completes: the seal takes new holds, the fork of an existing half fails, and one hold is left on the source.                                    | `writer-split-resume-after-release.test-snippet.ts.txt` |
| defect: bounded leak                | A compactor woken after its database was removed fences in on the tombstone and compacts it: `fenceCompactor` does not refuse a `Removed` manifest.                                                                      | `lifecycle-status.md`                                   |
| risk                                | A tombstone without `removedAt` never expires, contrary to D105's rollback table.                                                                                                                                        | `lifecycle-status.md`                                   |
| risk                                | A Workflow status that reads as running for ever keeps D96's collection cap pinned.                                                                                                                                      | `compaction-compactor-running-forever.snippet.ts.txt`   |
| risk (low)                          | A defect in the `BlockServer` arm of a hedged read is handed to the bucket arm like a failure (D134).                                                                                                                    | `storage-status.md`                                     |
| risk (low)                          | `withAbandonedSplitFinished` swallows why a resume failed.                                                                                                                                                               | `api-status.md`                                         |
| docs                                | Four comments in `db-client.ts` and `db-reader.ts` that later fixes made stale.                                                                                                                                          | `api-status.md`                                         |

The lifecycle, writer, and storage reviewers had not written reports when the work stopped;
their status logs are the record.
