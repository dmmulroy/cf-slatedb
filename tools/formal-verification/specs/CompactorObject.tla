---------------------------- MODULE CompactorObject ----------------------------
(***************************************************************************)
(* One database's compactor Durable Object at await granularity, with its  *)
(* Workflow jobs, its lease, the writer's flushes, eviction, and GC.       *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - compactor.ts ensureState (wake, requestCollection, dropSst, and the  *)
(*    alarm's re-fence, D78): under startLock, read the cell; with no      *)
(*    state, or a fenced one, commit fenceCompactor (an R2 await) and      *)
(*    write the new epoch, releasing the old epoch's leases.               *)
(*  - compactor.ts runAlarm -> poll, under planLock: read the cell, load   *)
(*    the manifest (an await), read each running job's progress, plan and *)
(*    start a job (budget.acquire, launcher.start: awaits), then write the *)
(*    state through updateState with an epoch guard (D78). A newer epoch   *)
(*    in the manifest marks the state fenced. A job no progress read has   *)
(*    seen running for compactionLeaseMs is finished (D94); a missing      *)
(*    Workflow instance is finished at once (D103).                        *)
(*  - compaction-job.ts / compaction-executor.ts commit: the job's epoch   *)
(*    must be the manifest's, its inputs present, and its outputs above   *)
(*    gcSstBoundary; a retried commit is recognized by the job id the      *)
(*    manifest records (D103).                                             *)
(*  - garbage-collector.ts with D96's cap: an unreferenced SST older than  *)
(*    gcMinAge is deleted unless a job the compactor records as running    *)
(*    may have written it.                                                 *)
(*  - The writer flushes into L0 up to l0MaxSsts, and wakes the compactor  *)
(*    while L0 can be compacted (its flush, and its stalled-write wake).   *)
(*  - Eviction: in-memory state is lost (pcs, locks); the cell, the alarm, *)
(*    the manifest, and Workflow instances stay. A started job the poll    *)
(*    never recorded holds its lease until the lease expires.              *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "twoFenceIns"        ensureState without startLock (D78, first half)  *)
(*   "beforeD78"          the code before D78: no startLock, the poll      *)
(*                        writes back the state it read, and a fenced      *)
(*                        compactor never fences in again                  *)
(* The stale write and the terminal fence are unreachable once startLock   *)
(* is held: only this object fences in, so no fence-in can overtake a poll *)
(* and no stored state is ever fenced. They are defense in depth here, so  *)
(* their variants are checked together with the missing lock.              *)
(*   "noUnseenBound"      an unreadable job counts as running forever (D94)*)
(*   "noJobRecord"        a retried commit is not recognized (D103)        *)
(*   "gcIgnoresRunning"   GC deletes a running job's outputs (D96)         *)
(*   "noStateLock"        updateState without stateLock: its cell.get and  *)
(*                        cell.set are two awaits, so a collection's write *)
(*                        can put back the state it read                   *)
(*   "noPlanLock"         dropSst without planLock (D62)                   *)
(*   "launchBeforeRecord" the poll launches a job before recording it, so  *)
(*                        an eviction between leaves it unrecorded (D116)  *)
(*                                                                         *)
(* A collection (the alarm's, or an operator's collect running beside a   *)
(* poll) reads the running jobs for D96's cap, collects, and writes when  *)
(* the next is due. dropSst (an operator's, D62) checks the running jobs   *)
(* and commits the drop of one SST of a sorted run.                       *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxL0, MaxFlushes, MaxEpoch, MaxEvictions, Broken,
  Ops  \* the processes beside the poll a config runs: "collect" (a collection), "drop" (dropSst)

ASSUME Broken \in {"none", "twoFenceIns", "beforeD78", "noUnseenBound", "noJobRecord",
                   "gcIgnoresRunning", "noStateLock", "noPlanLock", "launchBeforeRecord"}

Wakes == {"w1", "w2"}
Jobs == {1, 2}
NoJob == 0
\* A compaction merges at least this many L0 SSTs.
MinSources == 2

VARIABLES
  mEpoch,     \* the manifest's compactor epoch
  l0,         \* L0 SSTs in the manifest
  flushes,    \* flushes so far (bounded)
  cell,       \* the stored compactor state
  startLock,  \* "free" or the process holding it
  wpc,        \* each wake: "idle" | "read" | "fence" | "write"
  wepoch,     \* the epoch a wake fenced in with
  ppc,        \* the poll: "idle" | "loaded" | "recorded" | "started"
  psnap,      \* the state the poll read
  pkeep,      \* the jobs the poll keeps
  pnew,       \* the job the poll started (NoJob if none)
  alarm,      \* the alarm is set
  jstate,     \* each job: "none" | "started" | "committed" | "discarded"
  jepoch,     \* each job's compactor epoch
  visible,    \* each job's Workflow instance can be read
  unseen,     \* compactionLeaseMs has passed since a read saw it running
  outputs,    \* "none" | "young" | "old" | "deleted": the job's uploaded output
  applied,    \* the job's commit changed the manifest
  outcome,    \* what the job reported: "none" | "committed" | "discarded"
  retried,    \* the job's commit step ran a second time
  lease,      \* jobs holding a budget lease (capacity 1)
  evictions,
  cpc,        \* the collection: "idle" | "gc" | "write" | "set" ("set": noStateLock only)
  csnap,      \* the running jobs the collection read (D96's cap)
  wsnap,      \* the state its updateExisting read (noStateLock)
  dpc,        \* dropSst: "idle" | "checked" | "done"
  readsRun,   \* each job reads the sorted run holding the SST dropSst drops
  sstDropped, \* the drop is committed
  revived,    \* a job's output brought the dropped SST's rows back
  recorded    \* ghost: the running jobs the poll and fence-ins last wrote

vars == <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew, alarm,
          jstate, jepoch, visible, unseen, outputs, applied, outcome, retried, lease, evictions,
          cpc, csnap, wsnap, dpc, readsRun, sstDropped, revived, recorded>>
jobVars == <<jstate, jepoch, visible, unseen, outputs, applied, outcome, retried>>
\* The collection's, dropSst's, and the ghost's variables.
newVars == <<cpc, csnap, wsnap, dpc, readsRun, sstDropped, revived, recorded>>

NoState == [has |-> FALSE, epoch |-> 0, running |-> {}, fenced |-> FALSE]

Init ==
  /\ mEpoch = 0 /\ l0 = 0 /\ flushes = 0
  /\ cell = NoState
  /\ startLock = "free"
  /\ wpc = [w \in Wakes |-> "idle"] /\ wepoch = [w \in Wakes |-> 0]
  /\ ppc = "idle" /\ psnap = NoState /\ pkeep = {} /\ pnew = NoJob
  /\ alarm = FALSE
  /\ jstate = [j \in Jobs |-> "none"] /\ jepoch = [j \in Jobs |-> 0]
  /\ visible = [j \in Jobs |-> FALSE] /\ unseen = [j \in Jobs |-> FALSE]
  /\ outputs = [j \in Jobs |-> "none"] /\ applied = [j \in Jobs |-> FALSE]
  /\ outcome = [j \in Jobs |-> "none"] /\ retried = [j \in Jobs |-> FALSE]
  /\ lease = {} /\ evictions = 0
  /\ cpc = "idle" /\ csnap = {} /\ wsnap = NoState
  /\ dpc = "idle" /\ readsRun = [j \in Jobs |-> FALSE] /\ sstDropped = FALSE /\ revived = FALSE
  /\ recorded = {}

Locked == Broken \notin {"twoFenceIns", "beforeD78"}
StaleWrite == Broken = "beforeD78"
FencedTerminal == Broken = "beforeD78"

(* ----- The writer ----- *)
Flush ==
  /\ flushes < MaxFlushes /\ l0 < MaxL0
  /\ l0' = l0 + 1 /\ flushes' = flushes + 1
  /\ UNCHANGED <<mEpoch, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew, alarm, jobVars,
                 lease, evictions>>
  /\ UNCHANGED newVars

(* ----- ensureState from a wake (also requestCollection, dropSst) ----- *)
WakeBegin(w) ==
  /\ wpc[w] = "idle" /\ l0 >= MinSources
  /\ ~Locked \/ startLock = "free"
  /\ startLock' = IF Locked THEN w ELSE startLock
  /\ wpc' = [wpc EXCEPT ![w] = "read"]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, wepoch, ppc, psnap, pkeep, pnew, alarm, jobVars,
                 lease, evictions>>
  /\ UNCHANGED newVars

\* The cell read: a live state ends the start (and polls now); none, or a fenced one, fences in.
WakeRead(w) ==
  /\ wpc[w] = "read"
  /\ IF cell.has /\ (~cell.fenced \/ FencedTerminal)
       THEN /\ wpc' = [wpc EXCEPT ![w] = "idle"]
            /\ startLock' = IF Locked THEN "free" ELSE startLock
            /\ alarm' = (alarm \/ ~cell.fenced)
       ELSE /\ wpc' = [wpc EXCEPT ![w] = "fence"]
            /\ UNCHANGED <<startLock, alarm>>
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, wepoch, ppc, psnap, pkeep, pnew, jobVars, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* ManifestStore.update(fenceCompactor): the R2 commit.
WakeFence(w) ==
  /\ wpc[w] = "fence" /\ mEpoch < MaxEpoch
  /\ mEpoch' = mEpoch + 1 /\ wepoch' = [wepoch EXCEPT ![w] = mEpoch + 1]
  /\ wpc' = [wpc EXCEPT ![w] = "write"]
  /\ UNCHANGED <<l0, flushes, cell, startLock, ppc, psnap, pkeep, pnew, alarm, jobVars, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* updateState: the new epoch, no running jobs; the old epoch's leases are released.
WakeWrite(w) ==
  /\ wpc[w] = "write"
  /\ cell' = [has |-> TRUE, epoch |-> wepoch[w], running |-> {}, fenced |-> FALSE]
  /\ lease' = lease \ cell.running
  /\ wpc' = [wpc EXCEPT ![w] = "idle"]
  /\ startLock' = IF Locked THEN "free" ELSE startLock
  /\ alarm' = TRUE
  /\ UNCHANGED <<mEpoch, l0, flushes, wepoch, ppc, psnap, pkeep, pnew, jobVars, evictions>>
  /\ recorded' = {}
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, readsRun, sstDropped, revived>>

(* ----- The alarm and its poll (under planLock: one poll at a time) ----- *)
\* runAlarm: a fenced state fences in again first (D78), as a start does, under startLock.
AlarmFire ==
  /\ alarm /\ ppc = "idle" /\ cell.has
  /\ dpc # "checked" \/ Broken = "noPlanLock"
  /\ alarm' = FALSE
  /\ IF cell.fenced
       THEN IF FencedTerminal \/ startLock # "free" \/ mEpoch = MaxEpoch
              THEN UNCHANGED <<mEpoch, cell, lease, ppc, psnap>>
              ELSE /\ mEpoch' = mEpoch + 1
                   /\ cell' = [has |-> TRUE, epoch |-> mEpoch + 1, running |-> {}, fenced |-> FALSE]
                   /\ lease' = lease \ cell.running
                   /\ ppc' = "loaded"
                   /\ psnap' = [has |-> TRUE, epoch |-> mEpoch + 1, running |-> {}, fenced |-> FALSE]
       ELSE /\ ppc' = "loaded" /\ psnap' = cell
            /\ UNCHANGED <<mEpoch, cell, lease>>
  /\ UNCHANGED <<l0, flushes, startLock, wpc, wepoch, pkeep, pnew, jobVars, evictions>>
  /\ recorded' = IF cell.fenced /\ cell' # cell THEN {} ELSE recorded
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, readsRun, sstDropped, revived>>

\* A running job the poll keeps: readable and still running, or unreadable but not for long. A job
\* recorded and never launched has no instance: finished at once (D103).
Keeps(j) ==
  \/ visible[j] /\ jstate[j] = "started"
  \/ ~visible[j] /\ jstate[j] # "none" /\ (~unseen[j] \/ Broken = "noUnseenBound")

WriteAhead == Broken # "launchBeforeRecord"

\* loadLatest, each job's progress, and the plan with its lease and launch (all awaits; merged).
PollLoad ==
  /\ ppc = "loaded"
  /\ IF mEpoch # psnap.epoch
       THEN \* A newer epoch fences this one's jobs: mark the state fenced (epoch-guarded).
            /\ cell' = IF StaleWrite
                         THEN [psnap EXCEPT !.fenced = TRUE, !.running = {}]
                         ELSE IF cell.epoch = psnap.epoch
                                THEN [cell EXCEPT !.fenced = TRUE, !.running = {}]
                                ELSE cell
            /\ lease' = lease \ psnap.running
            /\ ppc' = "idle"
            /\ recorded' = IF cell' # cell THEN {} ELSE recorded
            /\ UNCHANGED <<pkeep, pnew, jobVars, readsRun>>
       ELSE LET keep == {j \in psnap.running : Keeps(j)}
                free == {j \in Jobs : jstate[j] = "none" /\ j \notin cell.running \cup lease}
            IN /\ pkeep' = keep
               \* updateExisting before the launch is epoch-guarded (D116): a fenced state starts nothing.
               /\ LET launch == l0 >= MinSources /\ keep = {} /\ lease \subseteq psnap.running
                                /\ free # {}
                                /\ (~WriteAhead \/ (cell.epoch = psnap.epoch /\ ~cell.fenced))
                  IN IF launch
                    THEN LET j == CHOOSE x \in free : TRUE IN
                         /\ pnew' = j
                         /\ lease' = (lease \ (psnap.running \ keep)) \cup {j}
                         /\ \E r \in IF sstDropped \/ "drop" \notin Ops THEN {FALSE} ELSE BOOLEAN :
                              readsRun' = [readsRun EXCEPT ![j] = r]
                         /\ IF WriteAhead
                              THEN /\ cell' = [cell EXCEPT !.running = @ \cup {j}]
                                   /\ recorded' = recorded \cup {j}
                                   /\ ppc' = "recorded"
                                   /\ UNCHANGED jobVars
                              ELSE /\ jstate' = [jstate EXCEPT ![j] = "started"]
                                   /\ jepoch' = [jepoch EXCEPT ![j] = psnap.epoch]
                                   /\ visible' = [visible EXCEPT ![j] = TRUE]
                                   /\ UNCHANGED <<unseen, outputs, applied, outcome, retried>>
                                   /\ ppc' = "started"
                                   /\ UNCHANGED <<cell, recorded>>
                    ELSE /\ pnew' = NoJob
                         /\ lease' = lease \ (psnap.running \ keep)
                         /\ UNCHANGED <<jobVars, readsRun>>
                         /\ ppc' = "started"
                         /\ UNCHANGED <<cell, recorded>>
  /\ UNCHANGED <<mEpoch, l0, flushes, startLock, wpc, wepoch, psnap, alarm, evictions>>
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, sstDropped, revived>>

\* launcher.start, after the job was recorded (D116).
PollLaunch ==
  /\ ppc = "recorded" /\ pnew # NoJob
  /\ jstate' = [jstate EXCEPT ![pnew] = "started"]
  /\ jepoch' = [jepoch EXCEPT ![pnew] = psnap.epoch]
  /\ visible' = [visible EXCEPT ![pnew] = TRUE]
  /\ ppc' = "started"
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, psnap, pkeep, pnew, alarm,
                 unseen, outputs, applied, outcome, retried, lease, evictions, newVars>>

\* updateState: written onto the current state only if the epoch is still the poll's (D78).
PollWrite ==
  /\ ppc = "started"
  /\ LET running == pkeep \cup (IF pnew = NoJob THEN {} ELSE {pnew}) IN
       IF StaleWrite
         THEN /\ cell' = [psnap EXCEPT !.running = running]
              /\ UNCHANGED lease
              /\ alarm' = (alarm \/ running # {})
         ELSE IF cell.epoch = psnap.epoch /\ ~cell.fenced
           THEN /\ cell' = [cell EXCEPT !.running = running]
                /\ UNCHANGED lease
                /\ alarm' = (alarm \/ running # {})
           ELSE /\ lease' = lease \ running
                /\ UNCHANGED <<cell, alarm>>
  /\ ppc' = "idle" /\ pkeep' = {} /\ pnew' = NoJob
  /\ UNCHANGED <<mEpoch, l0, flushes, startLock, wpc, wepoch, psnap, jobVars, evictions>>
  /\ recorded' = IF cell' # cell THEN cell'.running ELSE recorded
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, readsRun, sstDropped, revived>>

(* ----- A Workflow job ----- *)
Upload(j) ==
  /\ jstate[j] = "started" /\ outputs[j] = "none"
  /\ outputs' = [outputs EXCEPT ![j] = "young"]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, visible, unseen, applied, outcome, retried, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* commitCompaction: the epoch, the inputs, and the outputs above GC's boundary.
Commit(j) ==
  /\ jstate[j] = "started" /\ outputs[j] # "none"
  /\ IF jepoch[j] = mEpoch /\ l0 >= MinSources /\ outputs[j] # "deleted"
       THEN /\ l0' = l0 - MinSources
            /\ applied' = [applied EXCEPT ![j] = TRUE]
            /\ jstate' = [jstate EXCEPT ![j] = "committed"]
            /\ outcome' = [outcome EXCEPT ![j] = "committed"]
            /\ revived' = (revived \/ (readsRun[j] /\ sstDropped))
       ELSE /\ jstate' = [jstate EXCEPT ![j] = "discarded"]
            /\ outcome' = [outcome EXCEPT ![j] = "discarded"]
            /\ UNCHANGED <<l0, applied, revived>>
  /\ UNCHANGED <<mEpoch, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew, alarm,
                 jepoch, visible, unseen, outputs, retried, lease, evictions>>
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, readsRun, sstDropped, recorded>>

\* The commit step runs again after its result was lost (Workflows runs steps at least once).
RetryCommit(j) ==
  /\ jstate[j] = "committed" /\ ~retried[j]
  /\ retried' = [retried EXCEPT ![j] = TRUE]
  /\ outcome' = IF Broken = "noJobRecord" THEN [outcome EXCEPT ![j] = "discarded"] ELSE outcome
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, visible, unseen, outputs, applied, lease, evictions>>
  /\ UNCHANGED newVars

\* A finished job's Workflow instance is purged before the compactor read its outcome (a live
\* instance whose reads fail forever is an outage, outside what the compactor can fix).
Vanish(j) ==
  /\ visible[j] /\ jstate[j] \in {"committed", "discarded"}
  /\ visible' = [visible EXCEPT ![j] = FALSE]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, unseen, outputs, applied, outcome, retried, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* compactionLeaseMs passes with no read seeing an unreadable job running.
Unseen(j) ==
  /\ ~visible[j] /\ jstate[j] # "none" /\ ~unseen[j]
  /\ unseen' = [unseen EXCEPT ![j] = TRUE]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, visible, outputs, applied, outcome, retried, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* A job no state names and no lease holds is gone: its slot is free again.
Recycle(j) ==
  /\ jstate[j] \in {"committed", "discarded"} \/ (jstate[j] = "started" /\ ~visible[j] /\ unseen[j])
  /\ j \notin cell.running /\ j \notin lease /\ j \notin psnap.running /\ pnew # j
  /\ j \notin pkeep
  /\ jstate' = [jstate EXCEPT ![j] = "none"] /\ visible' = [visible EXCEPT ![j] = FALSE]
  /\ unseen' = [unseen EXCEPT ![j] = FALSE] /\ outputs' = [outputs EXCEPT ![j] = "none"]
  /\ applied' = [applied EXCEPT ![j] = FALSE] /\ outcome' = [outcome EXCEPT ![j] = "none"]
  /\ retried' = [retried EXCEPT ![j] = FALSE]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jepoch, lease, evictions>>
  /\ readsRun' = [readsRun EXCEPT ![j] = FALSE]
  /\ UNCHANGED <<cpc, csnap, wsnap, dpc, sstDropped, revived, recorded>>

(* ----- GC (D96) and lease expiry ----- *)
\* gcMinAge passes after the output was written. That is far longer than the poll takes from the
\* launch to recording the job, so a job its poll is still recording is never that old.
Age(j) ==
  /\ outputs[j] = "young" /\ pnew # j
  /\ outputs' = [outputs EXCEPT ![j] = "old"]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, visible, unseen, applied, outcome, retried, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* An unreferenced output older than gcMinAge, of a job the compactor does not record as running.
\* Without the collection process (Ops), one step reads the running jobs and deletes.
GcDelete(j) ==
  /\ outputs[j] = "old" /\ jstate[j] = "started"
  /\ IF "collect" \in Ops THEN cpc = "gc" ELSE TRUE
  /\ Broken = "gcIgnoresRunning" \/ j \notin (IF "collect" \in Ops THEN csnap ELSE cell.running)
  /\ outputs' = [outputs EXCEPT ![j] = "deleted"]
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jstate, jepoch, visible, unseen, applied, outcome, retried, lease,
                 evictions>>
  /\ UNCHANGED newVars

\* A lease no running job renews expires (compactionLeaseMs).
LeaseExpire(j) ==
  /\ j \in lease /\ j \notin cell.running /\ pnew # j
  /\ lease' = lease \ {j}
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, evictions>>
  /\ UNCHANGED newVars

\* Eviction: every request and the poll are gone, the locks free; storage and the alarm stay. An
\* alarm handler that did not finish is run again by the runtime (at-least-once alarms).
Evict ==
  /\ evictions < MaxEvictions
  /\ evictions' = evictions + 1
  /\ wpc' = [w \in Wakes |-> "idle"] /\ ppc' = "idle" /\ startLock' = "free"
  /\ pkeep' = {} /\ pnew' = NoJob
  /\ alarm' = (alarm \/ ppc # "idle" \/ cpc # "idle")
  /\ cpc' = "idle" /\ dpc' = IF dpc = "checked" THEN "idle" ELSE dpc
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, wepoch, psnap, jobVars, lease, csnap, wsnap, readsRun,
                 sstDropped, revived, recorded>>

(* ----- A collection (the alarm's, or an operator's collect beside it) ----- *)
\* runCollection, under collectionLock: the running jobs, for D96's cap.
CollStart ==
  /\ "collect" \in Ops /\ cpc = "idle" /\ cell.has
  /\ cpc' = "gc" /\ csnap' = cell.running
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, lease, evictions, wsnap, dpc, readsRun, sstDropped, revived,
                 recorded>>

\* collector.collect returns (its deletes are GcDelete).
CollEnd ==
  /\ cpc = "gc" /\ cpc' = "write"
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, lease, evictions, csnap, wsnap, dpc, readsRun, sstDropped,
                 revived, recorded>>

\* updateExisting(collectionDueAt): under stateLock one step, changing only the due time (not
\* modeled). Without it, cell.get here and cell.set (CollSet) are two awaits.
CollWrite ==
  /\ cpc = "write"
  /\ IF Broken = "noStateLock"
       THEN cpc' = "set" /\ wsnap' = cell
       ELSE cpc' = "idle" /\ UNCHANGED wsnap
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, lease, evictions, csnap, dpc, readsRun, sstDropped, revived,
                 recorded>>

CollSet ==
  /\ cpc = "set"
  /\ cell' = IF wsnap.has THEN wsnap ELSE cell
  /\ cpc' = "idle"
  /\ UNCHANGED <<mEpoch, l0, flushes, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew, alarm,
                 jobVars, lease, evictions, csnap, wsnap, dpc, readsRun, sstDropped, revived,
                 recorded>>

(* ----- dropSst (D62), under planLock ----- *)
\* The state's running jobs: refused while one reads the SST's sorted run.
DropCheck ==
  /\ "drop" \in Ops /\ dpc = "idle" /\ cell.has /\ ~cell.fenced
  /\ ppc = "idle" \/ Broken = "noPlanLock"
  /\ dpc' = IF \E j \in cell.running : readsRun[j] THEN "idle" ELSE "checked"
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, lease, evictions, cpc, csnap, wsnap, readsRun, sstDropped,
                 revived, recorded>>

\* manifests.update(dropSstFromManifest): the run keeps its id, so commitCompaction's
\* exact-inputs check does not notice a job that read the dropped SST.
DropCommit ==
  /\ dpc = "checked"
  /\ sstDropped' = TRUE /\ dpc' = "done"
  /\ UNCHANGED <<mEpoch, l0, flushes, cell, startLock, wpc, wepoch, ppc, psnap, pkeep, pnew,
                 alarm, jobVars, lease, evictions, cpc, csnap, wsnap, readsRun, revived,
                 recorded>>

WakeStep(w) == WakeBegin(w) \/ WakeRead(w) \/ WakeFence(w) \/ WakeWrite(w)
JobStep(j) == Upload(j) \/ Commit(j) \/ RetryCommit(j)

Next ==
  \/ Flush
  \/ \E w \in Wakes : WakeStep(w)
  \/ AlarmFire \/ PollLoad \/ PollLaunch \/ PollWrite
  \/ \E j \in Jobs : JobStep(j) \/ Vanish(j) \/ Unseen(j) \/ Recycle(j) \/ Age(j) \/ GcDelete(j)
                     \/ LeaseExpire(j)
  \/ Evict
  \/ CollStart \/ CollEnd \/ CollWrite \/ CollSet \/ DropCheck \/ DropCommit

\* The runtime fires a set alarm, Workflows runs a job's steps, time passes, leases expire, and the
\* writer's stalled-write wake comes back while L0 can be compacted.
Fairness ==
  /\ \A w \in Wakes : WF_vars(WakeStep(w))
  /\ WF_vars(AlarmFire) /\ WF_vars(PollLoad) /\ WF_vars(PollLaunch) /\ WF_vars(PollWrite)
  /\ \A j \in Jobs : WF_vars(Upload(j)) /\ WF_vars(Commit(j)) /\ WF_vars(Unseen(j))
                     /\ WF_vars(LeaseExpire(j)) /\ WF_vars(Recycle(j))
  /\ WF_vars(CollEnd) /\ WF_vars(CollWrite) /\ WF_vars(CollSet)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ mEpoch \in 0..MaxEpoch /\ l0 \in 0..MaxL0
  /\ cell.epoch \in 0..MaxEpoch /\ cell.running \subseteq Jobs
  /\ lease \subseteq Jobs

\* The budget's capacity: one compaction at a time.
LeaseCapacity == Cardinality(lease) <= 1

\* A commit applies a job's merge once, and a job reports what its commit did (D103).
OutcomeTrue == \A j \in Jobs : applied[j] => outcome[j] = "committed"

\* A dropped SST's rows never come back (D62).
DroppedStaysDropped == ~revived

\* The state names every job the poll recorded and has not since finished: D96's cap and the
\* lease renewal both read it.
RunningStaysRecorded == recorded \subseteq cell.running

\* The stored epoch never moves back (D78: two fence-ins, or a poll's stale write).
EpochNeverRegresses == [][cell'.epoch >= cell.epoch]_cell

\* L0 at its limit is eventually compacted (D78, D94, D96): the compactor keeps working.
L0Drains == (l0 = MaxL0) ~> (l0 < MaxL0)

\* A job recorded as running eventually leaves the record (D94).
RunningJobsLeave == \A j \in Jobs : (j \in cell.running) ~> (j \notin cell.running)
=============================================================================
