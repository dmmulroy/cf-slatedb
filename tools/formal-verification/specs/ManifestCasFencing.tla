---------------------------- MODULE ManifestCasFencing ----------------------------
(***************************************************************************)
(* Manifest CAS and epoch fencing for one shaledb database.             *)
(*                                                                         *)
(* Code this models (packages/):                                           *)
(*  - lsm-cloudflare/src/manifest-store.ts  ManifestStore.update: load the *)
(*    newest manifest, apply a pure edit, create id + 1 (putIfAbsent, then *)
(*    re-read GC's manifest boundary); on conflict reload and reapply.     *)
(*    ManifestStore.updateFrom: the same, but the first attempt applies    *)
(*    the edit to the committer's own view (the manifest it last loaded or *)
(*    committed) without loading; only a written commit is trusted from    *)
(*    that view, any other outcome reloads. After a commit the LATEST hint *)
(*    is written in the background: it may land late, out of order, or     *)
(*    never.                                                               *)
(*  - lsm-cloudflare/src/manifest-reader.ts ManifestReader.loadLatest:     *)
(*    read the hint and GC's manifest boundary, probe forward from         *)
(*    max(hint, boundary + 1) to the first missing id; a missing start     *)
(*    falls back to listing (the newest id).                               *)
(*  - lsm-core/src/manifest.ts fenceWriter, fenceCompactor, commitL0Flush  *)
(*    (writer epoch check, idempotent retry), commitCompaction (compactor  *)
(*    epoch check, inputs-present check).                                  *)
(*  - lsm-cloudflare/src/garbage-collector.ts, as far as manifests go:     *)
(*    advance gc/manifest.boundary, then delete manifests at or below it   *)
(*    (never the newest). Only with WithGc; any such manifest may be       *)
(*    deleted, a superset of what GC's age and pin rules allow.            *)
(*                                                                         *)
(* One step of this spec is one awaited effect in that code: an object     *)
(* store read, an SST upload, or one create (putIfAbsent and the boundary  *)
(* re-read: for a new id the boundary is below it whenever it is read; for *)
(* a collected id the boundary already covers it, so reading it at the     *)
(* create or after gives the same answer). loadLatest is one step. A       *)
(* writer flush uploads its SST and creates from its own view (reloading   *)
(* on conflict); a compaction job reads the head, uploads its merged       *)
(* output, loads the head again, and creates head + 1. SST data is the set *)
(* of sequence numbers it holds.                                           *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "noWriterEpochCheck" commitL0Flush skips the writer epoch check       *)
(*   "noInputsCheck"      commitCompaction skips the inputs-present check  *)
(*   "blindCreate"        create overwrites instead of If-None-Match: *    *)
(*   "noBoundaryReread"   create trusts putIfAbsent alone (WithGc)         *)
(*   "hintOnlyProbe"      loadLatest probes from the hint alone (WithGc)   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Compactors, JobsPerCompactor, MaxId, MaxFlushes, WithGc, Broken

ASSUME Broken \in {"none", "noWriterEpochCheck", "noInputsCheck", "blindCreate",
                   "noBoundaryReread", "hintOnlyProbe"}
ASSUME WithGc \in BOOLEAN

Ids == 1 .. MaxId
Jobs == Compactors \X (1 .. JobsPerCompactor)
NoManifest == [absent |-> TRUE]

\* A manifest: writer epoch, compactor epoch, L0 SSTs, and one sorted run (a set of SSTs).
Empty == [we |-> 0, ce |-> 0, l0 |-> {}, runs |-> {}]

VARIABLES
  store,        \* manifest id -> manifest or NoManifest (the object store's manifest keys)
  overwritten,  \* some create replaced an existing manifest (CreateOnce)
  reported,     \* ids whose create was reported as a commit
  recommitted,  \* some commit was reported at an id already reported once (CommitOnce)
  boundary,     \* gc/manifest.boundary: manifests at or below it may be deleted
  hint,         \* manifest/LATEST
  hintWrites,   \* background hint writes not yet landed: committed ids
  wpc, wEpoch, wSnap, wBase, wOpt, wSst, flushes,
  cpc, cEpoch, cSnap, cBase,
  jpc, jSnap, jBase, jInputs, jRuns, jOut,
  sstData,      \* uploaded SST -> its data (set of sequence numbers)
  nextSeq,
  acked,        \* sequence numbers whose flush commit succeeded (acknowledged to the writer)
  commits       \* every successful commit: kind, actor epoch, base manifest epoch

vars == <<store, overwritten, reported, recommitted, boundary, hint, hintWrites,
          wpc, wEpoch, wSnap, wBase, wOpt, wSst, flushes, cpc, cEpoch, cSnap, cBase,
          jpc, jSnap, jBase, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>
writerVars == <<wpc, wEpoch, wSnap, wBase, wOpt, wSst, flushes>>
compactorVars == <<cpc, cEpoch, cSnap, cBase>>
jobVars == <<jpc, jSnap, jBase, jInputs, jRuns, jOut>>
storeVars == <<store, overwritten, reported, recommitted, hintWrites>>

Exists(i) == i \in Ids /\ store[i] # NoManifest
\* The newest manifest: the largest id that exists.
Head == CHOOSE i \in Ids : Exists(i) /\ \A k \in Ids : Exists(k) => k <= i
Data(m) == UNION {sstData[s] : s \in m.l0 \cup m.runs}
Max(a, b) == IF a >= b THEN a ELSE b

\* ManifestReader.loadLatest: probe forward from the start to the first missing id; a start that
\* is missing (the hint named a collected manifest) falls back to listing, which finds the newest.
Start == IF Broken = "hintOnlyProbe" THEN hint ELSE Max(hint, boundary + 1)
ProbeFrom(s) == CHOOSE i \in s .. MaxId :
                  /\ \A k \in s .. i : Exists(k)
                  /\ (i = MaxId \/ ~Exists(i + 1))
LoadLatest == IF Exists(Start) THEN ProbeFrom(Start) ELSE Head

Init ==
  /\ store = [i \in Ids |-> IF i = 1 THEN Empty ELSE NoManifest]
  /\ overwritten = FALSE
  /\ reported = {1}
  /\ recommitted = FALSE
  /\ boundary = 0
  /\ hint = 1
  /\ hintWrites = {}
  /\ wpc = [w \in Writers |-> "fenceLoad"]
  /\ wEpoch = [w \in Writers |-> 0]
  /\ wSnap = [w \in Writers |-> 1]
  /\ wBase = [w \in Writers |-> Empty]
  /\ wOpt = [w \in Writers |-> FALSE]
  /\ wSst = [w \in Writers |-> <<>>]
  /\ flushes = 0
  /\ cpc = [c \in Compactors |-> "fenceLoad"]
  /\ cEpoch = [c \in Compactors |-> 0]
  /\ cSnap = [c \in Compactors |-> 1]
  /\ cBase = [c \in Compactors |-> Empty]
  /\ jpc = [j \in Jobs |-> "idle"]
  /\ jSnap = [j \in Jobs |-> 1]
  /\ jBase = [j \in Jobs |-> Empty]
  /\ jInputs = [j \in Jobs |-> {}]
  /\ jRuns = [j \in Jobs |-> {}]
  /\ jOut = [j \in Jobs |-> <<>>]
  /\ sstData = [s \in {} |-> {}]
  /\ nextSeq = 1
  /\ acked = {}
  /\ commits = {}

\* ManifestStore.create of manifest id i: "Exists" when putIfAbsent finds it (blindCreate writes
\* anyway); otherwise the manifest is written, and the boundary re-read makes it "Collected" (a
\* conflict: the id was used before and GC deleted it) or "Created" (the commit).
CreateOutcome(i) ==
  IF Exists(i) /\ Broken # "blindCreate" THEN "Exists"
  ELSE IF i <= boundary /\ Broken # "noBoundaryReread" THEN "Collected"
  ELSE "Created"

\* The create writes manifest m at id i (both "Collected" and "Created" write it).
Write(i, m) ==
  /\ store' = [store EXCEPT ![i] = m]
  /\ overwritten' = (overwritten \/ Exists(i))

\* A reported commit at id i, and its background LATEST hint write.
Report(i) ==
  /\ reported' = reported \cup {i}
  /\ recommitted' = (recommitted \/ i \in reported)
  /\ hintWrites' = IF WithGc THEN hintWrites \cup {i} ELSE hintWrites

\* One create attempt: a commit writes `m` at i and runs `onCommit`; a conflict runs `onConflict`.
Attempt(i, m, onCommit, onConflict) ==
  CASE CreateOutcome(i) = "Created"   -> Write(i, m) /\ Report(i) /\ onCommit
    [] CreateOutcome(i) = "Collected" -> Write(i, m) /\ UNCHANGED <<reported, recommitted, hintWrites>> /\ onConflict
    [] OTHER                          -> UNCHANGED storeVars /\ onConflict

---------------------------------------------------------------------------
\* Writer: WriterServer init runs ManifestStore.update(fenceWriter).
WriterFenceLoad(w) ==
  /\ wpc[w] = "fenceLoad"
  /\ wSnap' = [wSnap EXCEPT ![w] = LoadLatest]
  /\ wBase' = [wBase EXCEPT ![w] = store[LoadLatest]]
  /\ wpc' = [wpc EXCEPT ![w] = "fenceCreate"]
  /\ UNCHANGED <<wEpoch, wOpt, wSst, flushes>> /\ UNCHANGED storeVars
  /\ UNCHANGED <<boundary, hint>> /\ UNCHANGED compactorVars /\ UNCHANGED jobVars
  /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

WriterFenceCreate(w) ==
  /\ wpc[w] = "fenceCreate"
  /\ LET i == wSnap[w] + 1
         next == [wBase[w] EXCEPT !.we = @ + 1]
     IN /\ i \in Ids
        /\ Attempt(i, next,
             /\ wEpoch' = [wEpoch EXCEPT ![w] = next.we]
             /\ wSnap' = [wSnap EXCEPT ![w] = i]
             /\ wBase' = [wBase EXCEPT ![w] = next]
             /\ wpc' = [wpc EXCEPT ![w] = "ready"],
             /\ wpc' = [wpc EXCEPT ![w] = "fenceLoad"]     \* conflict: reload, reapply
             /\ UNCHANGED <<wEpoch, wSnap, wBase>>)
  /\ UNCHANGED <<wOpt, wSst, flushes>> /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED compactorVars /\ UNCHANGED jobVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* flushL0: SstStore writes the L0 SST (a new ULID name, so never a collision). The commit then
\* starts from the writer's own view (updateFrom), without loading.
WriterUpload(w) ==
  /\ wpc[w] = "ready"
  /\ flushes < MaxFlushes
  /\ LET sst == <<"l0", nextSeq>>
     IN /\ sstData' = [s \in DOMAIN sstData \cup {sst} |-> IF s = sst THEN {nextSeq} ELSE sstData[s]]
        /\ wSst' = [wSst EXCEPT ![w] = sst]
  /\ nextSeq' = nextSeq + 1
  /\ flushes' = flushes + 1
  /\ wOpt' = [wOpt EXCEPT ![w] = TRUE]
  /\ wpc' = [wpc EXCEPT ![w] = "flushCreate"]
  /\ UNCHANGED <<wEpoch, wSnap, wBase>> /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED compactorVars /\ UNCHANGED jobVars /\ UNCHANGED <<acked, commits>>

WriterFlushLoad(w) ==
  /\ wpc[w] = "flushLoad"
  /\ wSnap' = [wSnap EXCEPT ![w] = LoadLatest]
  /\ wBase' = [wBase EXCEPT ![w] = store[LoadLatest]]
  /\ wOpt' = [wOpt EXCEPT ![w] = FALSE]
  /\ wpc' = [wpc EXCEPT ![w] = "flushCreate"]
  /\ UNCHANGED <<wEpoch, wSst, flushes>> /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED compactorVars /\ UNCHANGED jobVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* commitL0Flush on the view: already applied and fenced write nothing, and are trusted only from
\* a freshly loaded view; from the writer's own view they reload. Otherwise create view + 1.
WriterFlushCreate(w) ==
  /\ wpc[w] = "flushCreate"
  /\ LET base == wBase[w]
         sst == wSst[w]
         i == wSnap[w] + 1
         next == [base EXCEPT !.l0 = base.l0 \cup {sst}]
         applied == sst \in base.l0
         fenced == base.we # wEpoch[w] /\ Broken # "noWriterEpochCheck"
     IN IF (applied \/ fenced) /\ wOpt[w]
        THEN /\ wpc' = [wpc EXCEPT ![w] = "flushLoad"]
             /\ UNCHANGED storeVars /\ UNCHANGED <<wSnap, wBase, acked, commits>>
        ELSE IF applied
        THEN /\ wpc' = [wpc EXCEPT ![w] = "ready"]          \* already applied: write nothing
             /\ acked' = acked \cup sstData[sst]
             /\ UNCHANGED storeVars /\ UNCHANGED <<wSnap, wBase, commits>>
        ELSE IF fenced
        THEN /\ wpc' = [wpc EXCEPT ![w] = "fenced"]         \* WriterFencedError: stop
             /\ UNCHANGED storeVars /\ UNCHANGED <<wSnap, wBase, acked, commits>>
        ELSE /\ i \in Ids
             /\ Attempt(i, next,
                  /\ acked' = acked \cup sstData[sst]
                  /\ commits' = commits \cup {[kind |-> "flush", epoch |-> wEpoch[w], baseEpoch |-> base.we]}
                  /\ wSnap' = [wSnap EXCEPT ![w] = i]
                  /\ wBase' = [wBase EXCEPT ![w] = next]
                  /\ wpc' = [wpc EXCEPT ![w] = "ready"],
                  /\ wpc' = [wpc EXCEPT ![w] = "flushLoad"]  \* conflict: reload, reapply
                  /\ UNCHANGED <<wSnap, wBase, acked, commits>>)
  /\ UNCHANGED <<wEpoch, wOpt, wSst, flushes>> /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED compactorVars /\ UNCHANGED jobVars /\ UNCHANGED <<sstData, nextSeq>>

---------------------------------------------------------------------------
\* Compactor: CompactorServer init runs ManifestStore.update(fenceCompactor).
CompactorFenceLoad(c) ==
  /\ cpc[c] = "fenceLoad"
  /\ cSnap' = [cSnap EXCEPT ![c] = LoadLatest]
  /\ cBase' = [cBase EXCEPT ![c] = store[LoadLatest]]
  /\ cpc' = [cpc EXCEPT ![c] = "fenceCreate"]
  /\ UNCHANGED cEpoch /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED jobVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

CompactorFenceCreate(c) ==
  /\ cpc[c] = "fenceCreate"
  /\ LET i == cSnap[c] + 1
         next == [cBase[c] EXCEPT !.ce = @ + 1]
     IN /\ i \in Ids
        /\ Attempt(i, next,
             /\ cEpoch' = [cEpoch EXCEPT ![c] = next.ce]
             /\ cpc' = [cpc EXCEPT ![c] = "ready"],
             /\ cpc' = [cpc EXCEPT ![c] = "fenceLoad"]
             /\ UNCHANGED cEpoch)
  /\ UNCHANGED <<cSnap, cBase>> /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED jobVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* The scheduler plans a job from the newest manifest it loads: some L0 SSTs plus the whole run.
JobPlan(j) ==
  /\ jpc[j] = "idle"
  /\ cpc[j[1]] = "ready"
  /\ LET m == store[LoadLatest]
     IN \E inputs \in SUBSET m.l0 :
          /\ inputs # {}
          /\ jInputs' = [jInputs EXCEPT ![j] = inputs]
          /\ jRuns' = [jRuns EXCEPT ![j] = m.runs]
  /\ jpc' = [jpc EXCEPT ![j] = "upload"]
  /\ UNCHANGED <<jSnap, jBase, jOut>> /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* The Workflow streams the merged output SST (a new ULID).
JobUpload(j) ==
  /\ jpc[j] = "upload"
  /\ LET out == <<"run", j, nextSeq>>
         data == UNION {sstData[s] : s \in jInputs[j] \cup jRuns[j]}
     IN /\ sstData' = [s \in DOMAIN sstData \cup {out} |-> IF s = out THEN data ELSE sstData[s]]
        /\ jOut' = [jOut EXCEPT ![j] = out]
  /\ nextSeq' = nextSeq + 1
  /\ jpc' = [jpc EXCEPT ![j] = "commitLoad"]
  /\ UNCHANGED <<jSnap, jBase, jInputs, jRuns>> /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED <<acked, commits>>

JobCommitLoad(j) ==
  /\ jpc[j] = "commitLoad"
  /\ jSnap' = [jSnap EXCEPT ![j] = LoadLatest]
  /\ jBase' = [jBase EXCEPT ![j] = store[LoadLatest]]
  /\ jpc' = [jpc EXCEPT ![j] = "commitCreate"]
  /\ UNCHANGED <<jInputs, jRuns, jOut>> /\ UNCHANGED storeVars /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* ManifestStore.update(commitCompaction): compactor epoch check, inputs-present check,
\* then replace exactly the inputs.
JobCommitCreate(j) ==
  /\ jpc[j] = "commitCreate"
  /\ LET base == jBase[j]
         c == j[1]
         i == jSnap[j] + 1
         inputsPresent == jInputs[j] \subseteq base.l0 /\ base.runs = jRuns[j]
         next == [base EXCEPT !.l0 = base.l0 \ jInputs[j], !.runs = {jOut[j]}]
     IN IF base.ce # cEpoch[c]
        THEN /\ jpc' = [jpc EXCEPT ![j] = "done"]           \* CompactorFencedError
             /\ UNCHANGED storeVars /\ UNCHANGED commits
        ELSE IF ~inputsPresent /\ Broken # "noInputsCheck"
        THEN /\ jpc' = [jpc EXCEPT ![j] = "done"]           \* CompactionInputsChangedError
             /\ UNCHANGED storeVars /\ UNCHANGED commits
        ELSE /\ i \in Ids
             /\ Attempt(i, next,
                  /\ commits' = commits \cup {[kind |-> "compaction", epoch |-> cEpoch[c], baseEpoch |-> base.ce]}
                  /\ jpc' = [jpc EXCEPT ![j] = "done"],
                  /\ jpc' = [jpc EXCEPT ![j] = "commitLoad"]
                  /\ UNCHANGED commits)
  /\ UNCHANGED <<jSnap, jBase, jInputs, jRuns, jOut>> /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED <<sstData, nextSeq, acked>>

---------------------------------------------------------------------------
\* GC (WithGc only): advance the manifest boundary below the newest manifest, then delete any
\* manifest at or below it.
GcAdvanceBoundary ==
  /\ WithGc
  /\ \E b \in (boundary + 1) .. (Head - 1) : boundary' = b
  /\ UNCHANGED storeVars /\ UNCHANGED hint
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED jobVars
  /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

GcDeleteManifest ==
  /\ WithGc
  /\ \E i \in 1 .. boundary :
       /\ Exists(i)
       /\ store' = [store EXCEPT ![i] = NoManifest]
  /\ UNCHANGED <<overwritten, reported, recommitted, hintWrites>> /\ UNCHANGED <<boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED jobVars
  /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* A background LATEST hint write lands, possibly after a newer one (the hint may move back), or
\* is lost (the runtime ended first).
HintLands ==
  /\ \E i \in hintWrites :
       /\ hint' = i
       /\ hintWrites' = hintWrites \ {i}
  /\ UNCHANGED <<store, overwritten, reported, recommitted, boundary>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED jobVars
  /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

HintLost ==
  /\ \E i \in hintWrites : hintWrites' = hintWrites \ {i}
  /\ UNCHANGED <<store, overwritten, reported, recommitted, boundary, hint>>
  /\ UNCHANGED writerVars /\ UNCHANGED compactorVars /\ UNCHANGED jobVars
  /\ UNCHANGED <<sstData, nextSeq, acked, commits>>

\* Every actor is finished or the id space is used up.
Done ==
  /\ \A w \in Writers : wpc[w] \in {"ready", "fenced"}
  /\ \A j \in Jobs : jpc[j] \in {"idle", "done"}
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Writers : WriterFenceLoad(w) \/ WriterFenceCreate(w) \/ WriterUpload(w)
                         \/ WriterFlushLoad(w) \/ WriterFlushCreate(w)
  \/ \E c \in Compactors : CompactorFenceLoad(c) \/ CompactorFenceCreate(c)
  \/ \E j \in Jobs : JobPlan(j) \/ JobUpload(j) \/ JobCommitLoad(j) \/ JobCommitCreate(j)
  \/ GcAdvanceBoundary \/ GcDeleteManifest \/ HintLands \/ HintLost
  \/ Done

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* Properties.

\* Nothing ever overwrites an existing manifest.
CreateOnce == ~overwritten

\* No commit is ever reported at an id that was committed before: a create over an id GC collected
\* is a conflict, never a second commit there.
CommitOnce == ~recommitted

\* Fencing: every commit was made by the epoch its base manifest names (a stale writer or
\* compactor never commits once a newer epoch has).
NoStaleCommit == \A c \in commits : c.epoch = c.baseEpoch

\* Fencing: an SST reaches the newest manifest only through a commit, so a fenced writer's
\* uploaded SST (an orphan) is never referenced.
FencedWriterOrphansUnreferenced ==
  \A w \in Writers : (wpc[w] = "fenced" /\ wSst[w] # <<>>) => wSst[w] \notin store[Head].l0

\* Linearizability of concurrent edits: every acknowledged flush's data is readable from the
\* newest manifest, whatever writer, compactor, GC, hint, and retry interleavings happened.
AckedDataVisible == acked \subseteq Data(store[Head])

\* loadLatest finds the newest manifest, however far the hint lags or which manifests GC deleted.
LoadFindsHead == LoadLatest = Head

\* Compactions never duplicate data into both L0 and the sorted run (inputs are replaced exactly).
NoDoubleReference == \A s \in store[Head].l0 : sstData[s] \cap UNION {sstData[r] : r \in store[Head].runs} = {}

TypeOK ==
  /\ Head \in Ids
  /\ boundary < Head
  /\ nextSeq \in Nat

=============================================================================
