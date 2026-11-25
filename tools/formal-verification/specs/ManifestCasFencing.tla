---------------------------- MODULE ManifestCasFencing ----------------------------
(***************************************************************************)
(* Manifest CAS and epoch fencing for one cf-slatedb database.             *)
(*                                                                         *)
(* Code this models (packages/):                                           *)
(*  - lsm-cloudflare/src/manifest-store.ts  ManifestStore.update: load the *)
(*    newest manifest, apply a pure edit, create id + 1 with               *)
(*    putIfAbsent; on conflict reload and reapply.                          *)
(*  - lsm-cloudflare/src/manifest-reader.ts ManifestReader.loadLatest.      *)
(*  - lsm-core/src/manifest.ts fenceWriter, fenceCompactor, commitL0Flush  *)
(*    (writer epoch check, idempotent retry), commitCompaction (compactor  *)
(*    epoch check, inputs-present check).                                  *)
(*                                                                         *)
(* One step of this spec is one awaited effect in that code: an object     *)
(* store read, an SST upload, or one putIfAbsent. A writer flush uploads   *)
(* its SST, loads the head, and creates head + 1; a compaction job reads   *)
(* the head, uploads its merged output, loads the head again, and creates  *)
(* head + 1. SST data is modeled as the set of sequence numbers it holds.  *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "noWriterEpochCheck" commitL0Flush skips the writer epoch check       *)
(*   "noInputsCheck"      commitCompaction skips the inputs-present check  *)
(*   "blindCreate"        create overwrites instead of If-None-Match: *    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Compactors, JobsPerCompactor, MaxId, MaxFlushes, Broken

ASSUME Broken \in {"none", "noWriterEpochCheck", "noInputsCheck", "blindCreate"}

Ids == 1 .. MaxId
Jobs == Compactors \X (1 .. JobsPerCompactor)
NoManifest == [absent |-> TRUE]

\* A manifest: writer epoch, compactor epoch, L0 SSTs, and one sorted run (a set of SSTs).
Empty == [we |-> 0, ce |-> 0, l0 |-> {}, runs |-> {}]

VARIABLES
  store,        \* manifest id -> manifest or NoManifest (the object store's manifest keys)
  firstWrite,   \* manifest id -> the manifest first created there (history, for CreateOnce)
  wpc, wEpoch, wSnap, wSst, flushes,
  cpc, cEpoch, cSnap,
  jpc, jSnap, jInputs, jRuns, jOut,
  sstData,      \* uploaded SST -> its data (set of sequence numbers)
  nextSeq,
  acked,        \* sequence numbers whose flush commit succeeded (acknowledged to the writer)
  commits       \* every successful commit: kind, actor epoch, base manifest epoch

vars == <<store, firstWrite, wpc, wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap,
          jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

Exists(i) == i \in Ids /\ store[i] # NoManifest
Head == CHOOSE i \in Ids : Exists(i) /\ (i = MaxId \/ ~Exists(i + 1))
Data(m) == UNION {sstData[s] : s \in m.l0 \cup m.runs}

Init ==
  /\ store = [i \in Ids |-> IF i = 1 THEN Empty ELSE NoManifest]
  /\ firstWrite = store
  /\ wpc = [w \in Writers |-> "fenceLoad"]
  /\ wEpoch = [w \in Writers |-> 0]
  /\ wSnap = [w \in Writers |-> 1]
  /\ wSst = [w \in Writers |-> <<>>]
  /\ flushes = 0
  /\ cpc = [c \in Compactors |-> "fenceLoad"]
  /\ cEpoch = [c \in Compactors |-> 0]
  /\ cSnap = [c \in Compactors |-> 1]
  /\ jpc = [j \in Jobs |-> "idle"]
  /\ jSnap = [j \in Jobs |-> 1]
  /\ jInputs = [j \in Jobs |-> {}]
  /\ jRuns = [j \in Jobs |-> {}]
  /\ jOut = [j \in Jobs |-> <<>>]
  /\ sstData = [s \in {} |-> {}]
  /\ nextSeq = 1
  /\ acked = {}
  /\ commits = {}

\* ManifestStore.create: putIfAbsent of manifest id i (blindCreate overwrites).
CanCreate(i) == i \in Ids /\ (~Exists(i) \/ Broken = "blindCreate")
Create(i, m) ==
  /\ store' = [store EXCEPT ![i] = m]
  /\ firstWrite' = IF firstWrite[i] = NoManifest THEN [firstWrite EXCEPT ![i] = m] ELSE firstWrite

---------------------------------------------------------------------------
\* Writer: WriterServer init runs ManifestStore.update(fenceWriter).
WriterFenceLoad(w) ==
  /\ wpc[w] = "fenceLoad"
  /\ wSnap' = [wSnap EXCEPT ![w] = Head]
  /\ wpc' = [wpc EXCEPT ![w] = "fenceCreate"]
  /\ UNCHANGED <<store, firstWrite, wEpoch, wSst, flushes, cpc, cEpoch, cSnap, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

WriterFenceCreate(w) ==
  /\ wpc[w] = "fenceCreate"
  /\ LET base == store[wSnap[w]]
         next == [base EXCEPT !.we = base.we + 1]
     IN IF CanCreate(wSnap[w] + 1)
        THEN /\ Create(wSnap[w] + 1, next)
             /\ wEpoch' = [wEpoch EXCEPT ![w] = next.we]
             /\ wpc' = [wpc EXCEPT ![w] = "ready"]
        ELSE /\ wpc' = [wpc EXCEPT ![w] = "fenceLoad"]       \* conflict: reload, reapply
             /\ UNCHANGED <<store, firstWrite, wEpoch>>
  /\ UNCHANGED <<wSnap, wSst, flushes, cpc, cEpoch, cSnap, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

\* flushL0: SstStore writes the L0 SST (a new ULID name, so never a collision).
WriterUpload(w) ==
  /\ wpc[w] = "ready"
  /\ flushes < MaxFlushes
  /\ LET sst == <<"l0", nextSeq>>
     IN /\ sstData' = [s \in DOMAIN sstData \cup {sst} |-> IF s = sst THEN {nextSeq} ELSE sstData[s]]
        /\ wSst' = [wSst EXCEPT ![w] = sst]
  /\ nextSeq' = nextSeq + 1
  /\ flushes' = flushes + 1
  /\ wpc' = [wpc EXCEPT ![w] = "flushLoad"]
  /\ UNCHANGED <<store, firstWrite, wEpoch, wSnap, cpc, cEpoch, cSnap, jpc, jSnap, jInputs, jRuns, jOut, acked, commits>>

WriterFlushLoad(w) ==
  /\ wpc[w] = "flushLoad"
  /\ wSnap' = [wSnap EXCEPT ![w] = Head]
  /\ wpc' = [wpc EXCEPT ![w] = "flushCreate"]
  /\ UNCHANGED <<store, firstWrite, wEpoch, wSst, flushes, cpc, cEpoch, cSnap, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

\* ManifestStore.update(commitL0Flush): epoch check, then putIfAbsent(head + 1).
WriterFlushCreate(w) ==
  /\ wpc[w] = "flushCreate"
  /\ LET base == store[wSnap[w]]
         sst == wSst[w]
     IN IF sst \in base.l0
        THEN /\ wpc' = [wpc EXCEPT ![w] = "ready"]          \* already applied: write nothing
             /\ acked' = acked \cup sstData[sst]
             /\ UNCHANGED <<store, firstWrite, commits>>
        ELSE IF base.we # wEpoch[w] /\ Broken # "noWriterEpochCheck"
        THEN /\ wpc' = [wpc EXCEPT ![w] = "fenced"]         \* WriterFencedError: stop
             /\ UNCHANGED <<store, firstWrite, acked, commits>>
        ELSE IF CanCreate(wSnap[w] + 1)
        THEN /\ Create(wSnap[w] + 1, [base EXCEPT !.l0 = base.l0 \cup {sst}])
             /\ acked' = acked \cup sstData[sst]
             /\ commits' = commits \cup {[kind |-> "flush", epoch |-> wEpoch[w], baseEpoch |-> base.we]}
             /\ wpc' = [wpc EXCEPT ![w] = "ready"]
        ELSE /\ wpc' = [wpc EXCEPT ![w] = "flushLoad"]      \* conflict: reload, reapply
             /\ UNCHANGED <<store, firstWrite, acked, commits>>
  /\ UNCHANGED <<wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq>>

---------------------------------------------------------------------------
\* Compactor: CompactorServer init runs ManifestStore.update(fenceCompactor).
CompactorFenceLoad(c) ==
  /\ cpc[c] = "fenceLoad"
  /\ cSnap' = [cSnap EXCEPT ![c] = Head]
  /\ cpc' = [cpc EXCEPT ![c] = "fenceCreate"]
  /\ UNCHANGED <<store, firstWrite, wpc, wEpoch, wSnap, wSst, flushes, cEpoch, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

CompactorFenceCreate(c) ==
  /\ cpc[c] = "fenceCreate"
  /\ LET base == store[cSnap[c]]
         next == [base EXCEPT !.ce = base.ce + 1]
     IN IF CanCreate(cSnap[c] + 1)
        THEN /\ Create(cSnap[c] + 1, next)
             /\ cEpoch' = [cEpoch EXCEPT ![c] = next.ce]
             /\ cpc' = [cpc EXCEPT ![c] = "ready"]
        ELSE /\ cpc' = [cpc EXCEPT ![c] = "fenceLoad"]
             /\ UNCHANGED <<store, firstWrite, cEpoch>>
  /\ UNCHANGED <<wpc, wEpoch, wSnap, wSst, flushes, cSnap, jpc, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

\* The scheduler plans a job from the head: some L0 SSTs plus the whole sorted run.
JobPlan(j) ==
  /\ jpc[j] = "idle"
  /\ cpc[j[1]] = "ready"
  /\ LET m == store[Head]
     IN \E inputs \in SUBSET m.l0 :
          /\ inputs # {}
          /\ jInputs' = [jInputs EXCEPT ![j] = inputs]
          /\ jRuns' = [jRuns EXCEPT ![j] = m.runs]
  /\ jpc' = [jpc EXCEPT ![j] = "upload"]
  /\ UNCHANGED <<store, firstWrite, wpc, wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap, jSnap, jOut, sstData, nextSeq, acked, commits>>

\* The Workflow streams the merged output SST (a new ULID).
JobUpload(j) ==
  /\ jpc[j] = "upload"
  /\ LET out == <<"run", j, nextSeq>>
         data == UNION {sstData[s] : s \in jInputs[j] \cup jRuns[j]}
     IN /\ sstData' = [s \in DOMAIN sstData \cup {out} |-> IF s = out THEN data ELSE sstData[s]]
        /\ jOut' = [jOut EXCEPT ![j] = out]
  /\ nextSeq' = nextSeq + 1
  /\ jpc' = [jpc EXCEPT ![j] = "commitLoad"]
  /\ UNCHANGED <<store, firstWrite, wpc, wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap, jSnap, jInputs, jRuns, acked, commits>>

JobCommitLoad(j) ==
  /\ jpc[j] = "commitLoad"
  /\ jSnap' = [jSnap EXCEPT ![j] = Head]
  /\ jpc' = [jpc EXCEPT ![j] = "commitCreate"]
  /\ UNCHANGED <<store, firstWrite, wpc, wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked, commits>>

\* ManifestStore.update(commitCompaction): compactor epoch check, inputs-present check,
\* then replace exactly the inputs.
JobCommitCreate(j) ==
  /\ jpc[j] = "commitCreate"
  /\ LET base == store[jSnap[j]]
         c == j[1]
         inputsPresent == jInputs[j] \subseteq base.l0 /\ base.runs = jRuns[j]
     IN IF base.ce # cEpoch[c]
        THEN /\ jpc' = [jpc EXCEPT ![j] = "done"]           \* CompactorFencedError
             /\ UNCHANGED <<store, firstWrite, commits>>
        ELSE IF ~inputsPresent /\ Broken # "noInputsCheck"
        THEN /\ jpc' = [jpc EXCEPT ![j] = "done"]           \* CompactionInputsChangedError
             /\ UNCHANGED <<store, firstWrite, commits>>
        ELSE IF CanCreate(jSnap[j] + 1)
        THEN /\ Create(jSnap[j] + 1, [base EXCEPT !.l0 = base.l0 \ jInputs[j], !.runs = {jOut[j]}])
             /\ commits' = commits \cup {[kind |-> "compaction", epoch |-> cEpoch[c], baseEpoch |-> base.ce]}
             /\ jpc' = [jpc EXCEPT ![j] = "done"]
        ELSE /\ jpc' = [jpc EXCEPT ![j] = "commitLoad"]
             /\ UNCHANGED <<store, firstWrite, commits>>
  /\ UNCHANGED <<wpc, wEpoch, wSnap, wSst, flushes, cpc, cEpoch, cSnap, jSnap, jInputs, jRuns, jOut, sstData, nextSeq, acked>>

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
  \/ Done

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* Properties.

\* A manifest id is created exactly once: what is stored is what was first created.
CreateOnce == \A i \in Ids : store[i] = firstWrite[i]

\* Fencing: every commit was made by the epoch its base manifest names (a stale writer or
\* compactor never commits once a newer epoch has).
NoStaleCommit == \A c \in commits : c.epoch = c.baseEpoch

\* Fencing: an SST reaches the newest manifest only through a commit, so a fenced writer's
\* uploaded SST (an orphan) is never referenced. Writers that were fenced never had a flush
\* acknowledged after their fence.
FencedWriterOrphansUnreferenced ==
  \A w \in Writers : (wpc[w] = "fenced" /\ wSst[w] # <<>>) => wSst[w] \notin store[Head].l0

\* Linearizability of concurrent edits: every acknowledged flush's data is readable from the
\* newest manifest, whatever writer, compactor, and retry interleavings happened.
AckedDataVisible == acked \subseteq Data(store[Head])

\* Compactions never duplicate data into both L0 and the sorted run (inputs are replaced exactly).
NoDoubleReference == \A s \in store[Head].l0 : sstData[s] \cap UNION {sstData[r] : r \in store[Head].runs} = {}

TypeOK ==
  /\ Head \in Ids
  /\ nextSeq \in Nat

=============================================================================
