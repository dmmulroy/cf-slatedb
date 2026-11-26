------------------------------- MODULE GcSafety -------------------------------
(***************************************************************************)
(* GC safety for one shaledb database: garbage collection never deletes *)
(* an SST that a live manifest, a checkpoint (including a fork's hold on   *)
(* its source), or a reader still within gcMinAge references, and never    *)
(* deletes an SST that is uploaded but not yet committed while its commit  *)
(* can still succeed.                                                      *)
(*                                                                         *)
(* Code this models:                                                       *)
(*  - lsm-core manifest.ts: commitL0Flush / commitCompaction reject a new  *)
(*    SST created before gcSstBoundary (SstBelowGcBoundaryError);          *)
(*    advanceGcSstBoundary only moves forward; checkpoints pin manifests.  *)
(*  - lsm-core garbage-collection-plan.ts: hasSstCollectionCandidates,     *)
(*    planManifestCollection (age = when the next manifest was written),   *)
(*    planSstCollection (unreferenced by retained manifests, created       *)
(*    before the boundary).                                                *)
(*  - lsm-cloudflare garbage-collector.ts collect: list SSTs; pre-check;   *)
(*    CAS-advance the boundary to now - gcMinAge if there are candidates;  *)
(*    list manifests; delete SSTs created before min(boundary, cutoff)     *)
(*    that no retained manifest references; delete old manifests.          *)
(*  - lsm-core dropSstFromManifest (D62): an operator's repair removes one *)
(*    SST from the newest manifest and adds none (DropSst).                *)
(*  - Writers and compactors commit by manifest CAS (ManifestStore.update),*)
(*    which this model makes atomic (ManifestCasFencing checks CAS).       *)
(*                                                                         *)
(* A checkpoint is a pin (Pin/Unpin). With WithFork, one fork of this     *)
(* database is modeled too, abstractly (D77): its hold is a checkpoint on  *)
(* this database (`hold`), its first manifest reads the held manifest's    *)
(* SSTs, its compaction rewrites them (ForkRewrite), and its own GC        *)
(* releases the hold (ForkRelease) only once that first manifest was       *)
(* superseded at least gcMinAge ago, i.e. once no manifest it keeps and a  *)
(* reader may hold names this database's SSTs (releasableExternalRefs over *)
(* planManifestCollection's retained set). An SST's creation time is its   *)
(* ULID time.                                                              *)
(* One GC runs at a time; its six steps interleave with everything else.   *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"             the code as written                                *)
(*   "noBoundaryCheck"  commits accept SSTs created before the boundary    *)
(*   "ownUploadAge"     a manifest's age is its own write time             *)
(*   "ignorePins"       GC deletes checkpointed manifests and their SSTs   *)
(*   "noMinBoundary"    SSTs are deleted below the cutoff, not             *)
(*                      min(boundary, cutoff) (expected redundant)         *)
(*   "dropDeletesObject" an SST drop also deletes the SST's object at once *)
(*                      instead of leaving it to GC                        *)
(*   "earlyForkRelease" the fork releases its hold as soon as its newest   *)
(*                      manifest no longer reads this database (before D77)*)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Ssts, MaxTime, MinAge, MaxManifests, Broken, WithFork

ASSUME Broken \in {"none", "noBoundaryCheck", "ownUploadAge", "ignorePins", "noMinBoundary",
                   "dropDeletesObject", "earlyForkRelease"}
ASSUME WithFork \in BOOLEAN
ASSUME MinAge >= 1

NotMinted == MaxTime + 1

VARIABLES
  now,        \* the clock
  created,    \* each SST's ULID time, NotMinted before it is written
  objects,    \* SST objects in the bucket
  pending,    \* SSTs uploaded by a flush or compaction and not yet committed or discarded
  chain,      \* every manifest ever committed, by id: [ssts, boundary, pins, hold, written]
  stored,     \* manifest ids whose objects still exist
  gpc, gNow, gSsts, gAdvance, gCommitted, gListed,  \* the GC run in progress
  fState,       \* the fork: "none", "reading" (its manifests read the held manifest's SSTs),
                \* "rewritten" (its newest manifest no longer does), "released" (hold removed)
  fHeld,        \* the manifest id the fork's hold pins (its first manifest reads its SSTs)
  fRewrittenAt  \* when the fork's rewrite committed (its first manifest's successor was written)

vars == <<now, created, objects, pending, chain, stored, gpc, gNow, gSsts, gAdvance, gCommitted, gListed,
          fState, fHeld, fRewrittenAt>>
gcVars == <<gpc, gNow, gSsts, gAdvance, gCommitted, gListed>>
forkVars == <<fState, fHeld, fRewrittenAt>>

LatestId == Len(chain)
Latest == chain[LatestId]
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b

Init ==
  /\ now = 0
  /\ created = [s \in Ssts |-> NotMinted]
  /\ objects = {}
  /\ pending = {}
  /\ chain = << [ssts |-> {}, boundary |-> 0, pins |-> {}, hold |-> 0, written |-> 0] >>
  /\ stored = {1}
  /\ gpc = "idle" /\ gNow = 0 /\ gSsts = {} /\ gAdvance = FALSE /\ gCommitted = 1 /\ gListed = {}
  /\ fState = "none" /\ fHeld = 1 /\ fRewrittenAt = 0

\* Commits a manifest (create-if-absent of id LatestId + 1, atomic here).
Commit(m) ==
  /\ chain' = Append(chain, [m EXCEPT !.written = now])
  /\ stored' = stored \cup {LatestId + 1}

CanAppend == Len(chain) < MaxManifests

Tick == /\ now < MaxTime /\ now' = now + 1
        /\ UNCHANGED <<created, objects, pending, chain, stored>> /\ UNCHANGED gcVars

\* A flush or compaction mints an SST (ULID time = now) and uploads it; it may commit much later.
Upload(s) ==
  /\ created[s] = NotMinted
  /\ created' = [created EXCEPT ![s] = now]
  /\ objects' = objects \cup {s}
  /\ pending' = pending \cup {s}
  /\ UNCHANGED <<now, chain, stored>> /\ UNCHANGED gcVars

\* commitL0Flush / commitCompaction: the edit applies to the newest manifest, or fails.
BoundaryAllows(s) == Broken = "noBoundaryCheck" \/ created[s] >= Latest.boundary

CommitFlush(s) ==
  /\ s \in pending /\ CanAppend /\ BoundaryAllows(s)
  /\ pending' = pending \ {s}
  /\ Commit([Latest EXCEPT !.ssts = @ \cup {s}])
  /\ UNCHANGED <<now, created, objects>> /\ UNCHANGED gcVars

\* A compaction replaces exactly its inputs (still in the newest manifest) with its output.
CommitCompaction(s, inputs) ==
  /\ s \in pending /\ CanAppend /\ BoundaryAllows(s)
  /\ inputs # {} /\ inputs \subseteq Latest.ssts
  /\ pending' = pending \ {s}
  /\ Commit([Latest EXCEPT !.ssts = (@ \ inputs) \cup {s}])
  /\ UNCHANGED <<now, created, objects>> /\ UNCHANGED gcVars

\* An operator drops one SST from the newest manifest (D62): it loses its last reference here, and
\* GC may delete it only once no manifest a reader may hold or a checkpoint pins references it.
DropSst(s) ==
  /\ CanAppend /\ s \in Latest.ssts
  /\ Commit([Latest EXCEPT !.ssts = @ \ {s}])
  /\ objects' = IF Broken = "dropDeletesObject" THEN objects \ {s} ELSE objects
  /\ UNCHANGED <<now, created, pending>> /\ UNCHANGED gcVars

\* A refused commit (fenced, inputs changed, or below the boundary): the SST is an orphan.
Discard(s) ==
  /\ s \in pending
  /\ pending' = pending \ {s}
  /\ UNCHANGED <<now, created, objects, chain, stored>> /\ UNCHANGED gcVars

\* A checkpoint (or a fork's hold on this database) pins the newest manifest; unpinning releases it.
Pin ==
  /\ CanAppend /\ LatestId \notin Latest.pins
  /\ Commit([Latest EXCEPT !.pins = @ \cup {LatestId}])
  /\ UNCHANGED <<now, created, objects, pending>> /\ UNCHANGED gcVars

Unpin(p) ==
  /\ CanAppend /\ p \in Latest.pins
  /\ Commit([Latest EXCEPT !.pins = @ \ {p}])
  /\ UNCHANGED <<now, created, objects, pending>> /\ UNCHANGED gcVars

(***************************************************************************)
(* The fork (WithFork), abstractly: its own manifests, compaction, and GC  *)
(* are another database's; only what this database sees is modeled.       *)
(***************************************************************************)
\* forkRange / holdForSuccessors: a checkpoint held by the fork pins the newest manifest (a fork
\* from an older checkpoint pins that checkpoint's manifest, which a pin already keeps). The fork's
\* first manifest reads the held manifest's SSTs.
ForkStart ==
  /\ WithFork /\ fState = "none" /\ CanAppend
  /\ Commit([Latest EXCEPT !.hold = LatestId])
  /\ fState' = "reading" /\ fHeld' = LatestId
  /\ UNCHANGED <<now, created, objects, pending, fRewrittenAt>> /\ UNCHANGED gcVars

\* The fork's full compaction commits in its own database: its newest manifest stops naming this
\* database's SSTs; its first manifest is superseded now, and readers may hold it for MinAge.
ForkRewrite ==
  /\ fState = "reading"
  /\ fState' = "rewritten" /\ fRewrittenAt' = now
  /\ UNCHANGED <<now, created, objects, pending, chain, stored, fHeld>> /\ UNCHANGED gcVars

\* The fork's collection (GarbageCollector.collect on the fork) releases the hold by CAS here:
\* only when its retained manifests name none of this database's SSTs, i.e. its first manifest was
\* superseded at least MinAge before the collection's clock reading (<= now).
ForkRelease ==
  /\ fState = "rewritten" /\ CanAppend
  /\ Broken = "earlyForkRelease" \/ fRewrittenAt + MinAge <= now
  /\ Commit([Latest EXCEPT !.hold = 0])
  /\ fState' = "released"
  /\ UNCHANGED <<now, created, objects, pending, fHeld, fRewrittenAt>> /\ UNCHANGED gcVars

(***************************************************************************)
(* GC, step by step (GarbageCollector.collect).                            *)
(***************************************************************************)
Cutoff == IF gNow >= MinAge THEN gNow - MinAge ELSE 0

\* List the SST objects; `now` (and so the cutoff) is fixed for the run.
GcListSsts ==
  /\ gpc = "idle"
  /\ gpc' = "listed" /\ gNow' = now /\ gSsts' = objects
  /\ UNCHANGED <<gAdvance, gCommitted, gListed>>
  /\ UNCHANGED <<now, created, objects, pending, chain, stored>>

\* hasSstCollectionCandidates against the newest manifest.
GcPrecheck ==
  /\ gpc = "listed"
  /\ gAdvance' = \E s \in gSsts : s \notin Latest.ssts /\ created[s] < Cutoff
  /\ gpc' = "prechecked"
  /\ UNCHANGED <<gNow, gSsts, gCommitted, gListed>>
  /\ UNCHANGED <<now, created, objects, pending, chain, stored>>

\* prepareGarbageCollection by CAS: a new manifest only when the boundary moves.
GcCommit ==
  /\ gpc = "prechecked"
  /\ IF gAdvance /\ Cutoff > Latest.boundary
       THEN /\ CanAppend
            /\ Commit([Latest EXCEPT !.boundary = Cutoff])
            /\ gCommitted' = LatestId + 1
       ELSE /\ gCommitted' = LatestId /\ UNCHANGED <<chain, stored>>
  /\ gpc' = "committed"
  /\ UNCHANGED <<gNow, gSsts, gAdvance, gListed>>
  /\ UNCHANGED <<now, created, objects, pending>>

GcListManifests ==
  /\ gpc = "committed"
  /\ gListed' = stored /\ gpc' = "listedManifests"
  /\ UNCHANGED <<gNow, gSsts, gAdvance, gCommitted>>
  /\ UNCHANGED <<now, created, objects, pending, chain, stored>>

\* planManifestCollection: superseded when the next listed manifest was written.
NextListed(id) == CHOOSE j \in gListed : j > id /\ \A k \in gListed : k > id => j <= k
SupersededLongAgo(id) ==
  IF Broken = "ownUploadAge"
    THEN chain[id].written + MinAge <= gNow
    ELSE /\ \E j \in gListed : j > id
         /\ chain[NextListed(id)].written + MinAge <= gNow
Pinned(id) ==
  Broken # "ignorePins" /\ (id \in chain[gCommitted].pins \/ id = chain[gCommitted].hold)
Deletable == {id \in gListed : id < gCommitted /\ SupersededLongAgo(id) /\ ~Pinned(id)}
Retained == (gListed \ Deletable) \cup {gCommitted}

\* planSstCollection, then delete the SSTs.
GcDeleteSsts ==
  /\ gpc = "listedManifests"
  /\ LET live == UNION {chain[id].ssts : id \in Retained}
         bound == IF Broken = "noMinBoundary" THEN Cutoff
                  ELSE Min(chain[gCommitted].boundary, Cutoff)
     IN objects' = objects \ {s \in gSsts : s \notin live /\ created[s] < bound}
  /\ gpc' = "deletedSsts"
  /\ UNCHANGED <<gNow, gSsts, gAdvance, gCommitted, gListed>>
  /\ UNCHANGED <<now, created, pending, chain, stored>>

GcDeleteManifests ==
  /\ gpc = "deletedSsts"
  /\ stored' = stored \ Deletable
  /\ gpc' = "idle"
  /\ UNCHANGED <<gNow, gSsts, gAdvance, gCommitted, gListed>>
  /\ UNCHANGED <<now, created, objects, pending, chain>>

\* Everything this database's own actors and its GC do; the fork's variables stay.
DatabaseNext ==
  \/ Tick
  \/ \E s \in Ssts : Upload(s) \/ CommitFlush(s) \/ Discard(s) \/ DropSst(s)
  \/ \E s \in Ssts, inputs \in SUBSET Ssts : CommitCompaction(s, inputs)
  \* With the fork, its hold is the only checkpoint (other pins are covered without it).
  \/ ~WithFork /\ Pin
  \/ ~WithFork /\ \E p \in 1..MaxManifests : Unpin(p)
  \/ GcListSsts \/ GcPrecheck \/ GcCommit \/ GcListManifests \/ GcDeleteSsts \/ GcDeleteManifests

Next ==
  \/ DatabaseNext /\ UNCHANGED forkVars
  \/ ForkStart \/ ForkRewrite \/ ForkRelease

Spec == Init /\ [][Next]_vars

\* SST names are interchangeable: TLC checks one ordering of them.
SstSymmetry == Permutations(Ssts)

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ now \in 0..MaxTime
  /\ objects \subseteq Ssts /\ pending \subseteq Ssts
  /\ stored \subseteq 1..Len(chain) /\ LatestId \in stored
  /\ fState \in {"none", "reading", "rewritten", "released"}

\* A reader may hold the newest manifest, or one superseded less than MinAge ago (D29).
MayBeHeld(id) == id = LatestId \/ now < chain[id + 1].written + MinAge

\* Every SST a manifest a reader may hold references is still stored.
ReadableManifestsComplete ==
  \A id \in 1..LatestId : MayBeHeld(id) => chain[id].ssts \subseteq objects

\* Every checkpointed manifest (and the one the fork's hold pins) is stored with all its SSTs.
PinnedManifestsComplete ==
  \A p \in Latest.pins \cup ({Latest.hold} \ {0}) : p \in stored /\ chain[p].ssts \subseteq objects

\* A reader of the fork may hold its first manifest while the fork reads it, and for MinAge after the
\* rewrite replaced it (MayBeHeld in the fork's own chain, D29); every SST it names is still stored.
ForkReadsComplete ==
  (fState = "reading" \/ (fState \in {"rewritten", "released"} /\ now < fRewrittenAt + MinAge))
    => chain[fHeld].ssts \subseteq objects
=============================================================================
