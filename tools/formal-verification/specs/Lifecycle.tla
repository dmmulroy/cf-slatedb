------------------------------- MODULE Lifecycle -------------------------------
(***************************************************************************)
(* Three databases' lifecycles: a source, a fork of it, and a fork of the  *)
(* fork; each fork's hold on its owner, its rewrite and its release, GC on *)
(* every database, deleteCheckpoint, removal (interrupted at any step and  *)
(* resumed), a stale committer after a removal, recreation of a removed id *)
(* with readers that cached the old incarnation.                           *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/, lsm-core):              *)
(*  - forkManifest / forkRange: a fork reads its owner's SSTs through views *)
(*    and holds a checkpoint on the owner; a fork of a fork copies the     *)
(*    original owner's handles and holds a checkpoint on its own source.   *)
(*  - The fork's compaction rewrites them into its own SSTs. At its        *)
(*    collection it releases a hold once no manifest it keeps (the newest, *)
(*    one replaced less than gcMinAge ago, or one its checkpoints pin)     *)
(*    reads the owner (releasableExternalRefs over the retained manifests, *)
(*    D77).                                                                *)
(*  - GarbageCollector.collect: an owned SST is deleted only when no kept  *)
(*    manifest references it: the newest, the recently replaced, and every *)
(*    manifest a checkpoint (a hold) pins.                                 *)
(*  - DatabaseLifecycle.deleteCheckpoint: a hold cannot be deleted while   *)
(*    its fork exists (D91).                                               *)
(*  - DatabaseLifecycle.remove: markRemoved (refused while forks hold it), *)
(*    destroy the writer's and compactor's state, release the holds this  *)
(*    database has on its owner, delete its objects but keep the Removed   *)
(*    head as its tombstone (D91), mark it cleared. A retry resumes it.    *)
(*  - DatabaseLifecycle.create of a removed id: refused while the          *)
(*    tombstone is younger than gcMinAge (D90); every reader's cached      *)
(*    manifest of the old incarnation is gone by then (held at most half   *)
(*    of gcMinAge, D67).                                                   *)
(*                                                                         *)
(* Manifests are abstracted per database as the SSTs its newest manifest    *)
(* references (refs), the SSTs manifests replaced less than gcMinAge ago    *)
(* reference (recent), and, per hold, the SSTs of the manifest it pins.    *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"             the code as written                                *)
(*   "releaseNewestOnly" a fork releases its hold once its newest manifest *)
(*                      no longer reads the owner (before D77)             *)
(*   "releaseIgnoresPins" a fork releases its hold although a manifest its *)
(*                      own fork pins still reads the owner (before D77,   *)
(*                      the chain form)                                    *)
(*   "deleteHold"       deleteCheckpoint deletes a fork's hold (before D91) *)
(*   "deleteHead"       removal deletes its Removed head, so a stale       *)
(*                      committer reopens the database (before D91)        *)
(*   "recreateAtOnce"   a removed id is recreated at once (before D90)     *)
(*   "deleteByPrefix"   a removal deletes every object under the prefix,   *)
(*                      not only those written no later than its tombstone *)
(*   "staleViewOutlives" a committer's view outlives gcMinAge (D91's       *)
(*                      assumption dropped), so it can commit between      *)
(*                      create's clear and its first manifest              *)
(*   "beforeD116"       deleteCheckpoint deletes a hold whose holder has   *)
(*                      no manifest yet, while forkRange may still write it *)
(*   "compensateAnyHold" a retried forkRange that finds the fork not its   *)
(*                      own removes the hold it found, not only one it     *)
(*                      created (before D136)                              *)
(*   "retryTrustsFirstManifest" a retried forkRange decides the fork is   *)
(*                      its own from the fork's first manifest, which the  *)
(*                      fork's GC deletes, and removes the hold it found   *)
(*                      (the code before D136)                             *)
(*                                                                         *)
(* Holds have ids (D136): <<fork, 1>> is the hold a fork was created with, *)
(* <<fork, 2>> one a retried forkRange commits when it finds none.          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Broken,
  ForkRace,     \* TRUE: the fork of the fork is created during the behavior, racing deleteCheckpoint (D116)
  SecondRemover, \* TRUE: a second removal of the fork of the fork (a retried remove, or the sweep) runs
                 \* beside the first, and may stall past gcMinAge and a recreation
  ForkRetry      \* TRUE: forkRange of the fork of the fork is called again after it returned, while the
                 \* fork of the fork commits newer manifests, collects its first, rewrites, and releases (D136)

ASSUME Broken \in {"none", "releaseNewestOnly", "releaseIgnoresPins", "deleteHold", "deleteHead",
                   "recreateAtOnce", "beforeD116", "deleteByPrefix", "staleViewOutlives",
                   "compensateAnyHold", "retryTrustsFirstManifest"}

DBs == {"src", "fork", "fof"}
Forks == {"fork", "fof"}
Parent(c) == IF c = "fork" THEN "src" ELSE "fork"
\* Each database's own SST: the source's data, and each fork's rewrite.
Own(db) == db
SSTs == DBs
Owner(s) == s
\* A hold's id: its holder, and 1 (the hold it was created with) or 2 (one a retry committed).
HoldIds == Forks \X {1, 2}

VARIABLES
  live,       \* each database: "open" | "removed"
  refs,       \* SSTs the newest manifest references
  recent,     \* SSTs manifests replaced less than gcMinAge ago reference
  holds,      \* holds[owner]: the holds (HoldIds) forks have on it
  pinned,     \* pinned[c]: the SSTs of the owner's manifest c's hold pins
  stored,     \* SSTs not deleted
  rpc,        \* each database's removal: "none" | "marked" | "destroyed" | "released" | "deleted" | "cleared"
  stale,      \* a committer holding a view from before the removal is still in flight
  aged,       \* the tombstone is older than gcMinAge
  inc,        \* each database's incarnation
  cache,      \* the incarnation a reader cached a manifest of (0: none)
  wrongInc,   \* a reader served a manifest of another incarnation
  reopened,   \* a removed incarnation answered as open again
  fpc,        \* the fork of the fork's creation (forkRange): "idle" | "held" | "done" | "dead"
  dpc,        \* an operator's deleteCheckpoint of that fork's hold: "idle" | "read" | "done"
  dLive,      \* what its read of the holder found: TRUE when the hold must stay
  r2,         \* the second removal of "fof": "none" | "marked" | "destroyed" | "released" | "done"
  newObj,     \* each database's recreated incarnation's first manifest exists
  rcl,        \* create of a removed id: "idle" | "cleared" (the old objects, its head among them, gone)
  rf,         \* the retried forkRange of the fork of the fork: "idle" | "acquired" | "done"
  rh,         \* the hold it found: "none" | "existing" (taken before it) | "created" (by it)
  first       \* the fork of the fork's first manifest: "kept" | "replaced" | "deleted" (by its GC)

vars == <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
          reopened, fpc, dpc, dLive, r2, newObj, rcl, rf, rh, first>>
\* The variables the retried forkRange and the fork of the fork's first manifest own.
retryVars == <<rf, rh, first>>

\* The forks holding a checkpoint on d.
HoldersOf(d) == {h[1] : h \in holds[d]}
\* d's holds without c's.
HoldsWithout(d, c) == {h \in holds[d] : h[1] # c}
\* The variables the fork-creation and hold-deletion processes own.
procVars == <<fpc, dpc, dLive, r2, newObj, rcl>>

Init ==
  \* The fork of the fork is created during the behavior (forkRange: its hold, then its manifest).
  /\ live = [d \in DBs |-> IF d = "fof" /\ ForkRace THEN "none" ELSE "open"]
  \* The fork and the fork of the fork read the source's SST through views.
  /\ refs = [d \in DBs |-> {"src"}]
  /\ recent = [d \in DBs |-> {}]
  /\ holds = [d \in DBs |-> IF d = "src" THEN {<<"fork", 1>>}
                            ELSE IF d = "fork" /\ ~ForkRace THEN {<<"fof", 1>>}
                            ELSE {}]
  /\ pinned = [c \in Forks |-> IF c = "fork" \/ ~ForkRace THEN {"src"} ELSE {}]
  /\ stored = SSTs \ {"fork", "fof"}
  /\ rpc = [d \in DBs |-> "none"]
  /\ stale = [d \in DBs |-> TRUE]
  /\ aged = [d \in DBs |-> FALSE]
  /\ inc = [d \in DBs |-> 1]
  /\ cache = [d \in DBs |-> 0]
  /\ wrongInc = FALSE /\ reopened = FALSE
  /\ fpc = IF ForkRace THEN "idle" ELSE "done"
  /\ dpc = IF ForkRace THEN "idle" ELSE "done"
  /\ dLive = FALSE
  /\ r2 = "none" /\ newObj = [d \in DBs |-> FALSE] /\ rcl = [d \in DBs |-> "idle"]
  /\ rf = "idle" /\ rh = "none" /\ first = "kept"

(* ----- A fork's rewrite and release ----- *)
\* The fork's compaction rewrites what it reads from its owner into its own SST.
Rewrite(c) ==
  /\ live[c] = "open" /\ refs[c] # {Own(c)}
  /\ refs' = [refs EXCEPT ![c] = {Own(c)}]
  /\ recent' = [recent EXCEPT ![c] = @ \cup refs[c]]
  /\ stored' = stored \cup {Own(c)}
  \* The rewrite's manifest replaces the fork of the fork's first.
  /\ first' = IF c = "fof" /\ first = "kept" THEN "replaced" ELSE first
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, holds, pinned, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 rf, rh>>

\* gcMinAge passes: the replaced manifests may be collected, and no reader holds them any longer.
Expire(d) ==
  /\ recent[d] # {}
  /\ recent' = [recent EXCEPT ![d] = {}]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

\* What a database still reads from SSTs others own, in every manifest it keeps.
Kept(c) ==
  IF Broken = "releaseNewestOnly" THEN refs[c]
  ELSE refs[c] \cup recent[c] \cup
       (IF Broken = "releaseIgnoresPins" THEN {}
        ELSE UNION {pinned[g] : g \in HoldersOf(c)})

\* At its collection the fork releases its hold once nothing it keeps reads an SST another database
\* owns (releasableExternalRefs: a fork of a fork reads the original owner's files through its
\* hold on its own source).
Release(c) ==
  /\ live[c] = "open" /\ c \in HoldersOf(Parent(c))
  /\ Kept(c) \subseteq {Own(c)}
  /\ holds' = [holds EXCEPT ![Parent(c)] = HoldsWithout(Parent(c), c)]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

(* ----- GC on every database ----- *)
\* What GC of d keeps: its newest and recent manifests, and every manifest a hold on it pins.
Protected(d) == refs[d] \cup recent[d] \cup UNION {pinned[g] : g \in HoldersOf(d)}

Collect(d) ==
  /\ live[d] = "open"
  /\ \E s \in stored : Owner(s) = d /\ s \notin Protected(d)
  /\ stored' = stored \ {s \in stored : Owner(s) = d /\ s \notin Protected(d)}
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, pinned, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

\* An operator deletes a fork's hold (D91: refused while the fork exists).
DeleteHold(d) ==
  /\ Broken = "deleteHold" /\ holds[d] # {}
  /\ holds' = [holds EXCEPT ![d] = {}]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

(* ----- Removal, interrupted anywhere and resumed ----- *)
\* Each step is one awaited effect of remove; a stopped removal is resumed by its retry (or, once
\* cleared and old enough, finished by the maintenance sweep).
RemoveStep(d) ==
  \/ /\ rpc[d] = "none" /\ live[d] = "open" /\ holds[d] = {}
     /\ rpc' = [rpc EXCEPT ![d] = "marked"] /\ live' = [live EXCEPT ![d] = "removed"]
     /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, refs, recent, holds, stored, cache>>
  \/ /\ rpc[d] = "marked"
     /\ rpc' = [rpc EXCEPT ![d] = "destroyed"]
     /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, stored, cache>>
  \/ /\ rpc[d] = "destroyed"
     \* A removed fork releases the hold it has on its owner.
     /\ holds' = IF d \in Forks THEN [holds EXCEPT ![Parent(d)] = HoldsWithout(Parent(d), d)] ELSE holds
     /\ rpc' = [rpc EXCEPT ![d] = "released"]
     /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, stored, cache>>
  \/ /\ rpc[d] = "released"
     /\ stored' = {s \in stored : Owner(s) # d}
     /\ refs' = [refs EXCEPT ![d] = {}] /\ recent' = [recent EXCEPT ![d] = {}]
     /\ rpc' = [rpc EXCEPT ![d] = "deleted"]
     /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, holds, cache>>
  \/ /\ rpc[d] = "deleted"
     /\ rpc' = [rpc EXCEPT ![d] = "cleared"]
     /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, stored, cache>>
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, pinned, stale, aged, inc, wrongInc, reopened>>

\* A second removal of the fork of the fork (a retried remove, or the maintenance sweep finishing
\* it) that read the Removed head, beside the first, and may stall past gcMinAge and a recreation.
\* Each step repeats the first removal's; its deletes take only objects written no later than the
\* tombstone it read, so nothing of a later incarnation (deleteByPrefix: everything).
Remove2 ==
  /\ SecondRemover
  /\ \/ /\ r2 = "none" /\ live["fof"] = "removed" /\ inc["fof"] = 1
        /\ r2' = "marked" /\ UNCHANGED <<holds, stored, refs, recent, newObj>>
     \/ /\ r2 = "marked" /\ r2' = "destroyed" /\ UNCHANGED <<holds, stored, refs, recent, newObj>>
     \/ /\ r2 = "destroyed"
        /\ holds' = [holds EXCEPT !["fork"] = HoldsWithout("fork", "fof")]
        /\ r2' = "released" /\ UNCHANGED <<stored, refs, recent, newObj>>
     \/ /\ r2 = "released"
        /\ IF inc["fof"] = 1
             THEN /\ stored' = {x \in stored : Owner(x) # "fof"}
                  /\ refs' = [refs EXCEPT !["fof"] = {}] /\ recent' = [recent EXCEPT !["fof"] = {}]
             ELSE UNCHANGED <<stored, refs, recent>>
        /\ newObj' = IF Broken = "deleteByPrefix" THEN [newObj EXCEPT !["fof"] = FALSE] ELSE newObj
        /\ r2' = "done" /\ UNCHANGED holds
  /\ UNCHANGED <<fpc, dpc, dLive, rcl, live, pinned, rpc, stale, aged, inc, cache, wrongInc, reopened>>

\* A committer that loaded the newest manifest before the removal commits after its deletes. The
\* Removed head stays (D91), so its create loses, it reloads, finds Removed, and is refused.
StaleCommit(d) ==
  /\ stale[d] /\ rpc[d] \in {"deleted", "cleared"}
  \* D91: no view outlives gcMinAge, so none commits once the tombstone is that old.
  /\ ~aged[d] \/ Broken = "staleViewOutlives"
  /\ stale' = [stale EXCEPT ![d] = FALSE]
  /\ IF Broken = "deleteHead" \/ rcl[d] = "cleared"
       THEN /\ live' = [live EXCEPT ![d] = "open"] /\ reopened' = TRUE
       ELSE UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, reopened>>
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, refs, recent, holds, pinned, stored, rpc, aged, inc, cache, wrongInc>>

\* gcMinAge passes after the removal: every reader's cache of the old incarnation has ended.
TombstoneAges(d) ==
  /\ rpc[d] # "none" /\ live[d] = "removed" /\ ~aged[d]
  /\ aged' = [aged EXCEPT ![d] = TRUE] /\ cache' = [cache EXCEPT ![d] = 0]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, pinned, stored, rpc, stale, inc, wrongInc, reopened>>

\* create of a removed id: refused while the tombstone is young (D90); it clears what is left.
RecreateClear(d) ==
  /\ live[d] = "removed" /\ rpc[d] = "cleared" /\ inc[d] = 1 /\ rcl[d] = "idle"
  /\ aged[d] \/ Broken = "recreateAtOnce"
  /\ rcl' = [rcl EXCEPT ![d] = "cleared"]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, live, refs, recent, holds, pinned, stored, rpc, stale,
                 aged, inc, cache, wrongInc, reopened>>

\* ... then manifest 1 of the new chain (after awaits).
Recreate(d) ==
  /\ live[d] = "removed" /\ rpc[d] = "cleared" /\ inc[d] = 1 /\ rcl[d] = "cleared"
  /\ rcl' = [rcl EXCEPT ![d] = "idle"]
  /\ live' = [live EXCEPT ![d] = "open"] /\ inc' = [inc EXCEPT ![d] = 2]
  /\ refs' = [refs EXCEPT ![d] = {}] /\ rpc' = [rpc EXCEPT ![d] = "none"]
  /\ aged' = [aged EXCEPT ![d] = FALSE] /\ stale' = [stale EXCEPT ![d] = FALSE]
  /\ newObj' = [newObj EXCEPT ![d] = TRUE]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, recent, holds, pinned, stored, cache, wrongInc, reopened>>

(* ----- The fork of the fork being created, and an operator deleting its hold ----- *)
\* forkRange of the fork, step 1: the hold on its source, pinning the source's newest manifest.
ForkHold ==
  /\ fpc = "idle" /\ live["fork"] = "open"
  /\ holds' = [holds EXCEPT !["fork"] = @ \cup {<<"fof", 1>>}]
  /\ pinned' = [pinned EXCEPT !["fof"] = refs["fork"]]
  /\ fpc' = "held"
  /\ UNCHANGED <<live, refs, recent, stored, rpc, stale, aged, inc, cache, wrongInc, reopened, dpc,
                 dLive, r2, newObj, rcl>>

\* Step 2, after an await: the fork's first manifest, reading what the hold pins.
ForkInit ==
  /\ fpc = "held"
  /\ live' = [live EXCEPT !["fof"] = "open"] /\ refs' = [refs EXCEPT !["fof"] = pinned["fof"]]
  /\ fpc' = "done"
  /\ UNCHANGED <<recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 dpc, dLive, r2, newObj, rcl>>

\* forkRange dies between its hold and the fork's first manifest: the hold is an orphan.
ForkDies ==
  /\ fpc = "held" /\ fpc' = "dead"
  /\ UNCHANGED <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, dpc, dLive, r2, newObj, rcl>>

\* deleteCheckpoint of the fork's hold: it reads the holder's head (an await) ... A removed holder
\* no longer reads the hold. A holder with no manifest is gone only once the hold is gcMinAge old
\* (D116); a forkRange still between its hold and the fork's first manifest is far younger, so
\* while fpc = "held" the hold is young.
DeleteHoldRead ==
  /\ dpc = "idle" /\ "fof" \in HoldersOf("fork")
  /\ dLive' = \/ live["fof"] = "open"
              \/ live["fof"] = "none" /\ fpc = "held" /\ Broken # "beforeD116"
  /\ dpc' = "read"
  /\ UNCHANGED <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, fpc, r2, newObj, rcl>>

\* ... and deletes the hold only if the holder was gone (D91, D116). Refused while the fork does not
\* exist, the operator retries (the hold may be an orphan); a refusal for a created fork is final.
DeleteHoldCommit ==
  /\ dpc = "read"
  /\ holds' = IF ~dLive THEN [holds EXCEPT !["fork"] = HoldsWithout("fork", "fof")] ELSE holds
  /\ dpc' = IF dLive /\ live["fof"] = "none" THEN "idle" ELSE "done"
  /\ UNCHANGED <<live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, fpc, dLive, r2, newObj, rcl>>

(* ----- forkRange of the fork of the fork, called again (D136) ----- *)
\* The fork of the fork commits a newer manifest that still reads what its hold pins (a flush): its
\* first manifest is replaced, and stays readable for gcMinAge.
ForkFlush ==
  /\ ForkRetry /\ live["fof"] = "open" /\ first = "kept"
  /\ first' = "replaced"
  /\ recent' = [recent EXCEPT !["fof"] = @ \cup refs["fof"]]
  /\ UNCHANGED <<live, refs, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 fpc, dpc, dLive, r2, newObj, rcl, rf, rh>>

\* gcMinAge after its replacement, the fork of the fork's GC deletes its first manifest.
CollectFirst ==
  /\ first = "replaced" /\ recent["fof"] = {}
  /\ first' = "deleted"
  /\ UNCHANGED <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, fpc, dpc, dLive, r2, newObj, rcl, rf, rh>>

\* The retry, step 1: the fork's hold on its source pinning the checkpoint's manifest. It finds the
\* one there (taken by the first call), or commits a new one (the fork released its own).
RetryAcquire ==
  /\ ForkRetry /\ rf = "idle" /\ fpc = "done"
  /\ live["fof"] = "open" /\ inc["fof"] = 1 /\ live["fork"] = "open"
  /\ IF "fof" \in HoldersOf("fork")
       THEN /\ rh' = "existing" /\ UNCHANGED holds
       ELSE /\ rh' = "created" /\ holds' = [holds EXCEPT !["fork"] = @ \cup {<<"fof", 2>>}]
  /\ rf' = "acquired"
  /\ UNCHANGED <<live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 fpc, dpc, dLive, r2, newObj, rcl, first>>

\* Step 2, after awaits: the fork's first manifest exists (or its id is at or below GC's boundary),
\* so initialize fails with DatabaseAlreadyExists. The retry returns the fork as its own when the
\* fork's newest manifest reads the hold it found; otherwise it undoes only a hold it created.
\* Before D136 it asked the first manifest instead, and undid whatever hold it had found.
RetryInit ==
  /\ rf = "acquired"
  /\ LET held == <<"fof", IF rh = "created" THEN 2 ELSE 1>>
         headReadsHold == live["fof"] = "open" /\ rh = "existing" /\ refs["fof"] \ {Own("fof")} # {}
         firstReadsHold == first = "kept" /\ rh = "existing"
         ours == IF Broken = "retryTrustsFirstManifest" THEN firstReadsHold ELSE headReadsHold
         undo == ~ours /\ (rh = "created" \/ Broken \in {"compensateAnyHold", "retryTrustsFirstManifest"})
     IN holds' = IF undo THEN [holds EXCEPT !["fork"] = @ \ {held}] ELSE holds
  /\ rf' = "done"
  /\ UNCHANGED <<live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 fpc, dpc, dLive, r2, newObj, rcl, rh, first>>

(* ----- A reader's manifest cache ----- *)
ReaderLoad(d) ==
  /\ live[d] = "open" /\ cache[d] = 0
  /\ cache' = [cache EXCEPT ![d] = inc[d]]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, wrongInc,
                 reopened>>

\* A read served from the cache (a covering hit, or within the refresh interval).
ReaderServe(d) ==
  /\ live[d] = "open" /\ cache[d] # 0
  /\ wrongInc' = (wrongInc \/ cache[d] # inc[d])
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache,
                 reopened>>

ReaderForget(d) ==
  /\ cache[d] # 0
  /\ cache' = [cache EXCEPT ![d] = 0]
  /\ UNCHANGED <<fpc, dpc, dLive, r2, newObj, rcl, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, wrongInc,
                 reopened>>

\* The actions that leave the retry's variables alone.
NextWithoutRetry ==
  \/ \E c \in Forks : Release(c)
  \/ Remove2 \/ ForkHold \/ ForkInit \/ ForkDies \/ DeleteHoldRead \/ DeleteHoldCommit
  \/ \E d \in DBs : Expire(d) \/ Collect(d) \/ DeleteHold(d) \/ RemoveStep(d) \/ StaleCommit(d)
                    \/ TombstoneAges(d) \/ RecreateClear(d) \/ Recreate(d) \/ ReaderLoad(d) \/ ReaderServe(d)
                    \/ ReaderForget(d)

Next ==
  \/ NextWithoutRetry /\ UNCHANGED retryVars
  \/ \E c \in Forks : Rewrite(c)
  \/ ForkFlush \/ CollectFirst \/ RetryAcquire \/ RetryInit

\* Compactions, collections, and time run; a stopped removal is retried.
Fairness ==
  /\ \A c \in Forks : WF_vars(Rewrite(c)) /\ WF_vars(Release(c) /\ UNCHANGED retryVars)
  /\ WF_vars(ForkInit /\ UNCHANGED retryVars) /\ WF_vars(RetryInit)
  \* An operator repairs an orphan hold (its fork was never created) with deleteCheckpoint.
  /\ WF_vars(fpc = "dead" /\ DeleteHoldRead /\ UNCHANGED retryVars)
  /\ WF_vars(DeleteHoldCommit /\ UNCHANGED retryVars)
  /\ \A d \in DBs : /\ WF_vars(Expire(d) /\ UNCHANGED retryVars)
                    /\ WF_vars(Collect(d) /\ UNCHANGED retryVars)
                    /\ WF_vars(RemoveStep(d) /\ UNCHANGED retryVars)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ \A d \in DBs : live[d] \in {"none", "open", "removed"} /\ refs[d] \subseteq SSTs
  /\ \A d \in DBs : holds[d] \subseteq HoldIds
  /\ stored \subseteq SSTs
  /\ rf \in {"idle", "acquired", "done"} /\ rh \in {"none", "existing", "created"}
  /\ first \in {"kept", "replaced", "deleted"}

\* No manifest any open database may still read (its newest, one a reader may hold, or one a
\* hold pins) needs a deleted SST.
NoReaderNeedsDeleted ==
  \A d \in DBs : live[d] = "open" => (refs[d] \cup recent[d]) \subseteq stored

\* A recreated database's first manifest is never deleted by a removal of the old incarnation.
NewIncarnationIntact == \A d \in DBs : (inc[d] = 2 /\ live[d] = "open") => newObj[d]

\* A removed database never answers as open again (D91).
RemovedNeverReopens == ~reopened

\* A recreated database never serves a manifest of its old incarnation (D90).
NoOldIncarnationServed == ~wrongInc

\* A removed database's prefix is eventually empty (its removal resumed to the end).
PrefixEmptied ==
  \A d \in DBs : (live[d] = "removed") ~> ({s \in stored : Owner(s) = d} = {} \/ live[d] = "open")

\* Every created fork's hold on its owner is eventually released. (A hold whose forkRange died
\* before the fork's first manifest is an orphan, deleted by an operator once gcMinAge old.)
HoldsReleased ==
  \A c \in Forks : (c \in HoldersOf(Parent(c)) /\ live[c] # "none") ~> (c \notin HoldersOf(Parent(c)))
=============================================================================
