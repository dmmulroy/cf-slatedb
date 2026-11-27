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
(*   "beforeD116"       deleteCheckpoint deletes a hold whose holder has   *)
(*                      no manifest yet, while forkRange may still write it *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Broken,
  ForkRace  \* TRUE: the fork of the fork is created during the behavior, racing deleteCheckpoint (D116)

ASSUME Broken \in {"none", "releaseNewestOnly", "releaseIgnoresPins", "deleteHold", "deleteHead",
                   "recreateAtOnce", "beforeD116"}

DBs == {"src", "fork", "fof"}
Forks == {"fork", "fof"}
Parent(c) == IF c = "fork" THEN "src" ELSE "fork"
\* Each database's own SST: the source's data, and each fork's rewrite.
Own(db) == db
SSTs == DBs
Owner(s) == s

VARIABLES
  live,       \* each database: "open" | "removed"
  refs,       \* SSTs the newest manifest references
  recent,     \* SSTs manifests replaced less than gcMinAge ago reference
  holds,      \* holds[owner]: the forks holding a checkpoint on it
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
  dLive       \* what its read of the holder found: TRUE when the hold must stay

vars == <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
          reopened, fpc, dpc, dLive>>
\* The variables the fork-creation and hold-deletion processes own.
procVars == <<fpc, dpc, dLive>>

Init ==
  \* The fork of the fork is created during the behavior (forkRange: its hold, then its manifest).
  /\ live = [d \in DBs |-> IF d = "fof" /\ ForkRace THEN "none" ELSE "open"]
  \* The fork and the fork of the fork read the source's SST through views.
  /\ refs = [d \in DBs |-> {"src"}]
  /\ recent = [d \in DBs |-> {}]
  /\ holds = [d \in DBs |-> IF d = "src" THEN {"fork"} ELSE IF d = "fork" /\ ~ForkRace THEN {"fof"}
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

(* ----- A fork's rewrite and release ----- *)
\* The fork's compaction rewrites what it reads from its owner into its own SST.
Rewrite(c) ==
  /\ live[c] = "open" /\ refs[c] # {Own(c)}
  /\ refs' = [refs EXCEPT ![c] = {Own(c)}]
  /\ recent' = [recent EXCEPT ![c] = @ \cup refs[c]]
  /\ stored' = stored \cup {Own(c)}
  /\ UNCHANGED <<fpc, dpc, dLive, live, holds, pinned, rpc, stale, aged, inc, cache, wrongInc, reopened>>

\* gcMinAge passes: the replaced manifests may be collected, and no reader holds them any longer.
Expire(d) ==
  /\ recent[d] # {}
  /\ recent' = [recent EXCEPT ![d] = {}]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

\* What a database still reads from SSTs others own, in every manifest it keeps.
Kept(c) ==
  IF Broken = "releaseNewestOnly" THEN refs[c]
  ELSE refs[c] \cup recent[c] \cup
       (IF Broken = "releaseIgnoresPins" THEN {}
        ELSE UNION {pinned[g] : g \in {x \in Forks : x \in holds[c]}})

\* At its collection the fork releases its hold once nothing it keeps reads an SST another database
\* owns (releasableExternalRefs: a fork of a fork reads the original owner's files through its
\* hold on its own source).
Release(c) ==
  /\ live[c] = "open" /\ c \in holds[Parent(c)]
  /\ Kept(c) \subseteq {Own(c)}
  /\ holds' = [holds EXCEPT ![Parent(c)] = @ \ {c}]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

(* ----- GC on every database ----- *)
\* What GC of d keeps: its newest and recent manifests, and every manifest a hold on it pins.
Protected(d) == refs[d] \cup recent[d] \cup UNION {pinned[g] : g \in holds[d]}

Collect(d) ==
  /\ live[d] = "open"
  /\ \E s \in stored : Owner(s) = d /\ s \notin Protected(d)
  /\ stored' = stored \ {s \in stored : Owner(s) = d /\ s \notin Protected(d)}
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, pinned, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

\* An operator deletes a fork's hold (D91: refused while the fork exists).
DeleteHold(d) ==
  /\ Broken = "deleteHold" /\ holds[d] # {}
  /\ holds' = [holds EXCEPT ![d] = {}]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened>>

(* ----- Removal, interrupted anywhere and resumed ----- *)
\* Each step is one awaited effect of remove; a stopped removal is resumed by its retry (or, once
\* cleared and old enough, finished by the maintenance sweep).
RemoveStep(d) ==
  \/ /\ rpc[d] = "none" /\ live[d] = "open" /\ holds[d] = {}
     /\ rpc' = [rpc EXCEPT ![d] = "marked"] /\ live' = [live EXCEPT ![d] = "removed"]
     /\ UNCHANGED <<fpc, dpc, dLive, refs, recent, holds, stored, cache>>
  \/ /\ rpc[d] = "marked"
     /\ rpc' = [rpc EXCEPT ![d] = "destroyed"]
     /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, stored, cache>>
  \/ /\ rpc[d] = "destroyed"
     \* A removed fork releases the hold it has on its owner.
     /\ holds' = IF d \in Forks THEN [holds EXCEPT ![Parent(d)] = @ \ {d}] ELSE holds
     /\ rpc' = [rpc EXCEPT ![d] = "released"]
     /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, stored, cache>>
  \/ /\ rpc[d] = "released"
     /\ stored' = {s \in stored : Owner(s) # d}
     /\ refs' = [refs EXCEPT ![d] = {}] /\ recent' = [recent EXCEPT ![d] = {}]
     /\ rpc' = [rpc EXCEPT ![d] = "deleted"]
     /\ UNCHANGED <<fpc, dpc, dLive, live, holds, cache>>
  \/ /\ rpc[d] = "deleted"
     /\ rpc' = [rpc EXCEPT ![d] = "cleared"]
     /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, stored, cache>>
  /\ UNCHANGED <<fpc, dpc, dLive, pinned, stale, aged, inc, wrongInc, reopened>>

\* A committer that loaded the newest manifest before the removal commits after its deletes. The
\* Removed head stays (D91), so its create loses, it reloads, finds Removed, and is refused.
StaleCommit(d) ==
  /\ stale[d] /\ rpc[d] \in {"deleted", "cleared"}
  /\ stale' = [stale EXCEPT ![d] = FALSE]
  /\ IF Broken = "deleteHead"
       THEN /\ live' = [live EXCEPT ![d] = "open"] /\ reopened' = TRUE
       ELSE UNCHANGED <<fpc, dpc, dLive, live, reopened>>
  /\ UNCHANGED <<fpc, dpc, dLive, refs, recent, holds, pinned, stored, rpc, aged, inc, cache, wrongInc>>

\* gcMinAge passes after the removal: every reader's cache of the old incarnation has ended.
TombstoneAges(d) ==
  /\ rpc[d] # "none" /\ live[d] = "removed" /\ ~aged[d]
  /\ aged' = [aged EXCEPT ![d] = TRUE] /\ cache' = [cache EXCEPT ![d] = 0]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, pinned, stored, rpc, stale, inc, wrongInc, reopened>>

\* create of a removed id: refused while the tombstone is young (D90); it clears what is left.
Recreate(d) ==
  /\ live[d] = "removed" /\ rpc[d] = "cleared" /\ inc[d] = 1
  /\ aged[d] \/ Broken = "recreateAtOnce"
  /\ live' = [live EXCEPT ![d] = "open"] /\ inc' = [inc EXCEPT ![d] = 2]
  /\ refs' = [refs EXCEPT ![d] = {}] /\ rpc' = [rpc EXCEPT ![d] = "none"]
  /\ aged' = [aged EXCEPT ![d] = FALSE] /\ stale' = [stale EXCEPT ![d] = FALSE]
  /\ UNCHANGED <<fpc, dpc, dLive, recent, holds, pinned, stored, cache, wrongInc, reopened>>

(* ----- The fork of the fork being created, and an operator deleting its hold ----- *)
\* forkRange of the fork, step 1: the hold on its source, pinning the source's newest manifest.
ForkHold ==
  /\ fpc = "idle" /\ live["fork"] = "open"
  /\ holds' = [holds EXCEPT !["fork"] = @ \cup {"fof"}]
  /\ pinned' = [pinned EXCEPT !["fof"] = refs["fork"]]
  /\ fpc' = "held"
  /\ UNCHANGED <<live, refs, recent, stored, rpc, stale, aged, inc, cache, wrongInc, reopened, dpc,
                 dLive>>

\* Step 2, after an await: the fork's first manifest, reading what the hold pins.
ForkInit ==
  /\ fpc = "held"
  /\ live' = [live EXCEPT !["fof"] = "open"] /\ refs' = [refs EXCEPT !["fof"] = pinned["fof"]]
  /\ fpc' = "done"
  /\ UNCHANGED <<recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc, reopened,
                 dpc, dLive>>

\* forkRange dies between its hold and the fork's first manifest: the hold is an orphan.
ForkDies ==
  /\ fpc = "held" /\ fpc' = "dead"
  /\ UNCHANGED <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, dpc, dLive>>

\* deleteCheckpoint of the fork's hold: it reads the holder's head (an await) ... A removed holder
\* no longer reads the hold. A holder with no manifest is gone only once the hold is gcMinAge old
\* (D116); a forkRange still between its hold and the fork's first manifest is far younger, so
\* while fpc = "held" the hold is young.
DeleteHoldRead ==
  /\ dpc = "idle" /\ "fof" \in holds["fork"]
  /\ dLive' = \/ live["fof"] = "open"
              \/ live["fof"] = "none" /\ fpc = "held" /\ Broken # "beforeD116"
  /\ dpc' = "read"
  /\ UNCHANGED <<live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, fpc>>

\* ... and deletes the hold only if the holder was gone (D91, D116). Refused, the operator retries
\* while the hold is an orphan (its forkRange died); a refusal for a created fork is final.
DeleteHoldCommit ==
  /\ dpc = "read"
  /\ holds' = IF ~dLive THEN [holds EXCEPT !["fork"] = @ \ {"fof"}] ELSE holds
  /\ dpc' = IF dLive /\ fpc = "dead" THEN "idle" ELSE "done"
  /\ UNCHANGED <<live, refs, recent, pinned, stored, rpc, stale, aged, inc, cache, wrongInc,
                 reopened, fpc, dLive>>

(* ----- A reader's manifest cache ----- *)
ReaderLoad(d) ==
  /\ live[d] = "open" /\ cache[d] = 0
  /\ cache' = [cache EXCEPT ![d] = inc[d]]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, wrongInc,
                 reopened>>

\* A read served from the cache (a covering hit, or within the refresh interval).
ReaderServe(d) ==
  /\ live[d] = "open" /\ cache[d] # 0
  /\ wrongInc' = (wrongInc \/ cache[d] # inc[d])
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, cache,
                 reopened>>

ReaderForget(d) ==
  /\ cache[d] # 0
  /\ cache' = [cache EXCEPT ![d] = 0]
  /\ UNCHANGED <<fpc, dpc, dLive, live, refs, recent, holds, pinned, stored, rpc, stale, aged, inc, wrongInc,
                 reopened>>

Next ==
  \/ \E c \in Forks : Rewrite(c) \/ Release(c)
  \/ ForkHold \/ ForkInit \/ ForkDies \/ DeleteHoldRead \/ DeleteHoldCommit
  \/ \E d \in DBs : Expire(d) \/ Collect(d) \/ DeleteHold(d) \/ RemoveStep(d) \/ StaleCommit(d)
                    \/ TombstoneAges(d) \/ Recreate(d) \/ ReaderLoad(d) \/ ReaderServe(d)
                    \/ ReaderForget(d)

\* Compactions, collections, and time run; a stopped removal is retried.
Fairness ==
  /\ \A c \in Forks : WF_vars(Rewrite(c)) /\ WF_vars(Release(c))
  /\ WF_vars(ForkInit)
  \* An operator repairs an orphan hold (its fork was never created) with deleteCheckpoint.
  /\ WF_vars(fpc = "dead" /\ DeleteHoldRead) /\ WF_vars(DeleteHoldCommit)
  /\ \A d \in DBs : WF_vars(Expire(d)) /\ WF_vars(Collect(d)) /\ WF_vars(RemoveStep(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ \A d \in DBs : live[d] \in {"none", "open", "removed"} /\ refs[d] \subseteq SSTs
  /\ stored \subseteq SSTs

\* No manifest any open database may still read (its newest, one a reader may hold, or one a
\* hold pins) needs a deleted SST.
NoReaderNeedsDeleted ==
  \A d \in DBs : live[d] = "open" => (refs[d] \cup recent[d]) \subseteq stored

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
  \A c \in Forks : (c \in holds[Parent(c)] /\ live[c] # "none") ~> (c \notin holds[Parent(c)])
=============================================================================
