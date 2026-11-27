------------------------------ MODULE ShardSplit ------------------------------
(***************************************************************************)
(* A shard split end to end: the directory's pending split, the seal with  *)
(* its holds, both forks, publishing the map, a crash after any step and   *)
(* its resumption by a waiting request, a router writing through a stale   *)
(* map, a router reading at Flushed, a change-feed consumer following the  *)
(* split, and a rogue seal naming other successors.                        *)
(*                                                                         *)
(* Code this models (packages/):                                           *)
(*  - lsm-sharding shard-split.ts split: beginSplit (record the pending    *)
(*    split), lifecycle.seal(shard, [left, right], holds), forkRange of    *)
(*    each half from its hold (in either order), completeSplit (publish).  *)
(*    Every step is idempotent; resumePending re-runs it from the record.  *)
(*  - lsm-sharding shard-router.ts: a write routed by a cached map to a    *)
(*    sealed shard (DatabaseSealed) or a fork not yet created              *)
(*    (DatabaseNotFound) waits on the directory for a newer map; after the *)
(*    routing wait it finishes the abandoned split (D101) and retries. A   *)
(*    Flushed read refreshes a map older than 1 s first (D89).             *)
(*  - db-writer.ts seal: a seal of a sealed database naming other          *)
(*    successors is refused (D95).                                         *)
(*  - The change feed (D80): a consumer reads the sealed source to its end *)
(*    (an empty page after the seal), then each successor from the        *)
(*    source's last sequence number; a successor's feed below its fork     *)
(*    point answers Expired.                                               *)
(*                                                                         *)
(* A write is <<key, value>>; values increase, so the newest is the        *)
(* largest. Flushed reads see every acknowledged write here: the staleness *)
(* modeled is the router's map, not the manifest refresh (D29).            *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"              the code as written                               *)
(*   "noResume"          a request that waited out the split does not      *)
(*                       finish it (before D101)                           *)
(*   "noFlushedRefresh"  a Flushed read uses its cached map however old    *)
(*                       (before D89)                                      *)
(*   "resealOthers"      a seal naming other successors succeeds and takes *)
(*                       holds for them (before D95)                       *)
(*   "feedGap"           the consumer leaves the source once it is sealed, *)
(*                       and a successor's feed below its fork point       *)
(*                       answers silently (before D80)                     *)
(*   "noDirectoryLock"   the directory's editMap without its lock: its      *)
(*                       cell.get and cell.set are two awaits, so a second  *)
(*                       beginSplit of the same shard can overwrite the     *)
(*                       pending record with its own successor ids (D116)   *)
(*   "forkRetryUndoesHold" a repeated forkRange of a half that exists      *)
(*                       removes the half's hold when it cannot find the   *)
(*                       half's first manifest (collected by its GC), as   *)
(*                       before D136                                       *)
(*                                                                         *)
(*   "resealTakesHolds"  a seal of the sealed shard takes a new hold for   *)
(*                       each successor without one (before D138, or a     *)
(*                       seal recorded without successorsHeld): a half     *)
(*                       that released its own gets one nothing releases   *)
(*   "noSkipReleased"    the split dies on a half with no hold instead of  *)
(*                       finding the half done (before D138)               *)
(*                                                                         *)
(* forkRange runs whenever the split's process reaches it, a half that     *)
(* already exists included (a resumed split repeats every step). A repeat  *)
(* finds the half its earlier call created, whose newest manifest reads    *)
(* the hold, and changes nothing (D136). A half releases its hold after    *)
(* its rewrite (Release, D77), possibly before the split published; a      *)
(* split resumed after that finds no hold for the half, and the half       *)
(* exists, so the half is done (D138).                                     *)
(*                                                                         *)
(* A second split request of the same shard and key (another client, or a *)
(* retry with its own process) runs beginSplit beside the first: each draws *)
(* fresh successor ids before editMap. Under the lock it adopts a pending   *)
(* record; its request then ends.                                          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxValues, MaxCrashes, Broken

ASSUME Broken \in {"none", "noResume", "noFlushedRefresh", "resealOthers", "feedGap",
                   "noDirectoryLock", "forkRetryUndoesHold", "resealTakesHolds", "noSkipReleased"}

Keys == {"a", "b"}
\* The split point: "a" goes left, "b" right.
Half(k) == IF k = "a" THEN "L" ELSE "R"
Forks == {"L", "R"}

VARIABLES
  dirVersion, \* the directory's map: 1 (the shard) or 2 (its halves)
  pending,    \* a split is recorded and not yet published
  spc,        \* the split: "idle" | "recorded" | "sealed" | "published" | "dead"
  crashes,
  sSealed,    \* the shard is sealed
  holds,      \* successors holding a checkpoint on the shard
  sData,      \* writes acknowledged by the shard
  sealedData, \* the shard's writes at its seal (what the holds pin)
  forked,     \* the forks that exist
  own,        \* writes acknowledged by each fork
  cpc,        \* the writer client: "idle" | "writing" | "waiting"
  ckey, cval, cmap, \* its write and its router's cached map version
  nextValue,
  rmap, rold, \* the reader's router map, and whether it is older than the bound
  staleRead,  \* a Flushed read through an old map returned an older value than a flushed one
  cons,       \* the feed consumer: "source" | "successors"
  drained,    \* its last read of the source came after the seal
  seen,       \* writes the consumer was given
  pids,       \* whose successor ids the pending record holds: 0 (none), 1, or 2
  myIds,      \* the ids the split's process seals and forks with
  b1,         \* its beginSplit: "idle" | "sawNone" | "sawPending" (noDirectoryLock: read, then write)
  p2,         \* the second request's beginSplit: "idle" | "sawNone" | "sawPending" | "done"
  misrouted,  \* a published map names successors other than the ones the seal named
  released,   \* halves whose compactor released their hold on the shard after the rewrite (D77)
  fdone       \* halves this run of the split has forked, or found forked

vars == <<dirVersion, pending, spc, crashes, sSealed, holds, sData, sealedData, forked, own, cpc,
          ckey, cval, cmap, nextValue, rmap, rold, staleRead, cons, drained, seen, pids, myIds, b1,
          p2, misrouted, released, fdone>>
relVars == <<released, fdone>>
dirVars == <<pids, myIds, b1, p2, misrouted>>
splitVars == <<dirVersion, pending, spc, crashes, sSealed, holds, sealedData, forked, dirVars,
               relVars>>
clientVars == <<cpc, ckey, cval, cmap, nextValue>>
readerVars == <<rmap, rold, staleRead>>
feedVars == <<cons, drained, seen>>

Init ==
  /\ dirVersion = 1 /\ pending = FALSE /\ spc = "idle" /\ crashes = 0
  /\ sSealed = FALSE /\ holds = {} /\ sData = {} /\ sealedData = {}
  /\ forked = {} /\ own = [f \in Forks |-> {}]
  /\ cpc = "idle" /\ ckey = "a" /\ cval = 0 /\ cmap = 1 /\ nextValue = 0
  /\ rmap = 1 /\ rold = FALSE /\ staleRead = FALSE
  /\ cons = "source" /\ drained = FALSE /\ seen = {}
  /\ pids = 0 /\ myIds = 0 /\ b1 = "idle" /\ p2 = "idle" /\ misrouted = FALSE
  /\ released = {} /\ fdone = {}

(* ----- The writer client and its router ----- *)
ClientStart(k) ==
  /\ cpc = "idle" /\ nextValue < MaxValues
  /\ cpc' = "writing" /\ ckey' = k /\ cval' = nextValue + 1 /\ nextValue' = nextValue + 1
  /\ UNCHANGED <<splitVars, sData, own, cmap, readerVars, feedVars>>

\* Routed by the cached map: the shard (version 1) or the key's half (version 2).
ClientWrite ==
  /\ cpc = "writing"
  /\ IF cmap = 1
       THEN IF sSealed
              THEN /\ cpc' = "waiting" /\ UNCHANGED <<sData, own>>
              ELSE /\ sData' = sData \cup {<<ckey, cval>>} /\ cpc' = "idle" /\ UNCHANGED own
       ELSE IF Half(ckey) \in forked
              THEN /\ own' = [own EXCEPT ![Half(ckey)] = @ \cup {<<ckey, cval>>}]
                   /\ cpc' = "idle" /\ UNCHANGED sData
              ELSE /\ cpc' = "waiting" /\ UNCHANGED <<sData, own>>
  /\ UNCHANGED <<splitVars, ckey, cval, cmap, nextValue, readerVars, feedVars>>

\* awaitMap: a newer published map, and the write is sent again.
ClientRefresh ==
  /\ cpc = "waiting" /\ dirVersion > cmap
  /\ cmap' = dirVersion /\ cpc' = "writing"
  /\ UNCHANGED <<splitVars, sData, own, ckey, cval, nextValue, readerVars, feedVars>>

\* The routing wait ran out with the split still pending and its process gone: the request
\* finishes it from the directory's record (D101).
ClientResume ==
  /\ UNCHANGED relVars
  /\ cpc = "waiting" /\ pending /\ spc = "dead" /\ Broken # "noResume"
  /\ spc' = "recorded" /\ myIds' = pids
  /\ UNCHANGED <<dirVersion, pending, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, pids, b1, p2, misrouted>>

(* ----- The split ----- *)
\* beginSplit: editMap records the pending split with this process's ids, or, finding one for
\* the same shard and key, returns it. Under the lock one step; without it, a read then a write.
DirLocked == Broken # "noDirectoryLock"

Begin ==
  /\ UNCHANGED relVars
  /\ spc = "idle" /\ b1 = "idle" /\ DirLocked
  /\ IF pending THEN myIds' = pids /\ UNCHANGED <<pending, pids>>
                ELSE pending' = TRUE /\ pids' = 1 /\ myIds' = 1
  /\ spc' = "recorded"
  /\ UNCHANGED <<dirVersion, crashes, sSealed, holds, sealedData, forked, sData, own, clientVars,
                 readerVars, feedVars, b1, p2, misrouted>>

BeginRead ==
  /\ UNCHANGED relVars
  /\ spc = "idle" /\ b1 = "idle" /\ ~DirLocked
  /\ b1' = IF pending THEN "sawPending" ELSE "sawNone"
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, pids, myIds, p2, misrouted>>

BeginWrite ==
  /\ UNCHANGED relVars
  /\ b1 \in {"sawNone", "sawPending"}
  /\ IF b1 = "sawNone" THEN pending' = TRUE /\ pids' = 1 /\ myIds' = 1
                       ELSE myIds' = pids /\ UNCHANGED <<pending, pids>>
  /\ spc' = "recorded" /\ b1' = "idle"
  /\ UNCHANGED <<dirVersion, crashes, sSealed, holds, sealedData, forked, sData, own, clientVars,
                 readerVars, feedVars, p2, misrouted>>

\* The second request's beginSplit (its own ids, 2); its request then ends, leaving any record.
Begin2 ==
  /\ UNCHANGED relVars
  /\ p2 = "idle" /\ DirLocked /\ dirVersion = 1
  /\ IF pending THEN UNCHANGED <<pending, pids>> ELSE pending' = TRUE /\ pids' = 2
  /\ p2' = "done"
  /\ UNCHANGED <<dirVersion, spc, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, myIds, b1, misrouted>>

Begin2Read ==
  /\ UNCHANGED relVars
  /\ p2 = "idle" /\ ~DirLocked /\ dirVersion = 1
  /\ p2' = IF pending THEN "sawPending" ELSE "sawNone"
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, pids, myIds, b1, misrouted>>

Begin2Write ==
  /\ UNCHANGED relVars
  /\ p2 \in {"sawNone", "sawPending"}
  /\ IF p2 = "sawNone" THEN pending' = TRUE /\ pids' = 2 ELSE UNCHANGED <<pending, pids>>
  /\ p2' = "done"
  /\ UNCHANGED <<dirVersion, spc, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, myIds, b1, misrouted>>

\* seal(shard, [L, R], holds): flushes and seals in one commit, taking both holds. A repeat finds it
\* done and takes no hold (D138): the holds it returns are the ones still there. Each run of the
\* split starts its forks after its seal.
Seal ==
  /\ spc = "recorded"
  /\ IF sSealed
       THEN /\ holds' = IF Broken = "resealTakesHolds" THEN holds \cup Forks ELSE holds
            /\ UNCHANGED <<sSealed, sealedData>>
       ELSE /\ sSealed' = TRUE /\ holds' = holds \cup Forks /\ sealedData' = sData
  /\ spc' = "sealed" /\ fdone' = {}
  /\ UNCHANGED <<dirVersion, pending, crashes, forked, sData, own, clientVars, readerVars, feedVars,
                 dirVars, released>>

\* The split's step for half f, after its seal:
\*  - a hold: forkRange from it. A repeat (the half exists) finds the half whose newest manifest
\*    reads the hold, returns it, and removes nothing (D136); before D136 it asked the half's first
\*    manifest, which the half's GC may have collected, and then removed the hold. A hold the half
\*    does not read (resealTakesHolds: taken after the half released its own) makes forkRange fail
\*    with DatabaseAlreadyExists; the half exists, so the split takes it as done, and the hold
\*    stays.
\*  - no hold, and the half exists: its compactor released the hold after its rewrite, so the half
\*    is done (D138). noSkipReleased: the split fails here, as it did before D138.
\*  - no hold and no half: the split fails (an operator deleted the hold; unreachable here).
Fork(f) ==
  /\ spc = "sealed" /\ f \notin fdone
  /\ CASE f \in holds /\ f \notin forked ->
            /\ forked' = forked \cup {f} /\ fdone' = fdone \cup {f}
            /\ UNCHANGED <<holds, spc>>
       [] f \in holds ->
            /\ fdone' = fdone \cup {f}
            /\ holds' = IF Broken = "forkRetryUndoesHold" THEN holds \ {f} ELSE holds
            /\ UNCHANGED <<forked, spc>>
       [] f \notin holds /\ f \in forked /\ Broken # "noSkipReleased" ->
            /\ fdone' = fdone \cup {f}
            /\ UNCHANGED <<forked, holds, spc>>
       [] OTHER ->
            /\ spc' = "dead"
            /\ UNCHANGED <<forked, holds, fdone>>
  /\ UNCHANGED <<dirVersion, pending, crashes, sSealed, sealedData, sData, own,
                 clientVars, readerVars, feedVars, dirVars, released>>

\* A half's compactor, gcMinAge after its rewrite, releases its hold on the shard (D77), whether or
\* not the split has published.
Release(f) ==
  /\ f \in forked /\ f \in holds /\ f \notin released
  /\ released' = released \cup {f} /\ holds' = holds \ {f}
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, dirVars, fdone>>

\* completeSplit publishes the successors the pending record names.
Publish ==
  /\ UNCHANGED relVars
  /\ spc = "sealed" /\ fdone = Forks
  /\ dirVersion' = 2 /\ pending' = FALSE /\ spc' = "published"
  /\ misrouted' = (misrouted \/ pids # myIds)
  /\ UNCHANGED <<crashes, sSealed, holds, sealedData, forked, sData, own, clientVars, readerVars,
                 feedVars, pids, myIds, b1, p2>>

\* The split's process dies after any step (its request ended, its Worker was evicted).
Crash ==
  /\ UNCHANGED relVars
  /\ spc \in {"recorded", "sealed"} /\ crashes < MaxCrashes
  /\ spc' = "dead" /\ crashes' = crashes + 1
  /\ UNCHANGED <<dirVersion, pending, sSealed, holds, sealedData, forked, sData, own, clientVars,
                 readerVars, feedVars, dirVars>>

\* Another caller seals the shard naming other successors (D95: refused).
ResealOthers ==
  /\ UNCHANGED relVars
  /\ sSealed /\ Broken = "resealOthers" /\ "L2" \notin holds
  /\ holds' = holds \cup {"L2"}
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars, dirVars>>

(* ----- A Flushed reader ----- *)
\* What a database holds for key k: the shard's writes, or a fork's pinned and own writes.
Newest(writes, k) ==
  LET values == {w[2] : w \in {x \in writes : x[1] = k}} IN
    IF values = {} THEN 0 ELSE CHOOSE v \in values : \A u \in values : u <= v
ForkWrites(f) == {w \in sealedData : Half(w[1]) = f} \cup own[f]

\* Time passes: the reader's map becomes older than the bound.
ReaderTick ==
  /\ ~rold /\ rold' = TRUE
  /\ UNCHANGED <<splitVars, sData, own, clientVars, rmap, staleRead, feedVars>>

\* A Flushed get through the router: an old map is refreshed first (D89).
ReadFlushed(k) ==
  /\ LET refresh == rold /\ Broken # "noFlushedRefresh"
         map == IF refresh THEN dirVersion ELSE rmap
         read == IF map = 1 THEN Newest(sData, k) ELSE Newest(ForkWrites(Half(k)), k)
         newest == Newest(own[Half(k)], k)
     IN /\ rmap' = map
        /\ rold' = (rold /\ ~refresh)
        /\ staleRead' = (staleRead \/ (rold /\ read < newest))
  /\ UNCHANGED <<splitVars, sData, own, clientVars, feedVars>>

(* ----- The change-feed consumer (D80) ----- *)
\* A page of the shard's feed: everything it acknowledged so far.
ConsumeSource ==
  /\ cons = "source"
  /\ seen' = seen \cup sData /\ drained' = sSealed
  /\ UNCHANGED <<splitVars, sData, own, clientVars, readerVars, cons>>

\* Leave the source once a read after the seal ended it (an empty page), for each successor from
\* the source's last sequence number. feedGap: as soon as the shard is sealed.
SwitchToSuccessors ==
  /\ cons = "source" /\ sSealed /\ (drained \/ Broken = "feedGap")
  /\ cons' = "successors"
  /\ UNCHANGED <<splitVars, sData, own, clientVars, readerVars, drained, seen>>

ConsumeSuccessors ==
  /\ cons = "successors"
  /\ seen' = seen \cup own["L"] \cup own["R"]
  /\ UNCHANGED <<splitVars, sData, own, clientVars, readerVars, cons, drained>>

Next ==
  \/ \E k \in Keys : ClientStart(k) \/ ReadFlushed(k)
  \/ ClientWrite \/ ClientRefresh \/ ClientResume
  \/ Begin \/ BeginRead \/ BeginWrite \/ Begin2 \/ Begin2Read \/ Begin2Write
  \/ Seal \/ Publish \/ Crash \/ ResealOthers \/ \E f \in Forks : Fork(f) \/ Release(f)
  \/ ReaderTick
  \/ ConsumeSource \/ SwitchToSuccessors \/ ConsumeSuccessors

\* The split's steps run while its process lives; a waiting request is eventually answered, and
\* waits out the routing wait.
Fairness ==
  /\ WF_vars(BeginWrite) /\ WF_vars(Seal) /\ WF_vars(Publish) /\ \A f \in Forks : WF_vars(Fork(f))
  /\ WF_vars(ClientWrite) /\ WF_vars(ClientRefresh) /\ WF_vars(ClientResume)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ dirVersion \in {1, 2} /\ cpc \in {"idle", "writing", "waiting"}
  /\ holds \subseteq (Forks \cup {"L2"})
  /\ released \subseteq Forks /\ fdone \subseteq forked

\* Once published, every write the shard acknowledged is in the successor that owns its key.
EveryShardWriteInItsSuccessor ==
  dirVersion = 2 => \A w \in sData : w \in ForkWrites(Half(w[1]))

\* Every hold on the shard belongs to a successor its seal names (D95).
HoldsMatchSeal == holds \subseteq Forks

\* Every half that exists keeps its hold on the shard (D136) until its own release after its rewrite
\* (D77), never ended by a repeated split step.
ForkKeepsHold == \A f \in forked \ released : f \in holds

\* No hold outlives its half's release (D138): a half that released its hold never holds the shard
\* again, so nothing keeps a hold its holder never reads, and the shard can be removed.
NoOrphanHold == released \cap holds = {}

\* A consumer that moved to the successors was given every write the shard acknowledged (D80).
ConsumerMissesNothing == cons = "successors" => sData \subseteq seen

\* A Flushed read through a map older than the bound never returns a value older than its
\* successor's flushed one (D89).
NoStaleBeyondBound == ~staleRead

\* The published map names the successors the shard's seal named, which the forks created (D116).
PublishedIsSealed == ~misrouted

\* A write that waits on the split eventually lands (D101).
WaitingWritesLand == (cpc = "waiting") ~> (cpc = "idle")
=============================================================================
