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
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxValues, MaxCrashes, Broken

ASSUME Broken \in {"none", "noResume", "noFlushedRefresh", "resealOthers", "feedGap"}

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
  seen        \* writes the consumer was given

vars == <<dirVersion, pending, spc, crashes, sSealed, holds, sData, sealedData, forked, own, cpc,
          ckey, cval, cmap, nextValue, rmap, rold, staleRead, cons, drained, seen>>
splitVars == <<dirVersion, pending, spc, crashes, sSealed, holds, sealedData, forked>>
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
  /\ cpc = "waiting" /\ pending /\ spc = "dead" /\ Broken # "noResume"
  /\ spc' = "recorded"
  /\ UNCHANGED <<dirVersion, pending, crashes, sSealed, holds, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars>>

(* ----- The split ----- *)
Begin ==
  /\ spc = "idle"
  /\ spc' = "recorded" /\ pending' = TRUE
  /\ UNCHANGED <<dirVersion, crashes, sSealed, holds, sealedData, forked, sData, own, clientVars,
                 readerVars, feedVars>>

\* seal(shard, [L, R], holds): flushes and seals in one commit, taking both holds; a repeat finds it
\* done.
Seal ==
  /\ spc = "recorded"
  /\ IF sSealed
       THEN UNCHANGED <<sSealed, holds, sealedData>>
       ELSE /\ sSealed' = TRUE /\ holds' = holds \cup Forks /\ sealedData' = sData
  /\ spc' = "sealed"
  /\ UNCHANGED <<dirVersion, pending, crashes, forked, sData, own, clientVars, readerVars, feedVars>>

\* forkRange from its hold: the sealed data in its range (a repeat finds the fork).
Fork(f) ==
  /\ spc = "sealed" /\ f \in holds /\ f \notin forked
  /\ forked' = forked \cup {f}
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, holds, sealedData, sData, own,
                 clientVars, readerVars, feedVars>>

Publish ==
  /\ spc = "sealed" /\ forked = Forks
  /\ dirVersion' = 2 /\ pending' = FALSE /\ spc' = "published"
  /\ UNCHANGED <<crashes, sSealed, holds, sealedData, forked, sData, own, clientVars, readerVars,
                 feedVars>>

\* The split's process dies after any step (its request ended, its Worker was evicted).
Crash ==
  /\ spc \in {"recorded", "sealed"} /\ crashes < MaxCrashes
  /\ spc' = "dead" /\ crashes' = crashes + 1
  /\ UNCHANGED <<dirVersion, pending, sSealed, holds, sealedData, forked, sData, own, clientVars,
                 readerVars, feedVars>>

\* Another caller seals the shard naming other successors (D95: refused).
ResealOthers ==
  /\ sSealed /\ Broken = "resealOthers" /\ "L2" \notin holds
  /\ holds' = holds \cup {"L2"}
  /\ UNCHANGED <<dirVersion, pending, spc, crashes, sSealed, sealedData, forked, sData, own,
                 clientVars, readerVars, feedVars>>

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
  \/ Begin \/ Seal \/ Publish \/ Crash \/ ResealOthers \/ \E f \in Forks : Fork(f)
  \/ ReaderTick
  \/ ConsumeSource \/ SwitchToSuccessors \/ ConsumeSuccessors

\* The split's steps run while its process lives; a waiting request is eventually answered, and
\* waits out the routing wait.
Fairness ==
  /\ WF_vars(Seal) /\ WF_vars(Publish) /\ \A f \in Forks : WF_vars(Fork(f))
  /\ WF_vars(ClientWrite) /\ WF_vars(ClientRefresh) /\ WF_vars(ClientResume)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ dirVersion \in {1, 2} /\ cpc \in {"idle", "writing", "waiting"}
  /\ holds \subseteq (Forks \cup {"L2"})

\* Once published, every write the shard acknowledged is in the successor that owns its key.
EveryShardWriteInItsSuccessor ==
  dirVersion = 2 => \A w \in sData : w \in ForkWrites(Half(w[1]))

\* Every hold on the shard belongs to a successor its seal names (D95).
HoldsMatchSeal == holds \subseteq Forks

\* A consumer that moved to the successors was given every write the shard acknowledged (D80).
ConsumerMissesNothing == cons = "successors" => sData \subseteq seen

\* A Flushed read through a map older than the bound never returns a value older than its
\* successor's flushed one (D89).
NoStaleBeyondBound == ~staleRead

\* A write that waits on the split eventually lands (D101).
WaitingWritesLand == (cpc = "waiting") ~> (cpc = "idle")
=============================================================================
