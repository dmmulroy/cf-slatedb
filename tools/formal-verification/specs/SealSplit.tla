------------------------------- MODULE SealSplit -------------------------------
(***************************************************************************)
(* Seal and split for one shaledb database: after the seal no write     *)
(* commits to the sealed database, and every write acknowledged before the *)
(* seal is visible in exactly one successor (each a range fork of a        *)
(* checkpoint of the sealed manifest).                                      *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - db-writer.ts write: under the commit lock, check the database is not *)
(*    sealed, commit the row to SQLite; the ack follows the lock release.  *)
(*  - db-writer.ts seal: under the commit lock, flushNow (every committed  *)
(*    row into L0, manifest CAS) and then the seal CAS (sealManifest,      *)
(*    writerEpoch + 1); later writes fail with DatabaseSealedError. A      *)
(*    split's seal (holdForSuccessors, D57) takes each successor's hold in *)
(*    the same CAS, pinning the sealed manifest: the checkpoint the forks  *)
(*    read.                                                                *)
(*  - database-lifecycle.ts forkRange from that hold (lsm-core             *)
(*    forkManifest: the pinned SSTs as views bounded to the fork's range); *)
(*    the two forks run concurrently, so they interleave in any order.     *)
(*  - The split order (lsm-sharding ShardSplit): seal with holds, then a   *)
(*    fork per successor range, then publish.                              *)
(*                                                                         *)
(* A write is (key, seq). The writer is the database's single Durable      *)
(* Object; fencing of a stale instance is checked by ManifestCasFencing.   *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"                  the code as written                           *)
(*   "sealWithoutLock"       seal flushes, releases the lock, then seals   *)
(*   "writerNotFenced"       writes still commit after the seal            *)
(*   "checkpointBeforeSeal"  the holds are taken before the seal           *)
(*   "overlappingRanges"     the successors' ranges overlap                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Keys, LeftKeys, MaxWrites, Broken

ASSUME Broken \in {"none", "sealWithoutLock", "writerNotFenced", "checkpointBeforeSeal",
                   "overlappingRanges"}
ASSUME LeftKeys \subseteq Keys

Successors == {"left", "right"}
Range(s) ==
  IF s = "left" THEN LeftKeys
  ELSE IF Broken = "overlappingRanges" THEN Keys ELSE Keys \ LeftKeys

VARIABLES
  seq,        \* the writer's last sequence number
  mem,        \* committed, unflushed writes (the SQLite memtable)
  flushed,    \* writes in the database's newest manifest (L0 and below)
  pending,    \* committed writes not yet acknowledged
  acked,      \* acknowledged writes
  sealed,     \* the newest manifest is sealed
  spc,        \* the seal in progress: "idle" | "flushed" (sealWithoutLock only) | "done"
  checkpoint, \* the writes the split's checkpoint pins
  taken,      \* whether the split's checkpoint exists
  forks,      \* each successor's writes
  forked,     \* the successors that exist
  afterSeal   \* writes committed to the database after it was sealed

vars == <<seq, mem, flushed, pending, acked, sealed, spc, checkpoint, taken, forks, forked, afterSeal>>

Writes == Keys \X (1..MaxWrites)

Init ==
  /\ seq = 0 /\ mem = {} /\ flushed = {} /\ pending = {} /\ acked = {}
  /\ sealed = FALSE /\ spc = "idle" /\ checkpoint = {} /\ taken = FALSE
  /\ forks = [s \in Successors |-> {}] /\ forked = {} /\ afterSeal = {}

\* DbWriter.write, under the commit lock: refused once sealed (unless the writer is not fenced).
Commit(k) ==
  /\ seq < MaxWrites
  /\ ~sealed \/ Broken = "writerNotFenced"
  /\ LET w == <<k, seq + 1>> IN
       /\ seq' = seq + 1
       /\ mem' = mem \cup {w}
       /\ pending' = pending \cup {w}
       /\ afterSeal' = IF sealed THEN afterSeal \cup {w} ELSE afterSeal
  /\ UNCHANGED <<flushed, acked, sealed, spc, checkpoint, taken, forks, forked>>

Ack(w) ==
  /\ w \in pending
  /\ pending' = pending \ {w} /\ acked' = acked \cup {w}
  /\ UNCHANGED <<seq, mem, flushed, sealed, spc, checkpoint, taken, forks, forked, afterSeal>>

\* An ordinary flush (alarm or request): every committed row into the manifest.
Flush ==
  /\ ~sealed /\ mem # {}
  /\ flushed' = flushed \cup mem /\ mem' = {}
  /\ UNCHANGED <<seq, pending, acked, sealed, spc, checkpoint, taken, forks, forked, afterSeal>>

\* DbWriter.seal: flushNow and the seal CAS under the commit lock, so no commit falls between. The
\* same CAS takes the successors' holds on the sealed manifest (unless they were taken before).
Seal ==
  /\ spc = "idle" /\ Broken # "sealWithoutLock"
  /\ flushed' = flushed \cup mem /\ mem' = {}
  /\ sealed' = TRUE /\ spc' = "done"
  /\ IF taken THEN UNCHANGED <<checkpoint, taken>>
               ELSE checkpoint' = flushed \cup mem /\ taken' = TRUE
  /\ UNCHANGED <<seq, pending, acked, forks, forked, afterSeal>>

SealFlushUnlocked ==
  /\ spc = "idle" /\ Broken = "sealWithoutLock"
  /\ flushed' = flushed \cup mem /\ mem' = {} /\ spc' = "flushed"
  /\ UNCHANGED <<seq, pending, acked, sealed, checkpoint, taken, forks, forked, afterSeal>>

SealCommitUnlocked ==
  /\ spc = "flushed"
  /\ sealed' = TRUE /\ spc' = "done"
  /\ IF taken THEN UNCHANGED <<checkpoint, taken>>
               ELSE checkpoint' = flushed /\ taken' = TRUE
  /\ UNCHANGED <<seq, mem, flushed, pending, acked, forks, forked, afterSeal>>

\* checkpointBeforeSeal only: the holds pin the newest manifest before the seal (the code takes them
\* in the seal's own CAS, above).
Checkpoint ==
  /\ ~taken
  /\ Broken = "checkpointBeforeSeal"
  /\ checkpoint' = flushed /\ taken' = TRUE
  /\ UNCHANGED <<seq, mem, flushed, pending, acked, sealed, spc, forks, forked, afterSeal>>

\* forkRange: the successor reads exactly the checkpoint's writes in its range.
Fork(s) ==
  /\ taken /\ s \notin forked
  /\ forks' = [forks EXCEPT ![s] = {w \in checkpoint : w[1] \in Range(s)}]
  /\ forked' = forked \cup {s}
  /\ UNCHANGED <<seq, mem, flushed, pending, acked, sealed, spc, checkpoint, taken, afterSeal>>

Next ==
  \/ \E k \in Keys : Commit(k)
  \/ \E w \in pending : Ack(w)
  \/ Flush \/ Seal \/ SealFlushUnlocked \/ SealCommitUnlocked
  \/ Checkpoint
  \/ \E s \in Successors : Fork(s)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
  /\ seq \in 0..MaxWrites
  /\ mem \subseteq Writes /\ flushed \subseteq Writes /\ acked \subseteq Writes
  /\ sealed \in BOOLEAN

SplitDone == forked = Successors

\* After the seal, no write commits to the sealed database.
NoCommitAfterSeal == afterSeal = {}

\* Once both successors exist, every acknowledged write is in exactly one of them.
EveryAckedWriteInExactlyOneSuccessor ==
  SplitDone =>
    \A w \in acked : Cardinality({s \in Successors : w \in forks[s]}) = 1
=============================================================================
