----------------------------- MODULE Transactions -----------------------------
(***************************************************************************)
(* SerializableSnapshot transactions in one cf-slatedb database admit only *)
(* serializable histories: every committed transaction read exactly the    *)
(* values the database held when it committed, so committing in commit     *)
(* order (one at a time, under the writer's commit lock) is a serial order. *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - db-client.ts beginTransaction (readSeq = the writer's last_seq),      *)
(*    getAt (a read at readSeq; a flush past readSeq aborts it:             *)
(*    SnapshotTooOld), transaction (buffers writes, records read keys).    *)
(*  - db-writer.ts commitTransaction: under the commit lock, abort if       *)
(*    flushed_seq > readSeq (SnapshotTooOld); abort if a written key, or    *)
(*    for SerializableSnapshot a read key, has a memtable row newer than    *)
(*    readSeq (MemTable.writtenAfter); else commit the batch (one seq).     *)
(*  - The memtable holds exactly the rows newer than flushed_seq.          *)
(*                                                                         *)
(* A transaction writes its own name as the value of each key it writes;  *)
(* direct writes (plain puts) are one-write transactions.                  *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"          the code as written                                   *)
(*   "snapshotOnly"  commits check written keys only (Snapshot isolation)  *)
(*   "noFlushCheck"  commits skip the flushed_seq check, so writes already *)
(*                   flushed out of the memtable go unseen                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Keys, Txns, MaxSeq, Broken

ASSUME Broken \in {"none", "snapshotOnly", "noFlushCheck"}

Init0 == "init"
Direct == "direct"

VARIABLES
  seq,       \* the writer's last sequence number
  flushed,   \* flushed_seq: rows at or below it left the memtable
  versions,  \* each key's committed versions: a set of [seq, value]
  state,     \* each transaction: "idle" | "active" | "committed" | "aborted"
  readSeq,   \* each transaction's read sequence number
  reads,     \* each transaction's reads: a set of <<key, value>>
  writes,    \* each transaction's written keys
  okAtCommit \* for each committed transaction: its reads equal the values at its commit

vars == <<seq, flushed, versions, state, readSeq, reads, writes, okAtCommit>>

Init ==
  /\ seq = 0 /\ flushed = 0
  /\ versions = [k \in Keys |-> {[seq |-> 0, value |-> Init0]}]
  /\ state = [t \in Txns |-> "idle"]
  /\ readSeq = [t \in Txns |-> 0]
  /\ reads = [t \in Txns |-> {}]
  /\ writes = [t \in Txns |-> {}]
  /\ okAtCommit = [t \in Txns |-> TRUE]

\* The newest version of k at or below s (versions at seq 0 always exist).
ValueAt(k, s) ==
  LET newest == CHOOSE v \in versions[k] :
                  v.seq <= s /\ \A w \in versions[k] : w.seq <= s => w.seq <= v.seq
  IN newest.value
Current(k) == ValueAt(k, seq)

Begin(t) ==
  /\ state[t] = "idle"
  /\ state' = [state EXCEPT ![t] = "active"] /\ readSeq' = [readSeq EXCEPT ![t] = seq]
  /\ UNCHANGED <<seq, flushed, versions, reads, writes, okAtCommit>>

\* getAt: a flush past readSeq aborts the read (the snapshot can no longer be read safely).
Read(t, k) ==
  /\ state[t] = "active" /\ k \notin writes[t]
  /\ IF flushed > readSeq[t]
       THEN /\ state' = [state EXCEPT ![t] = "aborted"] /\ UNCHANGED reads
       ELSE /\ reads' = [reads EXCEPT ![t] = @ \cup {<<k, ValueAt(k, readSeq[t])>>}]
            /\ UNCHANGED state
  /\ UNCHANGED <<seq, flushed, versions, readSeq, writes, okAtCommit>>

Write(t, k) ==
  /\ state[t] = "active"
  /\ writes' = [writes EXCEPT ![t] = @ \cup {k}]
  /\ UNCHANGED <<seq, flushed, versions, state, readSeq, reads, okAtCommit>>

ReadKeys(t) == {r[1] : r \in reads[t]}

\* MemTable.writtenAfter: a key has a memtable row (newer than flushed_seq) newer than readSeq.
InMemtableAfter(k, s) == \E v \in versions[k] : v.seq > s /\ v.seq > flushed

\* DbWriter.commitTransaction, atomic under the commit lock.
Commit(t) ==
  /\ state[t] = "active" /\ writes[t] # {} /\ seq < MaxSeq
  /\ LET tooOld == Broken # "noFlushCheck" /\ flushed > readSeq[t]
         checked == IF Broken = "snapshotOnly" THEN writes[t] ELSE writes[t] \cup ReadKeys(t)
         conflict == \E k \in checked : InMemtableAfter(k, readSeq[t])
     IN IF tooOld \/ conflict
          THEN /\ state' = [state EXCEPT ![t] = "aborted"]
               /\ UNCHANGED <<seq, versions, okAtCommit>>
          ELSE /\ okAtCommit' = [okAtCommit EXCEPT ![t] = \A r \in reads[t] : Current(r[1]) = r[2]]
               /\ seq' = seq + 1
               /\ versions' = [k \in Keys |->
                    IF k \in writes[t] THEN versions[k] \cup {[seq |-> seq + 1, value |-> t]}
                    ELSE versions[k]]
               /\ state' = [state EXCEPT ![t] = "committed"]
  /\ UNCHANGED <<flushed, readSeq, reads, writes>>

\* A plain put: a one-write transaction with no reads.
DirectWrite(k) ==
  /\ seq < MaxSeq
  /\ seq' = seq + 1
  /\ versions' = [versions EXCEPT ![k] = @ \cup {[seq |-> seq + 1, value |-> Direct]}]
  /\ UNCHANGED <<flushed, state, readSeq, reads, writes, okAtCommit>>

\* A flush: every committed row leaves the memtable.
Flush ==
  /\ flushed < seq
  /\ flushed' = seq
  /\ UNCHANGED <<seq, versions, state, readSeq, reads, writes, okAtCommit>>

Next ==
  \/ \E t \in Txns : Begin(t) \/ Commit(t)
  \/ \E t \in Txns, k \in Keys : Read(t, k) \/ Write(t, k)
  \/ \E k \in Keys : DirectWrite(k)
  \/ Flush

Spec == Init /\ [][Next]_vars

\* Keys and transactions are interchangeable: TLC checks one ordering of each.
Symmetry == Permutations(Keys) \cup Permutations(Txns)

TypeOK ==
  /\ seq \in 0..MaxSeq /\ flushed \in 0..MaxSeq
  /\ \A t \in Txns : state[t] \in {"idle", "active", "committed", "aborted"}

\* Serializable in commit order: every committed transaction read what the database held at its commit.
Serializable == \A t \in Txns : state[t] = "committed" => okAtCommit[t]
=============================================================================
