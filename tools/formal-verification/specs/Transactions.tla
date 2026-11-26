----------------------------- MODULE Transactions -----------------------------
(***************************************************************************)
(* SerializableSnapshot transactions in one shaledb database admit only *)
(* serializable histories: every committed transaction read exactly the    *)
(* values the database held when it committed, so committing in commit     *)
(* order (one at a time, under the writer's commit lock) is a serial order. *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/), D58:                   *)
(*  - mem-table.ts: rows above retained_from_seq stay in SQLite; a flush   *)
(*    only advances flushed_seq (markFlushed); a separate trim deletes     *)
(*    flushed rows and raises retained_from_seq (any amount here, a        *)
(*    superset of the retention policy).                                   *)
(*  - db-client.ts beginTransaction: readSeq = last_seq, and the manifest  *)
(*    that made flushed_seq durable (its lastL0Seq = flushed_seq), read    *)
(*    together. getAt/scanAt: the writer's rows above retained_from_seq    *)
(*    at or below readSeq (probeAt) merged with that pinned manifest; the  *)
(*    reader refuses when the manifest's lastL0Seq < retained_from_seq     *)
(*    (SnapshotTooOld).                                                    *)
(*  - db-writer.ts commitTransaction: under the commit lock, abort if      *)
(*    retained_from_seq > readSeq (SnapshotTooOld); abort if a written     *)
(*    key, or for SerializableSnapshot a read key, has a row newer than    *)
(*    readSeq (MemTable.writtenAfter, retained or unflushed); else commit  *)
(*    the batch (one seq).                                                 *)
(*  - Compaction rewrites only the newest manifest; a pinned manifest is   *)
(*    never rewritten, and any compaction of a manifest keeps each key's   *)
(*    newest version at or below its lastL0Seq, which is all a read takes  *)
(*    from it (its lastL0Seq is at most readSeq). A pinned manifest is     *)
(*    therefore its lastL0Seq here, read as the newest version at or below *)
(*    it, and compaction changes nothing a transaction can observe.        *)
(*                                                                         *)
(* A transaction writes its own name as the value of each key it writes;  *)
(* direct writes (plain puts) are one-write transactions.                  *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "snapshotOnly"       commits check written keys only (Snapshot)       *)
(*   "trimWithoutAbort"   commits skip the retained_from_seq check, so a   *)
(*                        trim past an open transaction hides its writes   *)
(*   "noReadHorizonCheck" reads skip the pinned manifest's horizon check,  *)
(*                        so rows trimmed below it are silently missed     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Keys, Txns, MaxSeq, Broken

ASSUME Broken \in {"none", "snapshotOnly", "trimWithoutAbort", "noReadHorizonCheck"}

Init0 == "init"
Direct == "direct"
InitVersion == [seq |-> 0, value |-> Init0]

VARIABLES
  seq,        \* the writer's last sequence number
  flushed,    \* flushed_seq: rows at or below it are in the newest manifest
  retained,   \* retained_from_seq: rows at or below it are gone from SQLite
  versions,   \* each key's committed versions: a set of [seq, value]
  state,      \* each transaction: "idle" | "active" | "committed" | "aborted"
  readSeq,    \* each transaction's read sequence number
  pinned,     \* each transaction's begin manifest, by its lastL0Seq
  reads,      \* each transaction's reads: a set of <<key, value>>
  writes,     \* each transaction's written keys
  okAtCommit  \* for each committed transaction: its reads equal the values at its commit

vars == <<seq, flushed, retained, versions, state, readSeq, pinned, reads, writes, okAtCommit>>

Init ==
  /\ seq = 0 /\ flushed = 0 /\ retained = 0
  /\ versions = [k \in Keys |-> {InitVersion}]
  /\ state = [t \in Txns |-> "idle"]
  /\ readSeq = [t \in Txns |-> 0]
  /\ pinned = [t \in Txns |-> 0]
  /\ reads = [t \in Txns |-> {}]
  /\ writes = [t \in Txns |-> {}]
  /\ okAtCommit = [t \in Txns |-> TRUE]

\* The newest of a set of versions (each set read here holds the seq-0 version or is non-empty).
Newest(vs) == CHOOSE v \in vs : \A w \in vs : w.seq <= v.seq
\* The value of k the database held at sequence number s.
ValueAt(k, s) == Newest({v \in versions[k] : v.seq <= s}).value
Current(k) == ValueAt(k, seq)

\* beginTransaction: readSeq and the manifest that made flushed_seq durable, read together.
Begin(t) ==
  /\ state[t] = "idle"
  /\ state' = [state EXCEPT ![t] = "active"]
  /\ readSeq' = [readSeq EXCEPT ![t] = seq]
  /\ pinned' = [pinned EXCEPT ![t] = flushed]
  /\ UNCHANGED <<seq, flushed, retained, versions, reads, writes, okAtCommit>>

\* getAt: the writer's rows in (retained, readSeq] (probeAt), else the pinned manifest's newest at or
\* below readSeq; refused when the manifest no longer covers what SQLite dropped.
Read(t, k) ==
  /\ state[t] = "active" /\ k \notin writes[t]
  /\ IF retained > pinned[t] /\ Broken # "noReadHorizonCheck"
       THEN /\ state' = [state EXCEPT ![t] = "aborted"] /\ UNCHANGED reads
       ELSE LET kept == {v \in versions[k] : v.seq > retained /\ v.seq <= readSeq[t]}
                value == IF kept # {} THEN Newest(kept).value ELSE ValueAt(k, pinned[t])
            IN /\ reads' = [reads EXCEPT ![t] = @ \cup {<<k, value>>}]
               /\ UNCHANGED state
  /\ UNCHANGED <<seq, flushed, retained, versions, readSeq, pinned, writes, okAtCommit>>

Write(t, k) ==
  /\ state[t] = "active"
  /\ writes' = [writes EXCEPT ![t] = @ \cup {k}]
  /\ UNCHANGED <<seq, flushed, retained, versions, state, readSeq, pinned, reads,
                 okAtCommit>>

ReadKeys(t) == {r[1] : r \in reads[t]}

\* MemTable.writtenAfter: a key has a row still in SQLite (above retained) newer than readSeq.
InSqliteAfter(k, s) == \E v \in versions[k] : v.seq > s /\ v.seq > retained

\* DbWriter.commitTransaction, atomic under the commit lock.
Commit(t) ==
  /\ state[t] = "active" /\ writes[t] # {} /\ seq < MaxSeq
  /\ LET tooOld == Broken # "trimWithoutAbort" /\ retained > readSeq[t]
         checked == IF Broken = "snapshotOnly" THEN writes[t] ELSE writes[t] \cup ReadKeys(t)
         conflict == \E k \in checked : InSqliteAfter(k, readSeq[t])
     IN IF tooOld \/ conflict
          THEN /\ state' = [state EXCEPT ![t] = "aborted"]
               /\ UNCHANGED <<seq, versions, okAtCommit>>
          ELSE /\ okAtCommit' = [okAtCommit EXCEPT ![t] = \A r \in reads[t] : Current(r[1]) = r[2]]
               /\ seq' = seq + 1
               /\ versions' = [k \in Keys |->
                    IF k \in writes[t] THEN versions[k] \cup {[seq |-> seq + 1, value |-> t]}
                    ELSE versions[k]]
               /\ state' = [state EXCEPT ![t] = "committed"]
  /\ UNCHANGED <<flushed, retained, readSeq, pinned, reads, writes>>

\* A plain put: a one-write transaction with no reads.
DirectWrite(k) ==
  /\ seq < MaxSeq
  /\ seq' = seq + 1
  /\ versions' = [versions EXCEPT ![k] = @ \cup {[seq |-> seq + 1, value |-> Direct]}]
  /\ UNCHANGED <<flushed, retained, state, readSeq, pinned, reads, writes, okAtCommit>>

\* A flush: every committed row into the newest manifest; flushed_seq advances, nothing is deleted.
Flush ==
  /\ flushed < seq
  /\ flushed' = seq
  /\ UNCHANGED <<seq, retained, versions, state, readSeq, pinned, reads, writes, okAtCommit>>

\* A trim: delete flushed rows up to some cut and raise retained_from_seq to it.
Trim ==
  /\ \E cut \in (retained + 1) .. flushed : retained' = cut
  /\ UNCHANGED <<seq, flushed, versions, state, readSeq, pinned, reads, writes,
                 okAtCommit>>

Next ==
  \/ \E t \in Txns : Begin(t) \/ Commit(t)
  \/ \E t \in Txns, k \in Keys : Read(t, k) \/ Write(t, k)
  \/ \E k \in Keys : DirectWrite(k)
  \/ Flush \/ Trim

Spec == Init /\ [][Next]_vars

\* Keys and transactions are interchangeable: TLC checks one ordering of each.
Symmetry == Permutations(Keys) \cup Permutations(Txns)

TypeOK ==
  /\ seq \in 0..MaxSeq /\ flushed \in 0..MaxSeq /\ retained \in 0..MaxSeq
  /\ retained <= flushed /\ flushed <= seq
  /\ \A t \in Txns : state[t] \in {"idle", "active", "committed", "aborted"}

\* Serializable in commit order: every committed transaction read what the database held at its commit.
Serializable == \A t \in Txns : state[t] = "committed" => okAtCommit[t]
=============================================================================
