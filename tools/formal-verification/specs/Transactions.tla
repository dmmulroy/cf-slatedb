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
(*    at or below readSeq (probeAt) merged with a manifest M such that     *)
(*    retained_from_seq <= M.lastL0Seq <= readSeq: the newest manifest    *)
(*    the reader holds when it is between those bounds, otherwise the      *)
(*    begin manifest when that is stored and between them (db-reader.ts   *)
(*    manifestFor, D76); with neither, the read is refused                 *)
(*    (SnapshotTooOld). "The newest the reader holds" is its cached one,   *)
(*    which may be any manifest committed so far: older than the begin     *)
(*    manifest, between it and the newest, or a flush's manifest committed *)
(*    before flushed_seq recorded it. GC may delete the begin manifest at  *)
(*    any time: a compaction replaces a flush's manifest while the writer  *)
(*    still names it.                                                      *)
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
(* direct writes (plain puts) are one-write transactions. A direct merge   *)
(* adds an operand of amount its seq (as U64Add would), so a value is its  *)
(* base and the sum of the operands above it: an operand counted twice or  *)
(* lost changes what a read returns (D135). A read takes the writer's rows *)
(* only above the manifest's lastL0Seq; each source's rows are tagged, so  *)
(* a row both hold is two rows, and the manifest's operands may be folded  *)
(* by a compaction into one row at the newest operand's seq.               *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "snapshotOnly"       commits check written keys only (Snapshot)       *)
(*   "trimWithoutAbort"   commits skip the retained_from_seq check, so a   *)
(*                        trim past an open transaction hides its writes   *)
(*   "noReadHorizonCheck" reads skip the manifest's lower bound, so rows   *)
(*                        trimmed below it are silently missed             *)
(*   "noReadUpperBound"   reads accept a manifest flushed past readSeq     *)
(*   "noPartition"        reads take every writer row beside the           *)
(*                        manifest's (getAt before D135)                   *)
(*   "dedupeBySeq"        reads take every writer row and drop the         *)
(*                        manifest's row of the same seq (scanAt before    *)
(*                        D135)                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Keys, Txns, MaxSeq, Broken, Live,
  Merges \* TRUE: direct merges commit operands (D135), at a smaller bound for states

ASSUME Broken \in {"none", "snapshotOnly", "trimWithoutAbort", "noReadHorizonCheck",
                   "noReadUpperBound", "beforeD76", "noPartition", "dedupeBySeq"}
ASSUME Live \in BOOLEAN
ASSUME Merges \in BOOLEAN

Init0 == "init"
Direct == "direct"
Operand == "operand"
InitVersion == [seq |-> 0, value |-> Init0, kind |-> "put"]

VARIABLES
  seq,        \* the writer's last sequence number
  flushed,    \* flushed_seq: rows at or below it are in the newest manifest
  retained,   \* retained_from_seq: rows at or below it are gone from SQLite
  versions,   \* each key's committed versions: a set of [seq, value, kind ("put" or "merge")]
  state,      \* each transaction: "idle" | "active" | "committed" | "aborted"
  readSeq,    \* each transaction's read sequence number
  pinned,     \* each transaction's begin manifest, by its lastL0Seq
  reads,      \* each transaction's reads: a set of <<key, value>>
  writes,     \* each transaction's written keys
  okAtCommit, \* for each committed transaction: its reads equal the values at its commit
  gone        \* Live: the flushes (by lastL0Seq) whose own manifest GC deleted

vars == <<seq, flushed, retained, versions, state, readSeq, pinned, reads, writes, okAtCommit,
          gone>>

Init ==
  /\ seq = 0 /\ flushed = 0 /\ retained = 0
  /\ versions = [k \in Keys |-> {InitVersion}]
  /\ state = [t \in Txns |-> "idle"]
  /\ readSeq = [t \in Txns |-> 0]
  /\ pinned = [t \in Txns |-> 0]
  /\ reads = [t \in Txns |-> {}]
  /\ writes = [t \in Txns |-> {}]
  /\ okAtCommit = [t \in Txns |-> TRUE]
  /\ gone = {}

\* The newest of a set of versions (each set read here holds the seq-0 version or is non-empty).
Newest(vs) == CHOOSE v \in vs : \A w \in vs : w.seq <= v.seq
\* A version as one source holds it: "w" the writer's SQLite, "m" the manifest. An operand adds its
\* seq; a compaction's fold adds the sum of the operands it folded.
RowOf(v, src) == [seq |-> v.seq, value |-> v.value, kind |-> v.kind, amt |-> v.seq, src |-> src]
RECURSIVE SumAmt(_)
SumAmt(R) == IF R = {} THEN 0 ELSE LET r == CHOOSE x \in R : TRUE IN r.amt + SumAmt(R \ {r})
\* resolveVisibleRow over rows R (they hold a put): the newest put's value and the sum of every
\* operand row above it, each row counted.
Resolve(R) ==
  LET base == Newest({r \in R : r.kind = "put"})
  IN <<base.value, SumAmt({r \in R : r.kind = "merge" /\ r.seq > base.seq})>>
\* The value of k the database held at sequence number s: each version once.
ValueAt(k, s) == Resolve({RowOf(v, "m") : v \in {x \in versions[k] : x.seq <= s}})
Current(k) == ValueAt(k, seq)
\* What a manifest with lastL0Seq m holds for k: its rows, or (compacted) the operands above the
\* newest put folded into one row at the newest operand's seq.
ManifestRows(k, m, folded) ==
  LET raw == {RowOf(v, "m") : v \in {x \in versions[k] : x.seq <= m}}
      ops == {r \in raw : r.kind = "merge" /\ r.seq > Newest({x \in raw : x.kind = "put"}).seq}
  IN IF ~folded \/ ops = {} THEN raw
     ELSE (raw \ ops) \cup {[seq |-> Newest(ops).seq, value |-> Operand, kind |-> "merge",
                               amt |-> SumAmt(ops), src |-> "m"]}
\* getAt/scanAt of k by t from the writer's rows in (retained, readSeq] and a manifest with
\* lastL0Seq m: the writer supplies only the rows above m (D135).
ReadValue(t, k, m, folded) ==
  LET kept == {v \in versions[k] : v.seq > retained /\ v.seq <= readSeq[t]}
      wr == {RowOf(v, "w") : v \in kept}
      mr == ManifestRows(k, m, folded)
  IN Resolve(CASE Broken = "noPartition" -> wr \cup mr
               [] Broken = "dedupeBySeq" -> wr \cup {r \in mr : r.seq \notin {v.seq : v \in kept}}
               [] OTHER -> {r \in wr : r.seq > m} \cup mr)

\* beginTransaction: readSeq and the manifest that made flushed_seq durable, read together.
Begin(t) ==
  /\ state[t] = "idle"
  /\ state' = [state EXCEPT ![t] = "active"]
  /\ readSeq' = [readSeq EXCEPT ![t] = seq]
  /\ pinned' = [pinned EXCEPT ![t] = flushed]
  /\ UNCHANGED <<seq, flushed, retained, versions, reads, writes, okAtCommit, gone>>

\* A manifest, by its lastL0Seq m, serves a read of t when it holds every row SQLite dropped and
\* was not flushed past the read's snapshot.
Fits(t, m) ==
  /\ (m >= retained \/ Broken = "noReadHorizonCheck")
  /\ (m <= readSeq[t] \/ Broken = "noReadUpperBound")

\* getAt: the writer's rows in (retained, readSeq] (probeAt), else the newest version at or below
\* the manifest's lastL0Seq. The reader uses the manifest it holds when that fits, and otherwise
\* the begin manifest when that fits. The one it holds is its cached manifest, which may be any
\* manifest committed so far: older than the begin manifest, between it and the newest, or a
\* flush's manifest committed before flushed_seq recorded it (so any lastL0Seq up to seq). The
\* read is refused as too old only when the begin manifest does not fit either (GC may also have
\* deleted it; refusing is always safe, so the model allows it whenever the begin manifest does
\* not fit).
Read(t, k) ==
  /\ ~Live
  /\ state[t] = "active" /\ k \notin writes[t]
  /\ \/ \E m \in 0 .. seq, folded \in BOOLEAN :
          /\ Fits(t, m)
          /\ reads' = [reads EXCEPT ![t] = @ \cup {<<k, ReadValue(t, k, m, folded)>>}]
          /\ UNCHANGED state
     \/ /\ ~Fits(t, pinned[t])
        /\ state' = [state EXCEPT ![t] = "aborted"]
        /\ UNCHANGED reads
  /\ UNCHANGED <<seq, flushed, retained, versions, readSeq, pinned, writes, okAtCommit, gone>>

Write(t, k) ==
  /\ state[t] = "active"
  /\ Live => (reads[t] # {} /\ writes[t] = {})
  /\ writes' = [writes EXCEPT ![t] = @ \cup {k}]
  /\ UNCHANGED <<seq, flushed, retained, versions, state, readSeq, pinned, reads,
                 okAtCommit, gone>>

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
               /\ UNCHANGED <<seq, versions, okAtCommit, gone>>
          ELSE /\ okAtCommit' = [okAtCommit EXCEPT ![t] = \A r \in reads[t] : Current(r[1]) = r[2]]
               /\ seq' = seq + 1
               /\ versions' = [k \in Keys |->
                    IF k \in writes[t] THEN versions[k] \cup {[seq |-> seq + 1, value |-> t, kind |-> "put"]}
                    ELSE versions[k]]
               /\ state' = [state EXCEPT ![t] = "committed"]
  /\ UNCHANGED <<flushed, retained, readSeq, pinned, reads, writes, gone>>

\* A plain put: a one-write transaction with no reads.
DirectWrite(k) ==
  /\ seq < MaxSeq
  \* Live: leave each transaction a sequence number to commit with
  /\ Live => seq + Cardinality({t \in Txns : state[t] # "committed"}) < MaxSeq
  /\ seq' = seq + 1
  /\ versions' = [versions EXCEPT ![k] = @ \cup {[seq |-> seq + 1, value |-> Direct, kind |-> "put"]}]
  /\ UNCHANGED <<flushed, retained, state, readSeq, pinned, reads, writes, okAtCommit, gone>>

\* A plain merge: one operand, committed like a one-write transaction with no reads.
DirectMerge(k) ==
  /\ Merges /\ seq < MaxSeq
  /\ Live => seq + Cardinality({t \in Txns : state[t] # "committed"}) < MaxSeq
  /\ seq' = seq + 1
  /\ versions' = [versions EXCEPT ![k] = @ \cup {[seq |-> seq + 1, value |-> Operand, kind |-> "merge"]}]
  /\ UNCHANGED <<flushed, retained, state, readSeq, pinned, reads, writes, okAtCommit, gone>>

\* A flush: every committed row into the newest manifest; flushed_seq advances, nothing is deleted.
Flush ==
  /\ flushed < seq
  /\ flushed' = seq
  /\ UNCHANGED <<seq, retained, versions, state, readSeq, pinned, reads, writes, okAtCommit, gone>>

\* A trim: delete flushed rows up to some cut and raise retained_from_seq to it.
Trim ==
  /\ \E cut \in (retained + 1) .. flushed : retained' = cut
  /\ UNCHANGED <<seq, flushed, versions, state, readSeq, pinned, reads, writes,
                 okAtCommit, gone>>

---------------------------------------------------------------------------
\* Live (D111): reads decide as DbReader.manifestFor does, GC deletes a flush's own manifest once
\* a compaction replaced it (the newest manifest is always stored), and an aborted transaction
\* is retried. Each transaction reads one key, writes one key, and commits.

\* manifestFor (D76): the newest manifest (its lastL0Seq is flushed) when it fits; otherwise the
\* begin manifest when it is stored and fits; otherwise SnapshotTooOld. Before D76 a read used
\* only the begin manifest, the writer's flushed manifest, which is the same deleted one on every
\* retry until the next flush.
LiveRead(t, k) ==
  /\ Live
  /\ state[t] = "active" /\ reads[t] = {}
  /\ LET stored(m) == m \notin gone
         newestFits == Broken # "beforeD76" /\ Fits(t, flushed)
         beginFits == stored(pinned[t]) /\ Fits(t, pinned[t])
     IN IF newestFits \/ beginFits
          THEN /\ \E folded \in BOOLEAN :
                    reads' = [reads EXCEPT ![t] =
                      {<<k, ReadValue(t, k, IF newestFits THEN flushed ELSE pinned[t], folded)>>}]
               /\ UNCHANGED state
          ELSE /\ state' = [state EXCEPT ![t] = "aborted"]
               /\ UNCHANGED reads
  /\ UNCHANGED <<seq, flushed, retained, versions, readSeq, pinned, writes, okAtCommit, gone>>

\* GC deletes a flush's own manifest: a compaction replaced it at the head (same lastL0Seq) and
\* gcMinAge passed. The writer still names it until its next flush.
Collect ==
  /\ Live
  /\ \E m \in 0 .. flushed : m \notin gone /\ gone' = gone \cup {m}
  /\ UNCHANGED <<seq, flushed, retained, versions, state, readSeq, pinned, reads, writes,
                 okAtCommit>>

\* The caller begins a new transaction and runs it again.
Retry(t) ==
  /\ Live /\ state[t] = "aborted"
  /\ state' = [state EXCEPT ![t] = "idle"]
  /\ reads' = [reads EXCEPT ![t] = {}] /\ writes' = [writes EXCEPT ![t] = {}]
  /\ UNCHANGED <<seq, flushed, retained, versions, readSeq, pinned, okAtCommit, gone>>

TxnStep(t) == Begin(t) \/ (\E k \in Keys : LiveRead(t, k) \/ Write(t, k)) \/ Commit(t) \/ Retry(t)

Next ==
  \/ \E t \in Txns : Begin(t) \/ Commit(t) \/ Retry(t)
  \/ \E t \in Txns, k \in Keys : LiveRead(t, k)
  \/ Collect
  \/ \E t \in Txns, k \in Keys : Read(t, k) \/ Write(t, k)
  \/ \E k \in Keys : DirectWrite(k) \/ DirectMerge(k)
  \/ Flush \/ Trim

Spec == Init /\ [][Next]_vars

\* Every transaction keeps running and retrying; writes, flushes, trims, and GC are never forced.
LiveSpec == Spec /\ \A t \in Txns : WF_vars(TxnStep(t))

\* Keys and transactions are interchangeable: TLC checks one ordering of each.
Symmetry == Permutations(Keys) \cup Permutations(Txns)

TypeOK ==
  /\ seq \in 0..MaxSeq /\ flushed \in 0..MaxSeq /\ retained \in 0..MaxSeq
  /\ retained <= flushed /\ flushed <= seq
  /\ \A t \in Txns : state[t] \in {"idle", "active", "committed", "aborted"}

\* Serializable in commit order: every committed transaction read what the database held at its commit.
Serializable == \A t \in Txns : state[t] = "committed" => okAtCommit[t]

\* Every read of a transaction, committed or not (a read-only transaction never commits), returned
\* the value the database held at the transaction's begin.
\* Liveness (Live): a transaction that is retried eventually commits. Writes are finite, so
\* conflicts with real writes end; SnapshotTooOld must end too (D76).
EveryTxnCommits == \A t \in Txns : <>(state[t] = "committed")

ReadsAtBegin ==
  \A t \in Txns : \A r \in reads[t] : r[2] = ValueAt(r[1], readSeq[t])
=============================================================================
