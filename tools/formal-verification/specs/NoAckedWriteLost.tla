---------------------------- MODULE NoAckedWriteLost ----------------------------
(***************************************************************************)
(* No acknowledged write is lost or hidden, for one shaledb database.   *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - db-writer.ts write: commit rows + last_seq in one SQLite transaction *)
(*    (MemTable.commit), then acknowledge.                                 *)
(*  - db-writer.ts flushNow: cut = last_seq; upload rows in               *)
(*    (flushed_seq, cut] as an L0 SST; ManifestStore.updateFrom            *)
(*    (commitL0Flush) sets lastL0Seq = cut; only then MemTable.markFlushed *)
(*    sets flushed_seq = cut (rows stay), and a separate MemTable.trim     *)
(*    deletes flushed rows up to a horizon at or below flushed_seq (D58).  *)
(*  - db-writer.ts ensureReady (restart after eviction or crash): seed     *)
(*    last_seq from the manifest and record lastL0Seq as flushed.          *)
(*  - db-writer.ts probe: read flushed_seq and the key's versions above it *)
(*    in one SQLite transaction (MemTable.consistently).                   *)
(*  - The Latest read (db-reader.ts, Phase 5): memtable rows from the      *)
(*    probe, plus SSTs of any manifest whose lastL0Seq >= flushed_seq (a   *)
(*    cached manifest is refreshed when it is older than that).            *)
(*                                                                         *)
(* Rows are modeled by their sequence numbers; one key is enough, since a  *)
(* write is hidden only if its row is in neither place a read looks.       *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"                  the code as written                           *)
(*   "truncateBeforeCommit"  advance flushed_seq and trim before the       *)
(*                           manifest commit                               *)
(*   "nonAtomicProbe"        read flushed_seq and rows in separate steps   *)
(*   "trimUnflushed"         a trim deletes rows above flushed_seq         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS MaxWrites, MaxCrashes, MaxReads, Broken

ASSUME Broken \in {"none", "truncateBeforeCommit", "nonAtomicProbe", "trimUnflushed"}

VARIABLES
  lastSeq,      \* writer_meta.last_seq (durable)
  flushedSeq,   \* writer_meta.flushed_seq (durable)
  mem,          \* memtable_rows: sequence numbers durable in SQLite (retained or unflushed)
  manifests,    \* the manifest chain: each [lastL0Seq, data] (data = seqs in its SSTs)
  acked,        \* acknowledged writes
  wpc, cut, sst,\* writer: "ready" | "uploaded" | "committed" | "down"; the flush in progress
  crashes,
  rpc, rAcked, rFs, rMem, rResult, reads  \* one Latest reader

vars == <<lastSeq, flushedSeq, mem, manifests, acked, wpc, cut, sst, crashes,
          rpc, rAcked, rFs, rMem, rResult, reads>>

Newest == manifests[Len(manifests)]
Max(a, b) == IF a >= b THEN a ELSE b

Init ==
  /\ lastSeq = 0 /\ flushedSeq = 0 /\ mem = {}
  /\ manifests = << [lastL0Seq |-> 0, data |-> {}] >>
  /\ acked = {}
  /\ wpc = "ready" /\ cut = 0 /\ sst = {}
  /\ crashes = 0
  /\ rpc = "idle" /\ rAcked = {} /\ rFs = 0 /\ rMem = {} /\ rResult = {} /\ reads = 0

writerVars == <<lastSeq, flushedSeq, mem, manifests, acked, wpc, cut, sst, crashes>>
readerVars == <<rpc, rAcked, rFs, rMem, rResult, reads>>

\* DbWriter.write: one SQLite transaction inserts the row and advances last_seq; then the ack.
Write ==
  /\ wpc # "down"
  /\ lastSeq < MaxWrites
  /\ lastSeq' = lastSeq + 1
  /\ mem' = mem \cup {lastSeq + 1}
  /\ acked' = acked \cup {lastSeq + 1}
  /\ UNCHANGED <<flushedSeq, manifests, wpc, cut, sst, crashes>> /\ UNCHANGED readerVars

\* flushNow: cut and upload of the rows <= cut (an SST with a fresh ULID).
FlushUpload ==
  /\ wpc = "ready"
  /\ lastSeq > flushedSeq
  /\ cut' = lastSeq
  /\ sst' = {s \in mem : s <= lastSeq}
  /\ IF Broken = "truncateBeforeCommit"
     THEN /\ mem' = {s \in mem : s > lastSeq}
          /\ flushedSeq' = Max(flushedSeq, lastSeq)
     ELSE UNCHANGED <<mem, flushedSeq>>
  /\ wpc' = "uploaded"
  /\ UNCHANGED <<lastSeq, manifests, acked, crashes>> /\ UNCHANGED readerVars

\* ManifestStore.update(commitL0Flush): the new manifest has the SST and lastL0Seq = cut.
FlushCommit ==
  /\ wpc = "uploaded"
  /\ manifests' = Append(manifests, [lastL0Seq |-> Max(Newest.lastL0Seq, cut), data |-> Newest.data \cup sst])
  /\ wpc' = "committed"
  /\ UNCHANGED <<lastSeq, flushedSeq, mem, acked, cut, sst, crashes>> /\ UNCHANGED readerVars

\* MemTable.markFlushed(cut): record flushed_seq; the rows stay (retained for transactions).
FlushTruncate ==
  /\ wpc = "committed"
  /\ IF Broken = "truncateBeforeCommit"
     THEN UNCHANGED flushedSeq
     ELSE flushedSeq' = Max(flushedSeq, cut)
  /\ wpc' = "ready"
  /\ UNCHANGED <<lastSeq, mem, manifests, acked, cut, sst, crashes>> /\ UNCHANGED readerVars

\* MemTable.trim: delete flushed rows up to some horizon at or below flushed_seq, any time.
Trim ==
  /\ wpc # "down"
  /\ \E horizon \in 0 .. (IF Broken = "trimUnflushed" THEN lastSeq ELSE flushedSeq) :
       /\ \E s \in mem : s <= horizon
       /\ mem' = {s \in mem : s > horizon}
  /\ UNCHANGED <<lastSeq, flushedSeq, manifests, acked, wpc, cut, sst, crashes>>
  /\ UNCHANGED readerVars

\* Eviction or crash at any point: volatile flush state is lost; SQLite and R2 survive.
Crash ==
  /\ wpc # "down"
  /\ crashes < MaxCrashes
  /\ wpc' = "down"
  /\ cut' = 0 /\ sst' = {}
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<lastSeq, flushedSeq, mem, manifests, acked>> /\ UNCHANGED readerVars

\* ensureReady after a restart: seed from the manifest; rows <= lastL0Seq are already in L0, so they
\* are recorded as flushed (and stay until a trim).
Restart ==
  /\ wpc = "down"
  /\ flushedSeq' = Max(flushedSeq, Newest.lastL0Seq)
  /\ lastSeq' = Max(lastSeq, Newest.lastL0Seq)
  /\ wpc' = "ready"
  /\ UNCHANGED <<mem, manifests, acked, cut, sst, crashes>> /\ UNCHANGED readerVars

\* A Latest read starts: every write acknowledged before now must be visible to it.
\* DbWriter.probe reads flushed_seq and the rows in one SQLite transaction.
Probe ==
  /\ rpc = "idle" /\ reads < MaxReads /\ wpc # "down"
  /\ rAcked' = acked
  /\ IF Broken = "nonAtomicProbe"
     THEN /\ rFs' = flushedSeq /\ rpc' = "gotFlushedSeq" /\ UNCHANGED rMem
     ELSE /\ rFs' = flushedSeq /\ rMem' = {s \in mem : s > flushedSeq} /\ rpc' = "probed"
  /\ UNCHANGED <<rResult, reads>> /\ UNCHANGED writerVars

ProbeRows ==
  /\ rpc = "gotFlushedSeq" /\ wpc # "down"
  /\ rMem' = {s \in mem : s > flushedSeq} /\ rpc' = "probed"
  /\ UNCHANGED <<rAcked, rFs, rResult, reads>> /\ UNCHANGED writerVars

\* The reader uses any manifest that covers flushed_seq: possibly stale, never older than that.
ReadManifest ==
  /\ rpc = "probed"
  /\ \E i \in 1..Len(manifests) :
       /\ manifests[i].lastL0Seq >= rFs
       /\ rResult' = rMem \cup manifests[i].data
  /\ rpc' = "done"
  /\ UNCHANGED <<rAcked, rFs, rMem, reads>> /\ UNCHANGED writerVars

ReaderReset ==
  /\ rpc = "done"
  /\ rpc' = "idle" /\ reads' = reads + 1
  /\ UNCHANGED <<rAcked, rFs, rMem, rResult>> /\ UNCHANGED writerVars

Next ==
  \/ Write \/ FlushUpload \/ FlushCommit \/ FlushTruncate \/ Trim \/ Crash \/ Restart
  \/ Probe \/ ProbeRows \/ ReadManifest \/ ReaderReset

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* Every acknowledged write is durable in the memtable or in the newest manifest's SSTs.
NoAckedWriteLost == acked \subseteq mem \cup Newest.data

\* A Latest read sees every write acknowledged before it started.
LatestSeesAcked == rpc = "done" => rAcked \subseteq rResult

\* The memtable never holds a row above last_seq (the sequence oracle never reuses a number).
SeqOracleMonotonic == \A s \in mem : s <= lastSeq

TypeOK == wpc \in {"ready", "uploaded", "committed", "down"}
=============================================================================
