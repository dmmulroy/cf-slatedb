---------------------------- MODULE NoAckedWriteLost ----------------------------
(***************************************************************************)
(* No acknowledged write is lost or hidden, for one shaledb database.   *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - db-writer.ts write: commit rows + last_seq in one SQLite transaction *)
(*    (MemTable.commit), then acknowledge.                                 *)
(*  - db-writer.ts flushNow: cut = last_seq; upload, of the rows in       *)
(*    (flushed_seq, cut], the versions a read can reach                    *)
(*    (keepReachableVersions; for one key: the newest) as an L0 SST;       *)
(*    ManifestStore.updateFrom                                             *)
(*    (commitL0Flush) sets lastL0Seq = cut; only then MemTable.markFlushed *)
(*    sets flushed_seq = cut (rows stay), and a separate MemTable.trim     *)
(*    deletes flushed rows up to a horizon at or below flushed_seq (D58).  *)
(*  - db-writer.ts ensureReady (restart after eviction or crash): seed     *)
(*    last_seq from the manifest and record lastL0Seq as flushed.          *)
(*  - db-writer.ts probe: read flushed_seq and the key's versions above it *)
(*    in one SQLite transaction (MemTable.consistently).                   *)
(*  - compaction-merge.ts compactRowStreams, committed by                  *)
(*    CompactionExecutor.commit: a new manifest whose SSTs hold only the   *)
(*    newest version of each key.                                          *)
(*  - The Latest read (db-client.ts pointView, db-reader.ts manifestFor):  *)
(*    the probe gives the snapshot (last_seq), flushed_seq, and the rows   *)
(*    above flushed_seq; the read uses any manifest M with                 *)
(*    flushed_seq <= M.lastL0Seq <= snapshot, and returns the newest       *)
(*    version at or below the snapshot from the probe's rows and M's SSTs. *)
(*    A manifest flushed past the snapshot may have dropped the version    *)
(*    the snapshot needs, so the read asks the writer again instead.       *)
(*                                                                         *)
(* Rows are modeled by their sequence numbers, all versions of one key:    *)
(* a later write supersedes an earlier one, so a flush and a compaction    *)
(* keep the newest version, and a read must return the newest version      *)
(* acknowledged before it started.                                         *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"                  the code as written                           *)
(*   "truncateBeforeCommit"  advance flushed_seq and trim before the       *)
(*                           manifest commit                               *)
(*   "nonAtomicProbe"        read flushed_seq and rows in separate steps   *)
(*   "trimUnflushed"         a trim deletes rows above flushed_seq         *)
(*   "noSnapshotUpperBound"  the read accepts a manifest flushed past its  *)
(*                           snapshot                                      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS MaxWrites, MaxCrashes, MaxReads, Broken

ASSUME Broken \in {"none", "truncateBeforeCommit", "nonAtomicProbe", "trimUnflushed",
                   "noSnapshotUpperBound"}

VARIABLES
  lastSeq,      \* writer_meta.last_seq (durable)
  flushedSeq,   \* writer_meta.flushed_seq (durable)
  mem,          \* memtable_rows: sequence numbers durable in SQLite (retained or unflushed)
  manifests,    \* the manifest chain: each [lastL0Seq, data] (data = versions in its SSTs)
  acked,        \* acknowledged writes
  wpc, cut, sst,\* writer: "ready" | "uploaded" | "committed" | "down"; the flush in progress
  crashes,
  rpc, rAcked, rFs, rSnap, rMem, rResult, reads  \* one Latest reader

vars == <<lastSeq, flushedSeq, mem, manifests, acked, wpc, cut, sst, crashes,
          rpc, rAcked, rFs, rSnap, rMem, rResult, reads>>

Newest == manifests[Len(manifests)]
Max(a, b) == IF a >= b THEN a ELSE b
\* The newest version in a set of versions of the key, as a set (empty when there is none).
NewestOf(versions) == {s \in versions : \A t \in versions : t <= s}

Init ==
  /\ lastSeq = 0 /\ flushedSeq = 0 /\ mem = {}
  /\ manifests = << [lastL0Seq |-> 0, data |-> {}] >>
  /\ acked = {}
  /\ wpc = "ready" /\ cut = 0 /\ sst = {}
  /\ crashes = 0
  /\ rpc = "idle" /\ rAcked = {} /\ rFs = 0 /\ rSnap = 0 /\ rMem = {} /\ rResult = {} /\ reads = 0

writerVars == <<lastSeq, flushedSeq, mem, manifests, acked, wpc, cut, sst, crashes>>
readerVars == <<rpc, rAcked, rFs, rSnap, rMem, rResult, reads>>

\* DbWriter.write: one SQLite transaction inserts the row and advances last_seq; then the ack.
Write ==
  /\ wpc # "down"
  /\ lastSeq < MaxWrites
  /\ lastSeq' = lastSeq + 1
  /\ mem' = mem \cup {lastSeq + 1}
  /\ acked' = acked \cup {lastSeq + 1}
  /\ UNCHANGED <<flushedSeq, manifests, wpc, cut, sst, crashes>> /\ UNCHANGED readerVars

\* flushNow: cut, and upload of the newest version among the rows in (flushed_seq, cut] (an SST
\* with a fresh ULID): the older ones are superseded for every read of this SST.
FlushUpload ==
  /\ wpc = "ready"
  /\ lastSeq > flushedSeq
  /\ cut' = lastSeq
  /\ sst' = NewestOf({s \in mem : flushedSeq < s /\ s <= lastSeq})
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

\* A compaction commits: a new manifest whose SSTs hold only the newest version. Older manifests
\* stay readable (a reader may hold one).
Compact ==
  /\ Len(manifests) < MaxWrites + 3
  /\ Newest.data # NewestOf(Newest.data)
  /\ manifests' = Append(manifests, [lastL0Seq |-> Newest.lastL0Seq, data |-> NewestOf(Newest.data)])
  /\ UNCHANGED <<lastSeq, flushedSeq, mem, acked, wpc, cut, sst, crashes>> /\ UNCHANGED readerVars

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

\* A Latest read starts: the newest write acknowledged before now must be what it returns.
\* DbWriter.probe reads last_seq (the read's snapshot), flushed_seq, and the rows above
\* flushed_seq in one SQLite transaction.
Probe ==
  /\ rpc = "idle" /\ reads < MaxReads /\ wpc # "down"
  /\ rAcked' = acked
  /\ rSnap' = lastSeq
  /\ IF Broken = "nonAtomicProbe"
     THEN /\ rFs' = flushedSeq /\ rpc' = "gotFlushedSeq" /\ UNCHANGED rMem
     ELSE /\ rFs' = flushedSeq /\ rMem' = {s \in mem : s > flushedSeq} /\ rpc' = "probed"
  /\ UNCHANGED <<rResult, reads>> /\ UNCHANGED writerVars

ProbeRows ==
  /\ rpc = "gotFlushedSeq" /\ wpc # "down"
  /\ rMem' = {s \in mem : s > flushedSeq} /\ rpc' = "probed"
  /\ UNCHANGED <<rAcked, rFs, rSnap, rResult, reads>> /\ UNCHANGED writerVars

\* The reader uses any manifest that covers flushed_seq and was not flushed past the read's
\* snapshot (possibly a stale cached one), and returns the newest version at or below the
\* snapshot from the probe's rows and that manifest's SSTs.
ReadManifest ==
  /\ rpc = "probed"
  /\ \E i \in 1..Len(manifests) :
       /\ manifests[i].lastL0Seq >= rFs
       /\ (Broken = "noSnapshotUpperBound" \/ manifests[i].lastL0Seq <= rSnap)
       /\ rResult' = NewestOf({s \in rMem \cup manifests[i].data : s <= rSnap})
  /\ rpc' = "done"
  /\ UNCHANGED <<rAcked, rFs, rSnap, rMem, reads>> /\ UNCHANGED writerVars

\* The only manifest the reader could load was flushed past its snapshot: it asks the writer
\* again (a new probe, a new snapshot) instead of reading it.
ReadAgain ==
  /\ rpc = "probed"
  /\ rpc' = "idle"
  /\ UNCHANGED <<rAcked, rFs, rSnap, rMem, rResult, reads>> /\ UNCHANGED writerVars

ReaderReset ==
  /\ rpc = "done"
  /\ rpc' = "idle" /\ reads' = reads + 1
  /\ UNCHANGED <<rAcked, rFs, rSnap, rMem, rResult>> /\ UNCHANGED writerVars

Next ==
  \/ Write \/ FlushUpload \/ FlushCommit \/ Compact \/ FlushTruncate \/ Trim \/ Crash \/ Restart
  \/ Probe \/ ProbeRows \/ ReadManifest \/ ReadAgain \/ ReaderReset

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* The newest acknowledged write (the key's current version; flushes and compactions drop the
\* ones it superseded) is durable in the memtable or in the newest manifest's SSTs.
NoAckedWriteLost == NewestOf(acked) \subseteq mem \cup Newest.data

\* A Latest read returns the newest write acknowledged before it started.
LatestSeesAcked == rpc = "done" => rResult = NewestOf(rAcked)

\* The memtable never holds a row above last_seq (the sequence oracle never reuses a number).
SeqOracleMonotonic == \A s \in mem : s <= lastSeq

TypeOK == wpc \in {"ready", "uploaded", "committed", "down"}
=============================================================================
