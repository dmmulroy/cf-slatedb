------------------------------ MODULE PagedScan ------------------------------
(***************************************************************************)
(* Paged scans of one shaledb database (D133): a Latest scan read in       *)
(* segments with resumeFrom (D93), pages that end on their work budget     *)
(* (D125), and a transaction scan read in several pages at one readSeq    *)
(* whose commit checks its read range (D125's phantom rule), with writes,  *)
(* flushes, trims, and GC of old manifests between any two steps.          *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/):                        *)
(*  - db-client.ts scanUnflushed / the Latest page: the first writer page  *)
(*    fixes the snapshot (last_seq); each segment merges the writer's rows *)
(*    above the page's flushed_seq with one manifest M,                    *)
(*    max(flushed_seq of every page so far) <= M.lastL0Seq <= snapshot     *)
(*    (D70). No such manifest (a flush overtook the scan): before any row  *)
(*    was returned the scan starts again at a new snapshot; after, it      *)
(*    fails with resumeFrom, the first key not returned (D93).             *)
(*  - The page budget (ScanPageBudget, D125): a page ends after examining  *)
(*    its budget, with resumeFrom the next key; the next page is a new     *)
(*    request (a Latest page at its own snapshot).                         *)
(*  - db-client.ts scanAtPage: every segment at readSeq: probeAt's rows    *)
(*    above retained_from_seq (read with it) and a manifest with           *)
(*    retained_from_seq <= M.lastL0Seq <= readSeq, else SnapshotTooOld;   *)
(*    the next page is another request at the same readSeq.                *)
(*  - db-writer.ts commitTransaction: refused when retained_from_seq >     *)
(*    readSeq, or a key in readRanges has a row newer than readSeq         *)
(*    (writtenAfter). The caller names the whole range it scanned.         *)
(*                                                                         *)
(* A segment here is one key; a page examines one key per unit of budget.  *)
(* Each write is one key: a put (its value is its seq), a delete, or a     *)
(* merge operand (adds its seq, as U64Add would), so a version counted     *)
(* twice or lost changes the value read (D135). Rows carry their source,   *)
(* the writer or the manifest, so a row both hold is two rows, as in the   *)
(* code. A flush commits and records flushed_seq at once (NoAckedWriteLost *)
(* and WriterObject check the steps between). Compaction is a choice at    *)
(* each read: the manifest's operands above the key's base may be folded   *)
(* into one row at the newest operand's seq (retainKeyVersions).           *)
(*                                                                         *)
(* Each read takes the writer's rows only above the manifest's lastL0Seq;  *)
(* the manifest holds the rest (collectKeyVersions, streamKeyVersions).    *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"               the code as written                              *)
(*   "restartAfterRows"   a Latest page overtaken after returning rows     *)
(*                        starts again at a new snapshot instead of ending *)
(*   "pageRangesOnly"     the transaction's readRanges name only the keys  *)
(*                        its pages returned                               *)
(*   "noPartition"        a read takes every writer row beside the         *)
(*                        manifest's (the point read before D135): an      *)
(*                        operand both hold is applied twice               *)
(*   "dedupeBySeq"        a read takes every writer row and drops the      *)
(*                        manifest's row of the same seq (the scan before  *)
(*                        D135): a fold's older operands are lost          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS NKeys, MaxSeq, MaxBudget, MaxGc, Scanner, Broken

ASSUME Broken \in {"none", "restartAfterRows", "pageRangesOnly", "noPartition", "dedupeBySeq"}
\* Which scanner runs: the Latest scan or the transaction scan (one per config, for states).
ASSUME Scanner \in {"latest", "txn"}

Keys == 1..NKeys
None == 0          \* "no value": a key absent or deleted
End == NKeys + 1   \* resumeFrom past the last key: the range is exhausted

VARIABLES
  lastSeq, flushedSeq, retained, keyOf, kindOf, stored, gcs,
  \* the Latest scanner: one page at a time
  lpc, lStart, lPos, lSnap, lMaxFs, lW, lRows, lBudget, lFixed,
  \* the transaction scanner
  tpc, tRead, tPos, tW, tRet, tRows, tBudget, tScanned, tResult, tPhantom

vars == <<lastSeq, flushedSeq, retained, keyOf, kindOf, stored, gcs,
          lpc, lStart, lPos, lSnap, lMaxFs, lW, lRows, lBudget, lFixed,
          tpc, tRead, tPos, tW, tRet, tRows, tBudget, tScanned, tResult, tPhantom>>
dbVars == <<lastSeq, flushedSeq, retained, keyOf, kindOf, stored, gcs>>
lVars == <<lpc, lStart, lPos, lSnap, lMaxFs, lW, lRows, lBudget, lFixed>>
tVars == <<tpc, tRead, tPos, tW, tRet, tRows, tBudget, tScanned, tResult, tPhantom>>

MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Max(a, b) == IF a >= b THEN a ELSE b
Kinds == {"put", "del", "merge"}
\* A version as one source holds it: its seq, kind, what it adds (a put's value, an operand's
\* amount, or a fold's sum), and its source ("w" the writer, "m" the manifest).
RowOf(v, src) == [seq |-> v, kind |-> kindOf[v], amt |-> IF kindOf[v] = "del" THEN 0 ELSE v,
                  src |-> src]
RECURSIVE SumAmt(_)
SumAmt(R) == IF R = {} THEN 0 ELSE LET r == CHOOSE x \in R : TRUE IN r.amt + SumAmt(R \ {r})
\* The newest put or delete among rows R (0: none).
BaseSeq(R) == LET bases == {r.seq : r \in {x \in R : x.kind # "merge"}}
              IN IF bases = {} THEN 0 ELSE MaxOf(bases)
\* resolveVisibleRow over rows R: the base's value plus every operand row above it, each row
\* counted (a row held twice counts twice); None when there is neither a live base nor an operand.
Resolve(R) ==
  LET b == BaseSeq(R)
      live == \E r \in R : r.seq = b /\ r.kind = "put"
      ops == {r \in R : r.kind = "merge" /\ r.seq > b}
  IN IF ~live /\ ops = {} THEN None ELSE (IF live THEN b ELSE 0) + SumAmt(ops)
\* The database's value of k at sequence number s: each version once.
ValueAt(k, s) == Resolve({RowOf(v, "m") : v \in {x \in 1..s : keyOf[x] = k}})
\* What a manifest with lastL0Seq m holds for k: its rows, or (compacted) the operands above the
\* base folded into one row at the newest operand's seq.
ManifestRows(k, m, folded) ==
  LET raw == {RowOf(v, "m") : v \in {x \in 1..m : keyOf[x] = k}}
      ops == {r \in raw : r.kind = "merge" /\ r.seq > BaseSeq(raw)}
  IN IF ~folded \/ ops = {} THEN raw
     ELSE (raw \ ops) \cup {[seq |-> MaxOf({r.seq : r \in ops}), kind |-> "merge",
                               amt |-> SumAmt(ops), src |-> "m"]}
\* What a read of k resolves from writer rows w and a manifest with lastL0Seq m.
Merged(k, w, m, folded) ==
  LET mr == ManifestRows(k, m, folded)
      wr == {RowOf(v, "w") : v \in w}
  IN Resolve(CASE Broken = "noPartition" -> wr \cup mr
               [] Broken = "dedupeBySeq" -> wr \cup {r \in mr : r.seq \notin w}
               [] OTHER -> {r \in wr : r.seq > m} \cup mr)
\* The live rows of keys lo..hi-1 at s.
Live(lo, hi, s) == {<<k, ValueAt(k, s)>> : k \in {x \in lo..(hi - 1) : ValueAt(x, s) # None}}

Init ==
  /\ lastSeq = 0 /\ flushedSeq = 0 /\ retained = 0
  /\ keyOf = [s \in 1..MaxSeq |-> 0] /\ kindOf = [s \in 1..MaxSeq |-> "del"]
  /\ stored = {0} /\ gcs = 0
  /\ lpc = "idle" /\ lStart = 1 /\ lPos = 1 /\ lSnap = 0 /\ lMaxFs = 0 /\ lW = {}
  /\ lRows = {} /\ lBudget = 0 /\ lFixed = FALSE
  /\ tpc = "begin" /\ tRead = 0 /\ tPos = 1 /\ tW = {} /\ tRet = 0 /\ tRows = {}
  /\ tBudget = 0 /\ tScanned = {} /\ tResult = "none" /\ tPhantom = FALSE

---------------------------------------------------------------------------
\* The database.

Write ==
  /\ lastSeq < MaxSeq
  /\ \E k \in Keys, kind \in Kinds :
       /\ keyOf' = [keyOf EXCEPT ![lastSeq + 1] = k]
       /\ kindOf' = [kindOf EXCEPT ![lastSeq + 1] = kind]
  /\ lastSeq' = lastSeq + 1
  /\ UNCHANGED <<flushedSeq, retained, stored, gcs>> /\ UNCHANGED lVars /\ UNCHANGED tVars

Flush ==
  /\ flushedSeq < lastSeq
  /\ flushedSeq' = lastSeq /\ stored' = stored \cup {lastSeq}
  /\ UNCHANGED <<lastSeq, retained, keyOf, kindOf, gcs>> /\ UNCHANGED lVars /\ UNCHANGED tVars

Trim ==
  /\ \E h \in (retained + 1)..flushedSeq : retained' = h
  /\ UNCHANGED <<lastSeq, flushedSeq, keyOf, kindOf, stored, gcs>>
  /\ UNCHANGED lVars /\ UNCHANGED tVars

\* GC deletes a manifest that is not the newest (superseded gcMinAge ago).
Collect ==
  /\ gcs < MaxGc
  /\ \E m \in stored \ {flushedSeq} : stored' = stored \ {m}
  /\ gcs' = gcs + 1
  /\ UNCHANGED <<lastSeq, flushedSeq, retained, keyOf, kindOf>>
  /\ UNCHANGED lVars /\ UNCHANGED tVars

---------------------------------------------------------------------------
\* The Latest scan: one page from lStart with a budget; segments of one key.

LPageStart ==
  /\ Scanner = "latest"
  /\ lpc = "idle" /\ lStart <= NKeys
  /\ \E b \in 1..MaxBudget : lBudget' = b
  /\ lpc' = "probe" /\ lPos' = lStart /\ lSnap' = 0 /\ lMaxFs' = 0 /\ lRows' = {} /\ lW' = {}
  /\ lFixed' = FALSE
  /\ UNCHANGED lStart /\ UNCHANGED dbVars /\ UNCHANGED tVars

\* DbWriter.scan: one writer page, atomic with flushed_seq; the first fixes the snapshot.
LProbe ==
  /\ lpc = "probe"
  /\ LET snap == IF lFixed THEN lSnap ELSE lastSeq
     IN /\ lSnap' = snap /\ lFixed' = TRUE
        /\ lW' = {v \in (flushedSeq + 1)..snap : keyOf[v] = lPos}
        /\ lMaxFs' = Max(lMaxFs, flushedSeq)
  /\ lpc' = "merge"
  /\ UNCHANGED <<lStart, lPos, lRows, lBudget>> /\ UNCHANGED dbVars /\ UNCHANGED tVars

\* The segment's merge with a manifest between D70's bounds; or a flush overtook the scan.
LMerge ==
  /\ lpc = "merge"
  /\ \/ \E m \in stored, folded \in BOOLEAN :
          /\ lMaxFs <= m /\ m <= lSnap
          /\ LET value == Merged(lPos, lW, m, folded)
             IN lRows' = IF value = None THEN lRows ELSE lRows \cup {<<lPos, value>>}
          /\ lPos' = lPos + 1 /\ lBudget' = lBudget - 1
          /\ lpc' = IF lPos + 1 = End \/ lBudget = 1 THEN "end" ELSE "probe"
          /\ UNCHANGED <<lSnap, lMaxFs, lStart, lFixed>>
     \/ /\ ~\E m \in stored : lMaxFs <= m /\ m <= lSnap
        /\ IF lRows = {} \/ Broken = "restartAfterRows"
           \* starts again from the range's start at a new snapshot (restart(): before any row
           \* was returned); the page's budget stays spent
           THEN /\ lSnap' = 0 /\ lMaxFs' = 0 /\ lpc' = "probe" /\ lFixed' = FALSE
                /\ lPos' = lStart
                /\ UNCHANGED <<lRows, lBudget, lStart>>
           \* ends the page with resumeFrom = the first key not returned
           ELSE /\ lpc' = "end"
                /\ UNCHANGED <<lSnap, lMaxFs, lPos, lRows, lBudget, lStart, lFixed>>
  /\ lW' = {}
  /\ UNCHANGED dbVars /\ UNCHANGED tVars

\* The page is answered; the caller asks for the next from resumeFrom (lPos).
LPageEnd ==
  /\ lpc = "end"
  /\ lpc' = "idle" /\ lStart' = lPos
  /\ UNCHANGED <<lPos, lSnap, lMaxFs, lW, lRows, lBudget, lFixed>> /\ UNCHANGED dbVars
  /\ UNCHANGED tVars

---------------------------------------------------------------------------
\* The transaction scan: pages at one readSeq, then the commit.

TBegin ==
  /\ Scanner = "txn"
  /\ tpc = "begin"
  /\ tRead' = lastSeq /\ tpc' = "pageStart"
  /\ UNCHANGED <<tPos, tW, tRet, tRows, tBudget, tScanned, tResult, tPhantom>>
  /\ UNCHANGED dbVars /\ UNCHANGED lVars

TPageStart ==
  /\ tpc = "pageStart"
  /\ \E b \in 1..MaxBudget : tBudget' = b
  /\ tpc' = IF tPos = End THEN "commit" ELSE "probe"
  /\ UNCHANGED <<tRead, tPos, tW, tRet, tRows, tScanned, tResult, tPhantom>>
  /\ UNCHANGED dbVars /\ UNCHANGED lVars

\* probeAt: the key's rows in (retained_from_seq, readSeq] with the horizon, atomically.
TProbe ==
  /\ tpc = "probe"
  /\ tW' = {v \in (retained + 1)..tRead : keyOf[v] = tPos} /\ tRet' = retained
  /\ tpc' = "merge"
  /\ UNCHANGED <<tRead, tPos, tRows, tBudget, tScanned, tResult, tPhantom>>
  /\ UNCHANGED dbVars /\ UNCHANGED lVars

TMerge ==
  /\ tpc = "merge"
  /\ \/ \E m \in stored, folded \in BOOLEAN :
          /\ tRet <= m /\ m <= tRead
          /\ LET value == Merged(tPos, tW, m, folded)
             IN tRows' = IF value = None THEN tRows ELSE tRows \cup {<<tPos, value>>}
          /\ tScanned' = tScanned \cup {tPos}
          /\ tPos' = tPos + 1 /\ tBudget' = tBudget - 1
          /\ tpc' = IF tPos + 1 = End \/ tBudget = 1 THEN "pageStart" ELSE "probe"
          /\ UNCHANGED tResult
     \/ /\ ~\E m \in stored : tRet <= m /\ m <= tRead
        /\ tpc' = "done" /\ tResult' = "SnapshotTooOld"
        /\ UNCHANGED <<tRows, tScanned, tPos, tBudget>>
  /\ tW' = {}
  /\ UNCHANGED <<tRead, tRet, tPhantom>> /\ UNCHANGED dbVars /\ UNCHANGED lVars

\* commitTransaction with readRanges: the whole range scanned, or ("pageRangesOnly") only the
\* keys the pages returned.
TCommit ==
  /\ tpc = "commit"
  /\ LET ranges == IF Broken = "pageRangesOnly" THEN {r[1] : r \in tRows} ELSE tScanned
         conflict == \E v \in (tRead + 1)..lastSeq : v > retained /\ keyOf[v] \in ranges
     IN tResult' = IF retained > tRead THEN "SnapshotTooOld"
                   ELSE IF conflict THEN "ReadConflict" ELSE "Committed"
  \* a write after readSeq to a key in the scanned range exists at the commit: a phantom
  /\ tPhantom' = \E v \in (tRead + 1)..lastSeq : keyOf[v] \in tScanned
  /\ tpc' = "done"
  /\ UNCHANGED <<tRead, tPos, tW, tRet, tRows, tBudget, tScanned>>
  /\ UNCHANGED dbVars /\ UNCHANGED lVars

Next ==
  \/ Write \/ Flush \/ Trim \/ Collect
  \/ LPageStart \/ LProbe \/ LMerge \/ LPageEnd
  \/ TBegin \/ TPageStart \/ TProbe \/ TMerge \/ TCommit

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* Every Latest page is the database at its snapshot over [its start, resumeFrom): no row
\* skipped, repeated, or from another moment. Pages follow each other from resumeFrom.
LatestPageAtSnapshot == lpc = "end" => lRows = Live(lStart, lPos, lSnap)

\* Every row a transaction's pages returned is the database at readSeq.
TxnPagesAtReadSeq == \A r \in tRows : r[2] = ValueAt(r[1], tRead)
\* A transaction's pages together hold every live key of the range at readSeq.
TxnPagesComplete == tpc = "commit" => tRows = Live(1, End, tRead)

\* A commit is admitted only if no write after readSeq touched a key in the range it scanned: a
\* phantom inside the range is always refused.
PhantomRefused == tResult = "Committed" => ~tPhantom

TypeOK == lpc \in {"idle", "probe", "merge", "end"} /\ tpc \in {"begin", "pageStart", "probe",
          "merge", "commit", "done"}
=============================================================================
