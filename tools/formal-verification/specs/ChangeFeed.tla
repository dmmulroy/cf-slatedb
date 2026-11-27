----------------------------- MODULE ChangeFeed -----------------------------
(***************************************************************************)
(* The change feed of one shaledb database (D113): a consumer following    *)
(* DbClient.changes from a cursor, against flushes whose commit may land   *)
(* and lose its response, writer restarts, and changelog pruning.          *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/ unless named):           *)
(*  - db-writer.ts flushThen with a changelog: afterSeq = flushed_seq and  *)
(*    cut = last_seq read together; the changelog SST of the rows in       *)
(*    (afterSeq, cut] exported; ManifestStore.updateFrom(commitL0Flush)    *)
(*    appends the entry [afterSeq + 1, cut]; then markFlushed(cut). A      *)
(*    commit may land and its response be lost: markFlushed does not run.  *)
(*    commitL0Flush refuses a flush whose afterSeq is below the manifest's *)
(*    lastL0Seq (FlushedSeqStaleError); the writer records the manifest's  *)
(*    lastL0Seq as flushed and flushes again from it (D113).               *)
(*  - startWriter after an eviction: flushed_seq raised to lastL0Seq.      *)
(*  - lsm-core pruneChangelog (retention, and D99's entry cap): drops the  *)
(*    oldest entry and raises changelogAfterSeq to its lastSeq (D80).      *)
(*  - db-client.ts changes: writer.changes (flushed_seq and the rows above *)
(*    max(afterSeq, flushed_seq), in one MemTable.consistently), then      *)
(*    db-reader.ts changelogRows for (afterSeq, flushed_seq] from a        *)
(*    manifest covering flushed_seq: Expired unless changelogAfterSeq <=   *)
(*    afterSeq (D80); entries in firstSeq order, each row once (rows at or *)
(*    below an earlier entry's lastSeq skipped, D113); at most `limit`     *)
(*    rows, and a flushed part cut at its limit ends the page (D93).       *)
(*                                                                         *)
(* Each sequence number is one batch of one row, so a change is its seq.   *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"                  the code as written                           *)
(*   "beforeD113"            no stale refusal and no reader skip: a lost   *)
(*                           response repeats changes in the feed          *)
(*   "lostResponseReexport"  no stale refusal (the reader skip stays): the *)
(*                           manifest holds overlapping entries            *)
(*   "noExpiredCheck"        the reader serves what entries remain after a *)
(*                           prune (before D80)                            *)
(*   "appendAfterCut"        a page appends the unflushed rows after a     *)
(*                           flushed part cut at its limit (before D93)    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxSeq, MaxLost, MaxRestarts, MaxPrunes, Limit, Broken

ASSUME Broken \in {"none", "beforeD113", "lostResponseReexport", "noExpiredCheck",
                   "appendAfterCut"}

VARIABLES
  lastSeq,     \* writer_meta.last_seq
  flushedSeq,  \* writer_meta.flushed_seq
  mL0,         \* the newest manifest's lastL0Seq
  entries,     \* its changelog: a set of [first, last] ranges, each holding every change in it
  cAfter,      \* its changelogAfterSeq
  fpc, fAfter, fCut,       \* the flush in progress
  lost, restarts, prunes,
  cpc, cursor, pFs, pUnflushed, delivered, expired  \* the consumer

vars == <<lastSeq, flushedSeq, mL0, entries, cAfter, fpc, fAfter, fCut, lost, restarts, prunes,
          cpc, cursor, pFs, pUnflushed, delivered, expired>>

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
StaleRefusal == Broken \notin {"beforeD113", "lostResponseReexport"}
ReaderSkip == Broken # "beforeD113"

\* The ascending sequence of a finite set of naturals.
RECURSIVE Ascending(_)
Ascending(S) == IF S = {} THEN << >>
                ELSE LET m == CHOOSE x \in S : \A y \in S : x <= y
                     IN <<m>> \o Ascending(S \ {m})

Take(s, n) == SubSeq(s, 1, Min(n, Len(s)))

\* changelogRows without the D113 skip: every entry in (first, last) order, each one's rows in
\* (after, upTo], one after another.
RECURSIVE Concat(_, _, _)
Concat(E, after, upTo) ==
  IF E = {} THEN << >>
  ELSE LET e == CHOOSE x \in E : \A y \in E : x[1] < y[1] \/ (x[1] = y[1] /\ x[2] <= y[2])
       IN Ascending({s \in e[1]..e[2] : after < s /\ s <= upTo}) \o Concat(E \ {e}, after, upTo)

\* changelogRows with the skip: each change in a kept entry once, in order.
FlushedRows(after, upTo) ==
  IF ReaderSkip
  THEN Ascending({s \in (after + 1)..upTo : \E e \in entries : e[1] <= s /\ s <= e[2]})
  ELSE Concat(entries, after, upTo)

Init ==
  /\ lastSeq = 0 /\ flushedSeq = 0 /\ mL0 = 0 /\ entries = {} /\ cAfter = 0
  /\ fpc = "idle" /\ fAfter = 0 /\ fCut = 0
  /\ lost = 0 /\ restarts = 0 /\ prunes = 0
  /\ cpc = "idle" /\ cursor = 0 /\ pFs = 0 /\ pUnflushed = << >> /\ delivered = << >>
  /\ expired = FALSE

consumerVars == <<cpc, cursor, pFs, pUnflushed, delivered, expired>>

Write ==
  /\ lastSeq < MaxSeq
  /\ lastSeq' = lastSeq + 1
  /\ UNCHANGED <<flushedSeq, mL0, entries, cAfter, fpc, fAfter, fCut, lost, restarts, prunes>>
  /\ UNCHANGED consumerVars

\* flushThen: meta read (afterSeq, cut) and the changelog SST of (afterSeq, cut] uploaded.
FlushStart ==
  /\ fpc = "idle" /\ lastSeq > flushedSeq
  /\ fpc' = "uploaded" /\ fAfter' = flushedSeq /\ fCut' = lastSeq
  /\ UNCHANGED <<lastSeq, flushedSeq, mL0, entries, cAfter, lost, restarts, prunes>>
  /\ UNCHANGED consumerVars

\* updateFrom(commitL0Flush): refused as stale (the manifest's flushed state recorded, the SSTs
\* left as orphans), or the entry appended; its response may be lost.
FlushCommit ==
  /\ fpc = "uploaded"
  /\ IF StaleRefusal /\ mL0 > fAfter
     THEN /\ flushedSeq' = Max(flushedSeq, mL0)
          /\ UNCHANGED <<mL0, entries, lost>>
     ELSE /\ entries' = entries \cup {<<fAfter + 1, fCut>>}
          /\ mL0' = Max(mL0, fCut)
          /\ \/ flushedSeq' = Max(flushedSeq, fCut) /\ UNCHANGED lost
             \/ lost < MaxLost /\ lost' = lost + 1 /\ UNCHANGED flushedSeq
  /\ fpc' = "idle" /\ fAfter' = 0 /\ fCut' = 0
  /\ UNCHANGED <<lastSeq, cAfter, restarts, prunes>> /\ UNCHANGED consumerVars

\* Eviction and startWriter: the flush in progress is gone; flushed_seq raised to lastL0Seq.
Restart ==
  /\ restarts < MaxRestarts
  /\ restarts' = restarts + 1
  /\ flushedSeq' = Max(flushedSeq, mL0)
  /\ fpc' = "idle" /\ fAfter' = 0 /\ fCut' = 0
  /\ UNCHANGED <<lastSeq, mL0, entries, cAfter, lost, prunes>> /\ UNCHANGED consumerVars

\* pruneChangelog: the oldest entry dropped; changelogAfterSeq raised to its lastSeq.
Prune ==
  /\ prunes < MaxPrunes
  /\ \E e \in entries :
       /\ \A x \in entries : e[1] < x[1] \/ (e[1] = x[1] /\ e[2] <= x[2])
       /\ entries' = entries \ {e}
       /\ cAfter' = Max(cAfter, e[2])
  /\ prunes' = prunes + 1
  /\ UNCHANGED <<lastSeq, flushedSeq, mL0, fpc, fAfter, fCut, lost, restarts>>
  /\ UNCHANGED consumerVars

\* writer.changes: flushed_seq and the unflushed rows above max(cursor, flushed_seq), together.
ConsumeProbe ==
  /\ cpc = "idle" /\ ~expired /\ Len(delivered) <= MaxSeq
  /\ pFs' = flushedSeq
  /\ pUnflushed' = Take(Ascending({s \in (Max(cursor, flushedSeq) + 1)..lastSeq : TRUE}), Limit)
  /\ cpc' = "probed"
  /\ UNCHANGED <<cursor, delivered, expired>>
  /\ UNCHANGED <<lastSeq, flushedSeq, mL0, entries, cAfter, fpc, fAfter, fCut, lost, restarts,
                 prunes>>

\* changelogRows for (cursor, flushed_seq] from the newest manifest, then the page.
ConsumeRead ==
  /\ cpc = "probed"
  /\ IF cursor < pFs /\ cAfter > cursor /\ Broken # "noExpiredCheck"
     THEN /\ expired' = TRUE
          /\ UNCHANGED <<cursor, delivered>>
     ELSE \E bound \in 1..Limit :
            \* the flushed part ends at `limit` rows or earlier at its byte bound (D93)
            LET all == IF cursor < pFs THEN FlushedRows(cursor, pFs) ELSE << >>
                flushed == Take(all, bound)
                complete == Len(all) <= bound
                page == IF complete \/ Broken = "appendAfterCut"
                        THEN Take(flushed \o pUnflushed, Limit)
                        ELSE flushed
            IN /\ delivered' = delivered \o page
               /\ cursor' = IF page = << >> THEN cursor ELSE page[Len(page)]
               /\ UNCHANGED expired
  /\ cpc' = "idle" /\ pFs' = 0 /\ pUnflushed' = << >>
  /\ UNCHANGED <<lastSeq, flushedSeq, mL0, entries, cAfter, fpc, fAfter, fCut, lost, restarts,
                 prunes>>

Next ==
  \/ Write \/ FlushStart \/ FlushCommit \/ Restart \/ Prune \/ ConsumeProbe \/ ConsumeRead

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
\* The consumer received changes 1, 2, ... in order, each once, with none skipped (or it was told
\* Expired and stopped).
ExactlyOnce == \A i \in 1..Len(delivered) : delivered[i] = i

\* No two changelog entries hold the same change.
NoOverlappingEntries ==
  \A e, f \in entries : e # f => (e[2] < f[1] \/ f[2] < e[1])

\* flushed_seq never passes the manifest: what the writer records as flushed is in L0.
FlushedInManifest == flushedSeq <= mL0
=============================================================================
