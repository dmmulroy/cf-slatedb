---------------------------- MODULE WriterObject ----------------------------
(***************************************************************************)
(* The writer Durable Object of one shaledb database at await granularity *)
(* (D109): concurrent requests, the alarm handler, eviction (memory lost,  *)
(* SQLite and the alarm kept), interruption of a request at any await, and *)
(* R2 failing before or after a manifest commit lands.                     *)
(*                                                                         *)
(* Code this models (packages/lsm-cloudflare/src/db-writer.ts unless       *)
(* named; each label is one step between two points where Effect may run   *)
(* another fiber: an R2 call, a lock, or between two SQLite statements     *)
(* that are not in one MemTable.consistently):                             *)
(*  - ensureReady / startWriter (initLock): fence in through               *)
(*    ManifestStore.update(fenceWriter) (a sealed manifest is kept),       *)
(*    seed + markFlushed(lastL0Seq), refreshUnflushedCounters, phase Ready, *)
(*    scheduleFlush.                                                       *)
(*  - commitBatch (commitLock): writableReady; for commitTransaction its   *)
(*    check (horizon, writtenAfter, horizon again: D84); then the          *)
(*    uninterruptible mark / MemTable.commit / end of the counters (D74b); *)
(*    the lock released, then scheduleFlush (Effect.ensuring), then ack.   *)
(*  - flushThen (flushLock): ensureReady, cut = last_seq, upload, the      *)
(*    manifest commit (commitL0Flush, plus sealEdit for a seal: D57),      *)
(*    markFlushed, trim, refresh; a WriterFencedError marks the phase      *)
(*    Fenced; refused as stale (D113): the manifest's lastL0Seq recorded   *)
(*    and the flush runs again. Nothing to flush: settleCounters... (D74). *)
(*  - refreshUnflushedCounters: scan, then install only if no commit ran   *)
(*    (generation, committing); retried or given up when overtaken.        *)
(*  - seal (commitLock): ensureReady, flushThen(sealEdit), or its own      *)
(*    commit when nothing was flushed, then phase Ready+sealed; any other  *)
(*    exit forgets the phase while the lock is held (D85).                 *)
(*  - probe: MemTable.consistently (snapshot, flushed_seq, rows), then the *)
(*    reader's manifest (flushed_seq <= lastL0Seq <= snapshot, else probe  *)
(*    again).                                                              *)
(*  - runAlarm: meta, ensureReady, flush when the counters are due,        *)
(*    scheduleFlush; a failure sets the alarm again (retryAlarm, D102)     *)
(*    unless the writer is fenced.                                         *)
(*  - scheduleFlush (scheduleLock, D102): one step; time is abstract: the  *)
(*    alarm is set whenever the counters show an unflushed row, and fires  *)
(*    when it is due.                                                      *)
(*                                                                         *)
(* Rows are their sequence numbers, each with one of Keys; SSTs keep every *)
(* row (NoAckedWriteLost checks keeping only the reachable versions).      *)
(*                                                                         *)
(* Broken selects a deliberately broken variant (must fail):               *)
(*   "none"              the code as written                               *)
(*   "noGeneration"      a counter refresh installs its scan even when a   *)
(*                       commit ran during it (before D74b)                *)
(*   "interruptedWrite"  the commit's mark, SQLite commit, and counters    *)
(*                       are interruptible and an interrupted write sets   *)
(*                       no alarm (before D74b's second half)              *)
(*   "noHorizonReread"   the transaction check does not re-read the        *)
(*                       horizon after its conflict query (before D84)     *)
(*   "sealKeepsPhase"    a seal that does not return keeps the phase       *)
(*                       (before D85)                                      *)
(*   "noAlarmRetry"      an alarm run that cannot read writer_meta sets no *)
(*                       alarm again (before D102)                         *)
(*   "alarmAfterCommit"  no alarm made sure of before the commit (before   *)
(*                       D131): an eviction after the SQLite commit leaves *)
(*                       rows with no alarm                                *)
(*   "alarmBeforeMark"   the alarm made sure of before the mark, and a     *)
(*                       scheduler ignores a commit under way: an alarm    *)
(*                       run between the two leaves none                   *)
(*   "schedulerIgnoresCommit" the check after the mark, but a scheduler    *)
(*                       that counts no row sets no alarm during a commit  *)
(*   "beforeD115"        both rules below as they were before D115         *)
(*   "markFencedAnyPhase" a flush's WriterFencedError marks the phase      *)
(*                       Fenced even when the writer started again since   *)
(*   "sealForgetsUntried" a seal that never applied its edit still forgets *)
(*                       the phase on exit                                 *)
(*   "noSettle"          a flush with nothing to flush keeps counters      *)
(*                       above SQLite, so the alarm fires forever (D74)    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Req, Keys, Ops, MaxOps, MaxSeq, MaxEvictions, MaxInterrupts, MaxFailures, MaxSeals, Broken

ASSUME Broken \in {"none", "noGeneration", "interruptedWrite", "noHorizonReread",
                   "sealKeepsPhase", "noAlarmRetry", "noSettle", "markFencedAnyPhase",
                   "sealForgetsUntried", "beforeD115", "alarmAfterCommit", "alarmBeforeMark",
                   "schedulerIgnoresCommit"}
ASSUME Ops \subseteq {"write", "txn", "flush", "seal", "probe"}

A == "alarm"
None == "none"
NoKey == "nokey"
TK == CHOOSE k \in Keys : TRUE   \* the key every transaction reads and writes
Max(a, b) == IF a >= b THEN a ELSE b

(* --algorithm WriterObject
variables
  \* SQLite (durable)
  lastSeq = 0, flushedSeq = 0, retained = 0, mem = {},
  keyOf = [s \in 1..MaxSeq |-> NoKey], marks = {},
  \* the platform alarm (durable)
  alarmSet = FALSE,
  \* the newest manifest in R2
  mEpoch = 0, mSealed = FALSE, mL0Seq = 0, mData = {}, sealedAt = 0,
  \* in memory: lost on eviction
  phase = "uninit", pEpoch = 0, pSealed = FALSE,
  cnt = {}, gen = 0, committing = FALSE,
  commitLock = None, flushLock = None, initLock = None,
  \* per request: results of procedures
  failed = [p \in Req \cup {A} |-> FALSE],
  rEpoch = [p \in Req \cup {A} |-> 0],
  rSealed = [p \in Req \cup {A} |-> FALSE],
  committed = [p \in Req \cup {A} |-> FALSE],
  \* a seal's edit has been applied to a manifest: from then its commit may land (D115)
  sealTried = [p \in Req \cup {A} |-> FALSE],
  \* history and bounds
  acked = {}, opsLeft = MaxOps, writesStarted = 0, evictions = 0, interrupts = 0, failures = 0, seals = 0,
  txnBad = FALSE, readBad = FALSE;

define
  Unflushed == {s \in mem : s > flushedSeq}
  CanFail == failures < MaxFailures
  RetainedFrom == IF retained <= flushedSeq THEN retained ELSE flushedSeq
  \* MemTable.trim: the newest flush in (retained, flushed] always stays; older ones go in order.
  OlderMarks == {c \in marks : RetainedFrom < c /\ c <= flushedSeq /\
                               \E d \in marks : c < d /\ d <= flushedSeq}
  TrimHorizons == {RetainedFrom} \cup OlderMarks
end define;

macro Schedule() begin
  \* scheduleFlush: under scheduleLock, from the counters only; with no row counted, a commit
  \* under way still keeps an alarm (D131).
  if cnt # {} \/ (committing /\ Broken \notin {"alarmBeforeMark", "schedulerIgnoresCommit"}) then
    alarmSet := TRUE;
  end if;
end macro;

procedure Refresh()
variables before = 0, scan = {};
begin
RfScan:
  before := gen;
  scan := {s \in mem : s > flushedSeq};
RfInstall:
  if (gen = before /\ ~committing) \/ Broken = "noGeneration" then
    cnt := scan;
    return;
  else
    either goto RfScan; or return; end either;
  end if;
end procedure;

procedure EnsureReady()
variables stEpoch = 0, stSealed = FALSE;
begin
ErCheck:
  \* The phase read from memory, and when not Ready, initLock taken and the phase read again:
  \* no await between them when the lock is free.
  if phase = "ready" then
    rEpoch[self] := pEpoch || rSealed[self] := pSealed;
    return;
  elsif phase = "fenced" then
    failed[self] := TRUE;
    return;
  else
    await initLock = None;
    initLock := self;
  end if;
StFence:
  \* ManifestStore.update(fenceWriter), unless sealed: an R2 round trip that may fail before or
  \* after the new manifest lands.
  either
    await CanFail;
    failures := failures + 1;
    initLock := None;
    failed[self] := TRUE;
    return;
  or
    if ~mSealed then mEpoch := mEpoch + 1; end if;
    either
      await CanFail;
      failures := failures + 1;
      initLock := None;
      failed[self] := TRUE;
      return;
    or
      \* then setIdentity, seed, markFlushed(lastL0Seq): rows in L0 are recorded as flushed
      \* (SQLite statements under initLock, with nothing else running on the writer's state).
      stEpoch := mEpoch || stSealed := mSealed;
      lastSeq := Max(lastSeq, mL0Seq) || flushedSeq := Max(flushedSeq, mL0Seq);
    end either;
  end either;
StRefresh:
  call Refresh();
StReady:
  phase := "ready" || pEpoch := stEpoch || pSealed := stSealed;
  rEpoch[self] := stEpoch || rSealed[self] := stSealed;
  if ~stSealed then Schedule(); end if;
  initLock := None;
  return;
end procedure;

procedure FlushThen(sealing)
variables fEpoch = 0, cut = 0, ffs = 0, sst = {};
begin
FlLock:
  await flushLock = None;
  flushLock := self;
  committed[self] := FALSE;
FlReady:
  call EnsureReady();
FlMeta:
  if failed[self] then
    return;
  elsif lastSeq <= flushedSeq then
    if cnt # {} /\ Broken # "noSettle" then
      goto FlSettle;
    else
      flushLock := None;
      return;
    end if;
  else
    fEpoch := rEpoch[self] || cut := lastSeq || ffs := flushedSeq;
  end if;
FlUpload:
  either
    await CanFail;
    failures := failures + 1;
    failed[self] := TRUE;
    return;
  or
    sst := {s \in mem : ffs < s /\ s <= cut};
  end either;
FlCommit:
  \* ManifestStore.updateFrom(commitL0Flush, then sealEdit for a seal), atomic at the head.
  either
    \* the create fails after the edit was applied
    await CanFail;
    failures := failures + 1;
    if sealing then sealTried[self] := TRUE; end if;
    failed[self] := TRUE;
    return;
  or
    if mEpoch # fEpoch then
      \* WriterFencedError: markFenced, only over the phase this flush started from (D115). (A sealed manifest has a newer epoch than any open
      \* writer's; a sealed writer's flush is refused with DatabaseSealedError.)
      if Broken \in {"markFencedAnyPhase", "beforeD115"} \/ (phase = "ready" /\ pEpoch = fEpoch) then
        phase := "fenced";
      end if;
      failed[self] := TRUE;
      return;
    elsif mSealed then
      failed[self] := TRUE;
      return;
    elsif mL0Seq > ffs then
      \* FlushedSeqStaleError (D113): an earlier commit landed unrecorded; record the manifest's
      \* lastL0Seq as flushed and flush again from it (this attempt's SST is an orphan).
      flushedSeq := Max(flushedSeq, mL0Seq);
      goto FlMeta;
    else
      mData := mData \cup sst || mL0Seq := Max(mL0Seq, cut);
      if sealing then
        sealTried[self] := TRUE;
        mSealed := TRUE || mEpoch := mEpoch + 1 || sealedAt := Max(mL0Seq, cut);
      end if;
      either
        \* the commit landed and its response was lost
        await CanFail;
        failures := failures + 1;
        failed[self] := TRUE;
        return;
      or
        committed[self] := TRUE;
      end either;
    end if;
  end either;
FlMark:
  \* markFlushed(cut): only after the commit.
  if cut > flushedSeq then
    flushedSeq := cut || marks := marks \cup {cut};
  end if;
FlTrim:
  with h \in TrimHorizons do
    retained := h || mem := {s \in mem : s > h} || marks := {c \in marks : c > h};
  end with;
FlRefresh:
  call Refresh();
FlDone:
  flushLock := None;
  return;
FlSettle:
  call Refresh();
FlSettleDone:
  flushLock := None;
  return;
end procedure;

process r \in Req
variables op = "none", seq = 0, tr = 0, snap = 0, rfs = 0, rows = {}, racked = {};
begin
Idle:
  await opsLeft > 0;
  opsLeft := opsLeft - 1;
  either
    await "write" \in Ops /\ writesStarted < MaxSeq;
    op := "write"; writesStarted := writesStarted + 1;
    goto WLock;
  or
    await "txn" \in Ops /\ writesStarted < MaxSeq;
    with t \in 0..lastSeq do tr := t; end with;
    op := "txn"; writesStarted := writesStarted + 1;
    goto WLock;
  or
    await "flush" \in Ops;
    op := "flush";
    goto FReady;
  or
    await "seal" \in Ops /\ seals < MaxSeals;
    op := "seal"; seals := seals + 1;
    goto SLock;
  or
    await "probe" \in Ops;
    op := "probe";
    goto PReady;
  end either;

\* ---- write and commitTransaction: commitBatch ----
WLock:
  await commitLock = None;
  commitLock := self;
WReady:
  call EnsureReady();
WCheck:
  if failed[self] \/ rSealed[self] then goto Cleanup;
  elsif op = "write" then goto WAlarmEarly;
  end if;
THorizon:
  if RetainedFrom > tr \/ tr > lastSeq then goto Cleanup; end if;
TQuery:
  \* MemTable.writtenAfter([TK], [], tr): retained or unflushed rows.
  if \E s \in mem : s > tr /\ keyOf[s] = TK then goto Cleanup; end if;
THorizonAgain:
  if Broken # "noHorizonReread" /\ RetainedFrom > tr then goto Cleanup; end if;
WAlarmEarly:
  \* "alarmBeforeMark": the alarm made sure of before the mark
  if Broken = "alarmBeforeMark" /\ ~alarmSet then alarmSet := TRUE; end if;
WMark:
  gen := gen + 1 || committing := TRUE;
WAlarm:
  \* ensureAlarmBeforeCommit (D131): an alarm exists before the rows reach SQLite
  if Broken \notin {"alarmAfterCommit", "alarmBeforeMark"} /\ ~alarmSet then alarmSet := TRUE; end if;
WSql:
  \* MemTable.commit: the next seq and its row, in one SQLite transaction.
  if op = "txn" then
    if \E s \in (tr + 1)..lastSeq : keyOf[s] = TK then txnBad := TRUE; end if;
    keyOf[lastSeq + 1] := TK;
  else
    with k \in Keys do keyOf[lastSeq + 1] := k; end with;
  end if;
  seq := lastSeq + 1;
  lastSeq := lastSeq + 1 || mem := mem \cup {lastSeq + 1};
WEnd:
  gen := gen + 1 || committing := FALSE || cnt := cnt \cup {seq};
WDone:
  \* the lock released, scheduleFlush (ensuring), then the acknowledgement
  commitLock := None;
  Schedule();
  acked := acked \cup {seq};
  goto Cleanup;

\* ---- requestFlush ----
FReady:
  call EnsureReady();
FGo:
  if failed[self] then goto Cleanup; else call FlushThen(FALSE); end if;
FDone:
  goto Cleanup;

\* ---- seal ----
SLock:
  await commitLock = None;
  commitLock := self;
SReady:
  call EnsureReady();
SCheck:
  if failed[self] then goto Cleanup;
  elsif rSealed[self] then op := "sealed"; goto Cleanup;
  end if;
SFlush:
  call FlushThen(TRUE);
SAfter:
  if failed[self] then goto Cleanup;
  elsif committed[self] then goto SSet;
  end if;
SCommit:
  \* nothing was flushed: the seal commits on its own (updateFrom(sealEdit)), its edit applied
  sealTried[self] := TRUE;
  either
    await CanFail;
    failures := failures + 1;
    goto Cleanup;
  or
    if mSealed then
      skip;
    elsif mEpoch # rEpoch[self] then
      goto Cleanup;
    else
      mSealed := TRUE || mEpoch := mEpoch + 1 || sealedAt := mL0Seq;
      either
        await CanFail;
        failures := failures + 1;
        goto Cleanup;
      or
        skip;
      end either;
    end if;
  end either;
SSet:
  phase := "ready" || pEpoch := mEpoch || pSealed := TRUE;
  op := "sealed";
  goto Cleanup;

\* ---- probe and the Latest read ----
PReady:
  call EnsureReady();
PRead:
  \* MemTable.consistently: snapshot, flushed_seq, and the rows above it, at once
  if failed[self] then
    goto Cleanup;
  else
    snap := lastSeq || rfs := flushedSeq || racked := acked ||
    rows := {s \in mem : flushedSeq < s /\ s <= lastSeq};
  end if;
PManifest:
  if rfs <= mL0Seq /\ mL0Seq <= snap then
    if ~(racked \subseteq rows \cup {s \in mData : s <= snap}) then readBad := TRUE; end if;
    goto Cleanup;
  else
    goto PReady;
  end if;

\* ---- every exit: finalizers (locks, the seal's onExit, the write's ensuring) ----
Cleanup:
  \* the seal's onExit runs inside its commit lock, and forgets the phase only once its edit was
  \* applied (D115)
  if /\ op = "seal" /\ commitLock = self /\ Broken # "sealKeepsPhase" /\ phase = "ready"
     /\ (sealTried[self] \/ Broken \in {"sealForgetsUntried", "beforeD115"}) then
    phase := "uninit";
  end if;
  if commitLock = self then commitLock := None; end if;
  if flushLock = self then flushLock := None; end if;
  if initLock = self then initLock := None; end if;
  if op \in {"write", "txn"} /\ Broken # "interruptedWrite" then Schedule(); end if;
  failed[self] := FALSE || sealTried[self] := FALSE;
  op := "none" || seq := 0 || tr := 0 || snap := 0 || rfs := 0 || rows := {} || racked := {};
  goto Idle;
end process;

process alarm = A
begin
AIdle:
  await alarmSet;
  alarmSet := FALSE;
AMeta:
  \* memTable.meta: unreadable storage sets the alarm again (D102)
  either
    await CanFail;
    failures := failures + 1;
    if Broken # "noAlarmRetry" then alarmSet := TRUE; end if;
    goto AIdle;
  or
    skip;
  end either;
AReady:
  call EnsureReady();
ADue:
  if failed[A] then goto AFail;
  elsif cnt # {} then call FlushThen(FALSE);
  end if;
ASched:
  if failed[A] then
    goto AFail;
  else
    Schedule();
    goto AEnd;
  end if;
AFail:
  \* retryAlarm, unless this writer is fenced
  if phase # "fenced" then alarmSet := TRUE; end if;
AEnd:
  if flushLock = A then flushLock := None; end if;
  if initLock = A then initLock := None; end if;
  failed[A] := FALSE;
  goto AIdle;
end process;
end algorithm; *)
\* BEGIN TRANSLATION
CONSTANT defaultInitValue
VARIABLES lastSeq, flushedSeq, retained, mem, keyOf, marks, alarmSet, mEpoch, 
          mSealed, mL0Seq, mData, sealedAt, phase, pEpoch, pSealed, cnt, gen, 
          committing, commitLock, flushLock, initLock, failed, rEpoch, 
          rSealed, committed, sealTried, acked, opsLeft, writesStarted, 
          evictions, interrupts, failures, seals, txnBad, readBad, pc, stack

(* define statement *)
Unflushed == {s \in mem : s > flushedSeq}
CanFail == failures < MaxFailures
RetainedFrom == IF retained <= flushedSeq THEN retained ELSE flushedSeq

OlderMarks == {c \in marks : RetainedFrom < c /\ c <= flushedSeq /\
                             \E d \in marks : c < d /\ d <= flushedSeq}
TrimHorizons == {RetainedFrom} \cup OlderMarks

VARIABLES before, scan, stEpoch, stSealed, sealing, fEpoch, cut, ffs, sst, op, 
          seq, tr, snap, rfs, rows, racked

vars == << lastSeq, flushedSeq, retained, mem, keyOf, marks, alarmSet, mEpoch, 
           mSealed, mL0Seq, mData, sealedAt, phase, pEpoch, pSealed, cnt, gen, 
           committing, commitLock, flushLock, initLock, failed, rEpoch, 
           rSealed, committed, sealTried, acked, opsLeft, writesStarted, 
           evictions, interrupts, failures, seals, txnBad, readBad, pc, stack, 
           before, scan, stEpoch, stSealed, sealing, fEpoch, cut, ffs, sst, 
           op, seq, tr, snap, rfs, rows, racked >>

ProcSet == (Req) \cup {A}

Init == (* Global variables *)
        /\ lastSeq = 0
        /\ flushedSeq = 0
        /\ retained = 0
        /\ mem = {}
        /\ keyOf = [s \in 1..MaxSeq |-> NoKey]
        /\ marks = {}
        /\ alarmSet = FALSE
        /\ mEpoch = 0
        /\ mSealed = FALSE
        /\ mL0Seq = 0
        /\ mData = {}
        /\ sealedAt = 0
        /\ phase = "uninit"
        /\ pEpoch = 0
        /\ pSealed = FALSE
        /\ cnt = {}
        /\ gen = 0
        /\ committing = FALSE
        /\ commitLock = None
        /\ flushLock = None
        /\ initLock = None
        /\ failed = [p \in Req \cup {A} |-> FALSE]
        /\ rEpoch = [p \in Req \cup {A} |-> 0]
        /\ rSealed = [p \in Req \cup {A} |-> FALSE]
        /\ committed = [p \in Req \cup {A} |-> FALSE]
        /\ sealTried = [p \in Req \cup {A} |-> FALSE]
        /\ acked = {}
        /\ opsLeft = MaxOps
        /\ writesStarted = 0
        /\ evictions = 0
        /\ interrupts = 0
        /\ failures = 0
        /\ seals = 0
        /\ txnBad = FALSE
        /\ readBad = FALSE
        (* Procedure Refresh *)
        /\ before = [ self \in ProcSet |-> 0]
        /\ scan = [ self \in ProcSet |-> {}]
        (* Procedure EnsureReady *)
        /\ stEpoch = [ self \in ProcSet |-> 0]
        /\ stSealed = [ self \in ProcSet |-> FALSE]
        (* Procedure FlushThen *)
        /\ sealing = [ self \in ProcSet |-> defaultInitValue]
        /\ fEpoch = [ self \in ProcSet |-> 0]
        /\ cut = [ self \in ProcSet |-> 0]
        /\ ffs = [ self \in ProcSet |-> 0]
        /\ sst = [ self \in ProcSet |-> {}]
        (* Process r *)
        /\ op = [self \in Req |-> "none"]
        /\ seq = [self \in Req |-> 0]
        /\ tr = [self \in Req |-> 0]
        /\ snap = [self \in Req |-> 0]
        /\ rfs = [self \in Req |-> 0]
        /\ rows = [self \in Req |-> {}]
        /\ racked = [self \in Req |-> {}]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in Req -> "Idle"
                                        [] self = A -> "AIdle"]

RfScan(self) == /\ pc[self] = "RfScan"
                /\ before' = [before EXCEPT ![self] = gen]
                /\ scan' = [scan EXCEPT ![self] = {s \in mem : s > flushedSeq}]
                /\ pc' = [pc EXCEPT ![self] = "RfInstall"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, stack, stEpoch, stSealed, sealing, 
                                fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                                rows, racked >>

RfInstall(self) == /\ pc[self] = "RfInstall"
                   /\ IF (gen = before[self] /\ ~committing) \/ Broken = "noGeneration"
                         THEN /\ cnt' = scan[self]
                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ before' = [before EXCEPT ![self] = Head(stack[self]).before]
                              /\ scan' = [scan EXCEPT ![self] = Head(stack[self]).scan]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         ELSE /\ \/ /\ pc' = [pc EXCEPT ![self] = "RfScan"]
                                    /\ UNCHANGED <<stack, before, scan>>
                                 \/ /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ before' = [before EXCEPT ![self] = Head(stack[self]).before]
                                    /\ scan' = [scan EXCEPT ![self] = Head(stack[self]).scan]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ cnt' = cnt
                   /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                   marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                   mData, sealedAt, phase, pEpoch, pSealed, 
                                   gen, committing, commitLock, flushLock, 
                                   initLock, failed, rEpoch, rSealed, 
                                   committed, sealTried, acked, opsLeft, 
                                   writesStarted, evictions, interrupts, 
                                   failures, seals, txnBad, readBad, stEpoch, 
                                   stSealed, sealing, fEpoch, cut, ffs, sst, 
                                   op, seq, tr, snap, rfs, rows, racked >>

Refresh(self) == RfScan(self) \/ RfInstall(self)

ErCheck(self) == /\ pc[self] = "ErCheck"
                 /\ IF phase = "ready"
                       THEN /\ /\ rEpoch' = [rEpoch EXCEPT ![self] = pEpoch]
                               /\ rSealed' = [rSealed EXCEPT ![self] = pSealed]
                            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                            /\ stEpoch' = [stEpoch EXCEPT ![self] = Head(stack[self]).stEpoch]
                            /\ stSealed' = [stSealed EXCEPT ![self] = Head(stack[self]).stSealed]
                            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            /\ UNCHANGED << initLock, failed >>
                       ELSE /\ IF phase = "fenced"
                                  THEN /\ failed' = [failed EXCEPT ![self] = TRUE]
                                       /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                       /\ stEpoch' = [stEpoch EXCEPT ![self] = Head(stack[self]).stEpoch]
                                       /\ stSealed' = [stSealed EXCEPT ![self] = Head(stack[self]).stSealed]
                                       /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                       /\ UNCHANGED initLock
                                  ELSE /\ initLock = None
                                       /\ initLock' = self
                                       /\ pc' = [pc EXCEPT ![self] = "StFence"]
                                       /\ UNCHANGED << failed, stack, stEpoch, 
                                                       stSealed >>
                            /\ UNCHANGED << rEpoch, rSealed >>
                 /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                 marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                 mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                 gen, committing, commitLock, flushLock, 
                                 committed, sealTried, acked, opsLeft, 
                                 writesStarted, evictions, interrupts, 
                                 failures, seals, txnBad, readBad, before, 
                                 scan, sealing, fEpoch, cut, ffs, sst, op, seq, 
                                 tr, snap, rfs, rows, racked >>

StFence(self) == /\ pc[self] = "StFence"
                 /\ \/ /\ CanFail
                       /\ failures' = failures + 1
                       /\ initLock' = None
                       /\ failed' = [failed EXCEPT ![self] = TRUE]
                       /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                       /\ stEpoch' = [stEpoch EXCEPT ![self] = Head(stack[self]).stEpoch]
                       /\ stSealed' = [stSealed EXCEPT ![self] = Head(stack[self]).stSealed]
                       /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                       /\ UNCHANGED <<lastSeq, flushedSeq, mEpoch>>
                    \/ /\ IF ~mSealed
                             THEN /\ mEpoch' = mEpoch + 1
                             ELSE /\ TRUE
                                  /\ UNCHANGED mEpoch
                       /\ \/ /\ CanFail
                             /\ failures' = failures + 1
                             /\ initLock' = None
                             /\ failed' = [failed EXCEPT ![self] = TRUE]
                             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                             /\ stEpoch' = [stEpoch EXCEPT ![self] = Head(stack[self]).stEpoch]
                             /\ stSealed' = [stSealed EXCEPT ![self] = Head(stack[self]).stSealed]
                             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                             /\ UNCHANGED <<lastSeq, flushedSeq>>
                          \/ /\ /\ stEpoch' = [stEpoch EXCEPT ![self] = mEpoch']
                                /\ stSealed' = [stSealed EXCEPT ![self] = mSealed]
                             /\ /\ flushedSeq' = Max(flushedSeq, mL0Seq)
                                /\ lastSeq' = Max(lastSeq, mL0Seq)
                             /\ pc' = [pc EXCEPT ![self] = "StRefresh"]
                             /\ UNCHANGED <<initLock, failed, failures, stack>>
                 /\ UNCHANGED << retained, mem, keyOf, marks, alarmSet, 
                                 mSealed, mL0Seq, mData, sealedAt, phase, 
                                 pEpoch, pSealed, cnt, gen, committing, 
                                 commitLock, flushLock, rEpoch, rSealed, 
                                 committed, sealTried, acked, opsLeft, 
                                 writesStarted, evictions, interrupts, seals, 
                                 txnBad, readBad, before, scan, sealing, 
                                 fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                                 rows, racked >>

StRefresh(self) == /\ pc[self] = "StRefresh"
                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Refresh",
                                                            pc        |->  "StReady",
                                                            before    |->  before[self],
                                                            scan      |->  scan[self] ] >>
                                                        \o stack[self]]
                   /\ before' = [before EXCEPT ![self] = 0]
                   /\ scan' = [scan EXCEPT ![self] = {}]
                   /\ pc' = [pc EXCEPT ![self] = "RfScan"]
                   /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                   marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                   mData, sealedAt, phase, pEpoch, pSealed, 
                                   cnt, gen, committing, commitLock, flushLock, 
                                   initLock, failed, rEpoch, rSealed, 
                                   committed, sealTried, acked, opsLeft, 
                                   writesStarted, evictions, interrupts, 
                                   failures, seals, txnBad, readBad, stEpoch, 
                                   stSealed, sealing, fEpoch, cut, ffs, sst, 
                                   op, seq, tr, snap, rfs, rows, racked >>

StReady(self) == /\ pc[self] = "StReady"
                 /\ /\ pEpoch' = stEpoch[self]
                    /\ pSealed' = stSealed[self]
                    /\ phase' = "ready"
                 /\ /\ rEpoch' = [rEpoch EXCEPT ![self] = stEpoch[self]]
                    /\ rSealed' = [rSealed EXCEPT ![self] = stSealed[self]]
                 /\ IF ~stSealed[self]
                       THEN /\ IF cnt # {} \/ (committing /\ Broken \notin {"alarmBeforeMark", "schedulerIgnoresCommit"})
                                  THEN /\ alarmSet' = TRUE
                                  ELSE /\ TRUE
                                       /\ UNCHANGED alarmSet
                       ELSE /\ TRUE
                            /\ UNCHANGED alarmSet
                 /\ initLock' = None
                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                 /\ stEpoch' = [stEpoch EXCEPT ![self] = Head(stack[self]).stEpoch]
                 /\ stSealed' = [stSealed EXCEPT ![self] = Head(stack[self]).stSealed]
                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                 /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                 marks, mEpoch, mSealed, mL0Seq, mData, 
                                 sealedAt, cnt, gen, committing, commitLock, 
                                 flushLock, failed, committed, sealTried, 
                                 acked, opsLeft, writesStarted, evictions, 
                                 interrupts, failures, seals, txnBad, readBad, 
                                 before, scan, sealing, fEpoch, cut, ffs, sst, 
                                 op, seq, tr, snap, rfs, rows, racked >>

EnsureReady(self) == ErCheck(self) \/ StFence(self) \/ StRefresh(self)
                        \/ StReady(self)

FlLock(self) == /\ pc[self] = "FlLock"
                /\ flushLock = None
                /\ flushLock' = self
                /\ committed' = [committed EXCEPT ![self] = FALSE]
                /\ pc' = [pc EXCEPT ![self] = "FlReady"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, initLock, failed, 
                                rEpoch, rSealed, sealTried, acked, opsLeft, 
                                writesStarted, evictions, interrupts, failures, 
                                seals, txnBad, readBad, stack, before, scan, 
                                stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                                sst, op, seq, tr, snap, rfs, rows, racked >>

FlReady(self) == /\ pc[self] = "FlReady"
                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "EnsureReady",
                                                          pc        |->  "FlMeta",
                                                          stEpoch   |->  stEpoch[self],
                                                          stSealed  |->  stSealed[self] ] >>
                                                      \o stack[self]]
                 /\ stEpoch' = [stEpoch EXCEPT ![self] = 0]
                 /\ stSealed' = [stSealed EXCEPT ![self] = FALSE]
                 /\ pc' = [pc EXCEPT ![self] = "ErCheck"]
                 /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                 marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                 mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                 gen, committing, commitLock, flushLock, 
                                 initLock, failed, rEpoch, rSealed, committed, 
                                 sealTried, acked, opsLeft, writesStarted, 
                                 evictions, interrupts, failures, seals, 
                                 txnBad, readBad, before, scan, sealing, 
                                 fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                                 rows, racked >>

FlMeta(self) == /\ pc[self] = "FlMeta"
                /\ IF failed[self]
                      THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                           /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                           /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                           /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                           /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                           /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                           /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           /\ UNCHANGED flushLock
                      ELSE /\ IF lastSeq <= flushedSeq
                                 THEN /\ IF cnt # {} /\ Broken # "noSettle"
                                            THEN /\ pc' = [pc EXCEPT ![self] = "FlSettle"]
                                                 /\ UNCHANGED << flushLock, 
                                                                 stack, 
                                                                 sealing, 
                                                                 fEpoch, cut, 
                                                                 ffs, sst >>
                                            ELSE /\ flushLock' = None
                                                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                                 /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                                                 /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                                                 /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                                                 /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                                                 /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                                                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                 ELSE /\ /\ cut' = [cut EXCEPT ![self] = lastSeq]
                                         /\ fEpoch' = [fEpoch EXCEPT ![self] = rEpoch[self]]
                                         /\ ffs' = [ffs EXCEPT ![self] = flushedSeq]
                                      /\ pc' = [pc EXCEPT ![self] = "FlUpload"]
                                      /\ UNCHANGED << flushLock, stack, 
                                                      sealing, sst >>
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, initLock, failed, 
                                rEpoch, rSealed, committed, sealTried, acked, 
                                opsLeft, writesStarted, evictions, interrupts, 
                                failures, seals, txnBad, readBad, before, scan, 
                                stEpoch, stSealed, op, seq, tr, snap, rfs, 
                                rows, racked >>

FlUpload(self) == /\ pc[self] = "FlUpload"
                  /\ \/ /\ CanFail
                        /\ failures' = failures + 1
                        /\ failed' = [failed EXCEPT ![self] = TRUE]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                        /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                        /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                        /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                        /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                     \/ /\ sst' = [sst EXCEPT ![self] = {s \in mem : ffs[self] < s /\ s <= cut[self]}]
                        /\ pc' = [pc EXCEPT ![self] = "FlCommit"]
                        /\ UNCHANGED <<failed, failures, stack, sealing, fEpoch, cut, ffs>>
                  /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                  marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                  mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                  gen, committing, commitLock, flushLock, 
                                  initLock, rEpoch, rSealed, committed, 
                                  sealTried, acked, opsLeft, writesStarted, 
                                  evictions, interrupts, seals, txnBad, 
                                  readBad, before, scan, stEpoch, stSealed, op, 
                                  seq, tr, snap, rfs, rows, racked >>

FlCommit(self) == /\ pc[self] = "FlCommit"
                  /\ \/ /\ CanFail
                        /\ failures' = failures + 1
                        /\ IF sealing[self]
                              THEN /\ sealTried' = [sealTried EXCEPT ![self] = TRUE]
                              ELSE /\ TRUE
                                   /\ UNCHANGED sealTried
                        /\ failed' = [failed EXCEPT ![self] = TRUE]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                        /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                        /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                        /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                        /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED <<flushedSeq, mEpoch, mSealed, mL0Seq, mData, sealedAt, phase, committed>>
                     \/ /\ IF mEpoch # fEpoch[self]
                              THEN /\ IF Broken \in {"markFencedAnyPhase", "beforeD115"} \/ (phase = "ready" /\ pEpoch = fEpoch[self])
                                         THEN /\ phase' = "fenced"
                                         ELSE /\ TRUE
                                              /\ phase' = phase
                                   /\ failed' = [failed EXCEPT ![self] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                                   /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                                   /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                                   /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                                   /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << flushedSeq, mEpoch, mSealed, 
                                                   mL0Seq, mData, sealedAt, 
                                                   committed, sealTried, 
                                                   failures >>
                              ELSE /\ IF mSealed
                                         THEN /\ failed' = [failed EXCEPT ![self] = TRUE]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                                              /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                                              /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                                              /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                                              /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                              /\ UNCHANGED << flushedSeq, 
                                                              mEpoch, mSealed, 
                                                              mL0Seq, mData, 
                                                              sealedAt, 
                                                              committed, 
                                                              sealTried, 
                                                              failures >>
                                         ELSE /\ IF mL0Seq > ffs[self]
                                                    THEN /\ flushedSeq' = Max(flushedSeq, mL0Seq)
                                                         /\ pc' = [pc EXCEPT ![self] = "FlMeta"]
                                                         /\ UNCHANGED << mEpoch, 
                                                                         mSealed, 
                                                                         mL0Seq, 
                                                                         mData, 
                                                                         sealedAt, 
                                                                         failed, 
                                                                         committed, 
                                                                         sealTried, 
                                                                         failures, 
                                                                         stack, 
                                                                         sealing, 
                                                                         fEpoch, 
                                                                         cut, 
                                                                         ffs, 
                                                                         sst >>
                                                    ELSE /\ /\ mData' = (mData \cup sst[self])
                                                            /\ mL0Seq' = Max(mL0Seq, cut[self])
                                                         /\ IF sealing[self]
                                                               THEN /\ sealTried' = [sealTried EXCEPT ![self] = TRUE]
                                                                    /\ /\ mEpoch' = mEpoch + 1
                                                                       /\ mSealed' = TRUE
                                                                       /\ sealedAt' = Max(mL0Seq', cut[self])
                                                               ELSE /\ TRUE
                                                                    /\ UNCHANGED << mEpoch, 
                                                                                    mSealed, 
                                                                                    sealedAt, 
                                                                                    sealTried >>
                                                         /\ \/ /\ CanFail
                                                               /\ failures' = failures + 1
                                                               /\ failed' = [failed EXCEPT ![self] = TRUE]
                                                               /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                                               /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                                                               /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                                                               /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                                                               /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                                                               /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                                                               /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                                               /\ UNCHANGED committed
                                                            \/ /\ committed' = [committed EXCEPT ![self] = TRUE]
                                                               /\ pc' = [pc EXCEPT ![self] = "FlMark"]
                                                               /\ UNCHANGED <<failed, failures, stack, sealing, fEpoch, cut, ffs, sst>>
                                                         /\ UNCHANGED flushedSeq
                                   /\ phase' = phase
                  /\ UNCHANGED << lastSeq, retained, mem, keyOf, marks, 
                                  alarmSet, pEpoch, pSealed, cnt, gen, 
                                  committing, commitLock, flushLock, initLock, 
                                  rEpoch, rSealed, acked, opsLeft, 
                                  writesStarted, evictions, interrupts, seals, 
                                  txnBad, readBad, before, scan, stEpoch, 
                                  stSealed, op, seq, tr, snap, rfs, rows, 
                                  racked >>

FlMark(self) == /\ pc[self] = "FlMark"
                /\ IF cut[self] > flushedSeq
                      THEN /\ /\ flushedSeq' = cut[self]
                              /\ marks' = (marks \cup {cut[self]})
                      ELSE /\ TRUE
                           /\ UNCHANGED << flushedSeq, marks >>
                /\ pc' = [pc EXCEPT ![self] = "FlTrim"]
                /\ UNCHANGED << lastSeq, retained, mem, keyOf, alarmSet, 
                                mEpoch, mSealed, mL0Seq, mData, sealedAt, 
                                phase, pEpoch, pSealed, cnt, gen, committing, 
                                commitLock, flushLock, initLock, failed, 
                                rEpoch, rSealed, committed, sealTried, acked, 
                                opsLeft, writesStarted, evictions, interrupts, 
                                failures, seals, txnBad, readBad, stack, 
                                before, scan, stEpoch, stSealed, sealing, 
                                fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                                rows, racked >>

FlTrim(self) == /\ pc[self] = "FlTrim"
                /\ \E h \in TrimHorizons:
                     /\ marks' = {c \in marks : c > h}
                     /\ mem' = {s \in mem : s > h}
                     /\ retained' = h
                /\ pc' = [pc EXCEPT ![self] = "FlRefresh"]
                /\ UNCHANGED << lastSeq, flushedSeq, keyOf, alarmSet, mEpoch, 
                                mSealed, mL0Seq, mData, sealedAt, phase, 
                                pEpoch, pSealed, cnt, gen, committing, 
                                commitLock, flushLock, initLock, failed, 
                                rEpoch, rSealed, committed, sealTried, acked, 
                                opsLeft, writesStarted, evictions, interrupts, 
                                failures, seals, txnBad, readBad, stack, 
                                before, scan, stEpoch, stSealed, sealing, 
                                fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                                rows, racked >>

FlRefresh(self) == /\ pc[self] = "FlRefresh"
                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Refresh",
                                                            pc        |->  "FlDone",
                                                            before    |->  before[self],
                                                            scan      |->  scan[self] ] >>
                                                        \o stack[self]]
                   /\ before' = [before EXCEPT ![self] = 0]
                   /\ scan' = [scan EXCEPT ![self] = {}]
                   /\ pc' = [pc EXCEPT ![self] = "RfScan"]
                   /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                   marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                   mData, sealedAt, phase, pEpoch, pSealed, 
                                   cnt, gen, committing, commitLock, flushLock, 
                                   initLock, failed, rEpoch, rSealed, 
                                   committed, sealTried, acked, opsLeft, 
                                   writesStarted, evictions, interrupts, 
                                   failures, seals, txnBad, readBad, stEpoch, 
                                   stSealed, sealing, fEpoch, cut, ffs, sst, 
                                   op, seq, tr, snap, rfs, rows, racked >>

FlDone(self) == /\ pc[self] = "FlDone"
                /\ flushLock' = None
                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, initLock, failed, 
                                rEpoch, rSealed, committed, sealTried, acked, 
                                opsLeft, writesStarted, evictions, interrupts, 
                                failures, seals, txnBad, readBad, before, scan, 
                                stEpoch, stSealed, op, seq, tr, snap, rfs, 
                                rows, racked >>

FlSettle(self) == /\ pc[self] = "FlSettle"
                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Refresh",
                                                           pc        |->  "FlSettleDone",
                                                           before    |->  before[self],
                                                           scan      |->  scan[self] ] >>
                                                       \o stack[self]]
                  /\ before' = [before EXCEPT ![self] = 0]
                  /\ scan' = [scan EXCEPT ![self] = {}]
                  /\ pc' = [pc EXCEPT ![self] = "RfScan"]
                  /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                  marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                  mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                  gen, committing, commitLock, flushLock, 
                                  initLock, failed, rEpoch, rSealed, committed, 
                                  sealTried, acked, opsLeft, writesStarted, 
                                  evictions, interrupts, failures, seals, 
                                  txnBad, readBad, stEpoch, stSealed, sealing, 
                                  fEpoch, cut, ffs, sst, op, seq, tr, snap, 
                                  rfs, rows, racked >>

FlSettleDone(self) == /\ pc[self] = "FlSettleDone"
                      /\ flushLock' = None
                      /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                      /\ fEpoch' = [fEpoch EXCEPT ![self] = Head(stack[self]).fEpoch]
                      /\ cut' = [cut EXCEPT ![self] = Head(stack[self]).cut]
                      /\ ffs' = [ffs EXCEPT ![self] = Head(stack[self]).ffs]
                      /\ sst' = [sst EXCEPT ![self] = Head(stack[self]).sst]
                      /\ sealing' = [sealing EXCEPT ![self] = Head(stack[self]).sealing]
                      /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                      /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, 
                                      keyOf, marks, alarmSet, mEpoch, mSealed, 
                                      mL0Seq, mData, sealedAt, phase, pEpoch, 
                                      pSealed, cnt, gen, committing, 
                                      commitLock, initLock, failed, rEpoch, 
                                      rSealed, committed, sealTried, acked, 
                                      opsLeft, writesStarted, evictions, 
                                      interrupts, failures, seals, txnBad, 
                                      readBad, before, scan, stEpoch, stSealed, 
                                      op, seq, tr, snap, rfs, rows, racked >>

FlushThen(self) == FlLock(self) \/ FlReady(self) \/ FlMeta(self)
                      \/ FlUpload(self) \/ FlCommit(self) \/ FlMark(self)
                      \/ FlTrim(self) \/ FlRefresh(self) \/ FlDone(self)
                      \/ FlSettle(self) \/ FlSettleDone(self)

Idle(self) == /\ pc[self] = "Idle"
              /\ opsLeft > 0
              /\ opsLeft' = opsLeft - 1
              /\ \/ /\ "write" \in Ops /\ writesStarted < MaxSeq
                    /\ op' = [op EXCEPT ![self] = "write"]
                    /\ writesStarted' = writesStarted + 1
                    /\ pc' = [pc EXCEPT ![self] = "WLock"]
                    /\ UNCHANGED <<seals, tr>>
                 \/ /\ "txn" \in Ops /\ writesStarted < MaxSeq
                    /\ \E t \in 0..lastSeq:
                         tr' = [tr EXCEPT ![self] = t]
                    /\ op' = [op EXCEPT ![self] = "txn"]
                    /\ writesStarted' = writesStarted + 1
                    /\ pc' = [pc EXCEPT ![self] = "WLock"]
                    /\ seals' = seals
                 \/ /\ "flush" \in Ops
                    /\ op' = [op EXCEPT ![self] = "flush"]
                    /\ pc' = [pc EXCEPT ![self] = "FReady"]
                    /\ UNCHANGED <<writesStarted, seals, tr>>
                 \/ /\ "seal" \in Ops /\ seals < MaxSeals
                    /\ op' = [op EXCEPT ![self] = "seal"]
                    /\ seals' = seals + 1
                    /\ pc' = [pc EXCEPT ![self] = "SLock"]
                    /\ UNCHANGED <<writesStarted, tr>>
                 \/ /\ "probe" \in Ops
                    /\ op' = [op EXCEPT ![self] = "probe"]
                    /\ pc' = [pc EXCEPT ![self] = "PReady"]
                    /\ UNCHANGED <<writesStarted, seals, tr>>
              /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                              alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                              sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                              committing, commitLock, flushLock, initLock, 
                              failed, rEpoch, rSealed, committed, sealTried, 
                              acked, evictions, interrupts, failures, txnBad, 
                              readBad, stack, before, scan, stEpoch, stSealed, 
                              sealing, fEpoch, cut, ffs, sst, seq, snap, rfs, 
                              rows, racked >>

WLock(self) == /\ pc[self] = "WLock"
               /\ commitLock = None
               /\ commitLock' = self
               /\ pc' = [pc EXCEPT ![self] = "WReady"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                               sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                               committing, flushLock, initLock, failed, rEpoch, 
                               rSealed, committed, sealTried, acked, opsLeft, 
                               writesStarted, evictions, interrupts, failures, 
                               seals, txnBad, readBad, stack, before, scan, 
                               stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                               sst, op, seq, tr, snap, rfs, rows, racked >>

WReady(self) == /\ pc[self] = "WReady"
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "EnsureReady",
                                                         pc        |->  "WCheck",
                                                         stEpoch   |->  stEpoch[self],
                                                         stSealed  |->  stSealed[self] ] >>
                                                     \o stack[self]]
                /\ stEpoch' = [stEpoch EXCEPT ![self] = 0]
                /\ stSealed' = [stSealed EXCEPT ![self] = FALSE]
                /\ pc' = [pc EXCEPT ![self] = "ErCheck"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, before, scan, sealing, fEpoch, cut, 
                                ffs, sst, op, seq, tr, snap, rfs, rows, racked >>

WCheck(self) == /\ pc[self] = "WCheck"
                /\ IF failed[self] \/ rSealed[self]
                      THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                      ELSE /\ IF op[self] = "write"
                                 THEN /\ pc' = [pc EXCEPT ![self] = "WAlarmEarly"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "THorizon"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, stack, before, scan, stEpoch, 
                                stSealed, sealing, fEpoch, cut, ffs, sst, op, 
                                seq, tr, snap, rfs, rows, racked >>

THorizon(self) == /\ pc[self] = "THorizon"
                  /\ IF RetainedFrom > tr[self] \/ tr[self] > lastSeq
                        THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "TQuery"]
                  /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                  marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                  mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                  gen, committing, commitLock, flushLock, 
                                  initLock, failed, rEpoch, rSealed, committed, 
                                  sealTried, acked, opsLeft, writesStarted, 
                                  evictions, interrupts, failures, seals, 
                                  txnBad, readBad, stack, before, scan, 
                                  stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                                  sst, op, seq, tr, snap, rfs, rows, racked >>

TQuery(self) == /\ pc[self] = "TQuery"
                /\ IF \E s \in mem : s > tr[self] /\ keyOf[s] = TK
                      THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "THorizonAgain"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, stack, before, scan, stEpoch, 
                                stSealed, sealing, fEpoch, cut, ffs, sst, op, 
                                seq, tr, snap, rfs, rows, racked >>

THorizonAgain(self) == /\ pc[self] = "THorizonAgain"
                       /\ IF Broken # "noHorizonReread" /\ RetainedFrom > tr[self]
                             THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                             ELSE /\ pc' = [pc EXCEPT ![self] = "WAlarmEarly"]
                       /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, 
                                       keyOf, marks, alarmSet, mEpoch, mSealed, 
                                       mL0Seq, mData, sealedAt, phase, pEpoch, 
                                       pSealed, cnt, gen, committing, 
                                       commitLock, flushLock, initLock, failed, 
                                       rEpoch, rSealed, committed, sealTried, 
                                       acked, opsLeft, writesStarted, 
                                       evictions, interrupts, failures, seals, 
                                       txnBad, readBad, stack, before, scan, 
                                       stEpoch, stSealed, sealing, fEpoch, cut, 
                                       ffs, sst, op, seq, tr, snap, rfs, rows, 
                                       racked >>

WAlarmEarly(self) == /\ pc[self] = "WAlarmEarly"
                     /\ IF Broken = "alarmBeforeMark" /\ ~alarmSet
                           THEN /\ alarmSet' = TRUE
                           ELSE /\ TRUE
                                /\ UNCHANGED alarmSet
                     /\ pc' = [pc EXCEPT ![self] = "WMark"]
                     /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                     marks, mEpoch, mSealed, mL0Seq, mData, 
                                     sealedAt, phase, pEpoch, pSealed, cnt, 
                                     gen, committing, commitLock, flushLock, 
                                     initLock, failed, rEpoch, rSealed, 
                                     committed, sealTried, acked, opsLeft, 
                                     writesStarted, evictions, interrupts, 
                                     failures, seals, txnBad, readBad, stack, 
                                     before, scan, stEpoch, stSealed, sealing, 
                                     fEpoch, cut, ffs, sst, op, seq, tr, snap, 
                                     rfs, rows, racked >>

WMark(self) == /\ pc[self] = "WMark"
               /\ /\ committing' = TRUE
                  /\ gen' = gen + 1
               /\ pc' = [pc EXCEPT ![self] = "WAlarm"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                               sealedAt, phase, pEpoch, pSealed, cnt, 
                               commitLock, flushLock, initLock, failed, rEpoch, 
                               rSealed, committed, sealTried, acked, opsLeft, 
                               writesStarted, evictions, interrupts, failures, 
                               seals, txnBad, readBad, stack, before, scan, 
                               stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                               sst, op, seq, tr, snap, rfs, rows, racked >>

WAlarm(self) == /\ pc[self] = "WAlarm"
                /\ IF Broken \notin {"alarmAfterCommit", "alarmBeforeMark"} /\ ~alarmSet
                      THEN /\ alarmSet' = TRUE
                      ELSE /\ TRUE
                           /\ UNCHANGED alarmSet
                /\ pc' = [pc EXCEPT ![self] = "WSql"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, mEpoch, mSealed, mL0Seq, mData, 
                                sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                                committing, commitLock, flushLock, initLock, 
                                failed, rEpoch, rSealed, committed, sealTried, 
                                acked, opsLeft, writesStarted, evictions, 
                                interrupts, failures, seals, txnBad, readBad, 
                                stack, before, scan, stEpoch, stSealed, 
                                sealing, fEpoch, cut, ffs, sst, op, seq, tr, 
                                snap, rfs, rows, racked >>

WSql(self) == /\ pc[self] = "WSql"
              /\ IF op[self] = "txn"
                    THEN /\ IF \E s \in (tr[self] + 1)..lastSeq : keyOf[s] = TK
                               THEN /\ txnBad' = TRUE
                               ELSE /\ TRUE
                                    /\ UNCHANGED txnBad
                         /\ keyOf' = [keyOf EXCEPT ![lastSeq + 1] = TK]
                    ELSE /\ \E k \in Keys:
                              keyOf' = [keyOf EXCEPT ![lastSeq + 1] = k]
                         /\ UNCHANGED txnBad
              /\ seq' = [seq EXCEPT ![self] = lastSeq + 1]
              /\ /\ lastSeq' = lastSeq + 1
                 /\ mem' = (mem \cup {lastSeq + 1})
              /\ pc' = [pc EXCEPT ![self] = "WEnd"]
              /\ UNCHANGED << flushedSeq, retained, marks, alarmSet, mEpoch, 
                              mSealed, mL0Seq, mData, sealedAt, phase, pEpoch, 
                              pSealed, cnt, gen, committing, commitLock, 
                              flushLock, initLock, failed, rEpoch, rSealed, 
                              committed, sealTried, acked, opsLeft, 
                              writesStarted, evictions, interrupts, failures, 
                              seals, readBad, stack, before, scan, stEpoch, 
                              stSealed, sealing, fEpoch, cut, ffs, sst, op, tr, 
                              snap, rfs, rows, racked >>

WEnd(self) == /\ pc[self] = "WEnd"
              /\ /\ cnt' = (cnt \cup {seq[self]})
                 /\ committing' = FALSE
                 /\ gen' = gen + 1
              /\ pc' = [pc EXCEPT ![self] = "WDone"]
              /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                              alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                              sealedAt, phase, pEpoch, pSealed, commitLock, 
                              flushLock, initLock, failed, rEpoch, rSealed, 
                              committed, sealTried, acked, opsLeft, 
                              writesStarted, evictions, interrupts, failures, 
                              seals, txnBad, readBad, stack, before, scan, 
                              stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                              sst, op, seq, tr, snap, rfs, rows, racked >>

WDone(self) == /\ pc[self] = "WDone"
               /\ commitLock' = None
               /\ IF cnt # {} \/ (committing /\ Broken \notin {"alarmBeforeMark", "schedulerIgnoresCommit"})
                     THEN /\ alarmSet' = TRUE
                     ELSE /\ TRUE
                          /\ UNCHANGED alarmSet
               /\ acked' = (acked \cup {seq[self]})
               /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, mEpoch, mSealed, mL0Seq, mData, sealedAt, 
                               phase, pEpoch, pSealed, cnt, gen, committing, 
                               flushLock, initLock, failed, rEpoch, rSealed, 
                               committed, sealTried, opsLeft, writesStarted, 
                               evictions, interrupts, failures, seals, txnBad, 
                               readBad, stack, before, scan, stEpoch, stSealed, 
                               sealing, fEpoch, cut, ffs, sst, op, seq, tr, 
                               snap, rfs, rows, racked >>

FReady(self) == /\ pc[self] = "FReady"
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "EnsureReady",
                                                         pc        |->  "FGo",
                                                         stEpoch   |->  stEpoch[self],
                                                         stSealed  |->  stSealed[self] ] >>
                                                     \o stack[self]]
                /\ stEpoch' = [stEpoch EXCEPT ![self] = 0]
                /\ stSealed' = [stSealed EXCEPT ![self] = FALSE]
                /\ pc' = [pc EXCEPT ![self] = "ErCheck"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, before, scan, sealing, fEpoch, cut, 
                                ffs, sst, op, seq, tr, snap, rfs, rows, racked >>

FGo(self) == /\ pc[self] = "FGo"
             /\ IF failed[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                        /\ UNCHANGED << stack, sealing, fEpoch, cut, ffs, sst >>
                   ELSE /\ /\ sealing' = [sealing EXCEPT ![self] = FALSE]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FlushThen",
                                                                    pc        |->  "FDone",
                                                                    fEpoch    |->  fEpoch[self],
                                                                    cut       |->  cut[self],
                                                                    ffs       |->  ffs[self],
                                                                    sst       |->  sst[self],
                                                                    sealing   |->  sealing[self] ] >>
                                                                \o stack[self]]
                        /\ fEpoch' = [fEpoch EXCEPT ![self] = 0]
                        /\ cut' = [cut EXCEPT ![self] = 0]
                        /\ ffs' = [ffs EXCEPT ![self] = 0]
                        /\ sst' = [sst EXCEPT ![self] = {}]
                        /\ pc' = [pc EXCEPT ![self] = "FlLock"]
             /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                             alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                             sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                             committing, commitLock, flushLock, initLock, 
                             failed, rEpoch, rSealed, committed, sealTried, 
                             acked, opsLeft, writesStarted, evictions, 
                             interrupts, failures, seals, txnBad, readBad, 
                             before, scan, stEpoch, stSealed, op, seq, tr, 
                             snap, rfs, rows, racked >>

FDone(self) == /\ pc[self] = "FDone"
               /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                               sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                               committing, commitLock, flushLock, initLock, 
                               failed, rEpoch, rSealed, committed, sealTried, 
                               acked, opsLeft, writesStarted, evictions, 
                               interrupts, failures, seals, txnBad, readBad, 
                               stack, before, scan, stEpoch, stSealed, sealing, 
                               fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, 
                               rows, racked >>

SLock(self) == /\ pc[self] = "SLock"
               /\ commitLock = None
               /\ commitLock' = self
               /\ pc' = [pc EXCEPT ![self] = "SReady"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                               sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                               committing, flushLock, initLock, failed, rEpoch, 
                               rSealed, committed, sealTried, acked, opsLeft, 
                               writesStarted, evictions, interrupts, failures, 
                               seals, txnBad, readBad, stack, before, scan, 
                               stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                               sst, op, seq, tr, snap, rfs, rows, racked >>

SReady(self) == /\ pc[self] = "SReady"
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "EnsureReady",
                                                         pc        |->  "SCheck",
                                                         stEpoch   |->  stEpoch[self],
                                                         stSealed  |->  stSealed[self] ] >>
                                                     \o stack[self]]
                /\ stEpoch' = [stEpoch EXCEPT ![self] = 0]
                /\ stSealed' = [stSealed EXCEPT ![self] = FALSE]
                /\ pc' = [pc EXCEPT ![self] = "ErCheck"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, before, scan, sealing, fEpoch, cut, 
                                ffs, sst, op, seq, tr, snap, rfs, rows, racked >>

SCheck(self) == /\ pc[self] = "SCheck"
                /\ IF failed[self]
                      THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                           /\ op' = op
                      ELSE /\ IF rSealed[self]
                                 THEN /\ op' = [op EXCEPT ![self] = "sealed"]
                                      /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "SFlush"]
                                      /\ op' = op
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, stack, before, scan, stEpoch, 
                                stSealed, sealing, fEpoch, cut, ffs, sst, seq, 
                                tr, snap, rfs, rows, racked >>

SFlush(self) == /\ pc[self] = "SFlush"
                /\ /\ sealing' = [sealing EXCEPT ![self] = TRUE]
                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FlushThen",
                                                            pc        |->  "SAfter",
                                                            fEpoch    |->  fEpoch[self],
                                                            cut       |->  cut[self],
                                                            ffs       |->  ffs[self],
                                                            sst       |->  sst[self],
                                                            sealing   |->  sealing[self] ] >>
                                                        \o stack[self]]
                /\ fEpoch' = [fEpoch EXCEPT ![self] = 0]
                /\ cut' = [cut EXCEPT ![self] = 0]
                /\ ffs' = [ffs EXCEPT ![self] = 0]
                /\ sst' = [sst EXCEPT ![self] = {}]
                /\ pc' = [pc EXCEPT ![self] = "FlLock"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, before, scan, stEpoch, stSealed, op, 
                                seq, tr, snap, rfs, rows, racked >>

SAfter(self) == /\ pc[self] = "SAfter"
                /\ IF failed[self]
                      THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                      ELSE /\ IF committed[self]
                                 THEN /\ pc' = [pc EXCEPT ![self] = "SSet"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "SCommit"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, stack, before, scan, stEpoch, 
                                stSealed, sealing, fEpoch, cut, ffs, sst, op, 
                                seq, tr, snap, rfs, rows, racked >>

SCommit(self) == /\ pc[self] = "SCommit"
                 /\ sealTried' = [sealTried EXCEPT ![self] = TRUE]
                 /\ \/ /\ CanFail
                       /\ failures' = failures + 1
                       /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                       /\ UNCHANGED <<mEpoch, mSealed, sealedAt>>
                    \/ /\ IF mSealed
                             THEN /\ TRUE
                                  /\ pc' = [pc EXCEPT ![self] = "SSet"]
                                  /\ UNCHANGED << mEpoch, mSealed, sealedAt, 
                                                  failures >>
                             ELSE /\ IF mEpoch # rEpoch[self]
                                        THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                                             /\ UNCHANGED << mEpoch, mSealed, 
                                                             sealedAt, 
                                                             failures >>
                                        ELSE /\ /\ mEpoch' = mEpoch + 1
                                                /\ mSealed' = TRUE
                                                /\ sealedAt' = mL0Seq
                                             /\ \/ /\ CanFail
                                                   /\ failures' = failures + 1
                                                   /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                                                \/ /\ TRUE
                                                   /\ pc' = [pc EXCEPT ![self] = "SSet"]
                                                   /\ UNCHANGED failures
                 /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                 marks, alarmSet, mL0Seq, mData, phase, pEpoch, 
                                 pSealed, cnt, gen, committing, commitLock, 
                                 flushLock, initLock, failed, rEpoch, rSealed, 
                                 committed, acked, opsLeft, writesStarted, 
                                 evictions, interrupts, seals, txnBad, readBad, 
                                 stack, before, scan, stEpoch, stSealed, 
                                 sealing, fEpoch, cut, ffs, sst, op, seq, tr, 
                                 snap, rfs, rows, racked >>

SSet(self) == /\ pc[self] = "SSet"
              /\ /\ pEpoch' = mEpoch
                 /\ pSealed' = TRUE
                 /\ phase' = "ready"
              /\ op' = [op EXCEPT ![self] = "sealed"]
              /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
              /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                              alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                              sealedAt, cnt, gen, committing, commitLock, 
                              flushLock, initLock, failed, rEpoch, rSealed, 
                              committed, sealTried, acked, opsLeft, 
                              writesStarted, evictions, interrupts, failures, 
                              seals, txnBad, readBad, stack, before, scan, 
                              stEpoch, stSealed, sealing, fEpoch, cut, ffs, 
                              sst, seq, tr, snap, rfs, rows, racked >>

PReady(self) == /\ pc[self] = "PReady"
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "EnsureReady",
                                                         pc        |->  "PRead",
                                                         stEpoch   |->  stEpoch[self],
                                                         stSealed  |->  stSealed[self] ] >>
                                                     \o stack[self]]
                /\ stEpoch' = [stEpoch EXCEPT ![self] = 0]
                /\ stSealed' = [stSealed EXCEPT ![self] = FALSE]
                /\ pc' = [pc EXCEPT ![self] = "ErCheck"]
                /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                mData, sealedAt, phase, pEpoch, pSealed, cnt, 
                                gen, committing, commitLock, flushLock, 
                                initLock, failed, rEpoch, rSealed, committed, 
                                sealTried, acked, opsLeft, writesStarted, 
                                evictions, interrupts, failures, seals, txnBad, 
                                readBad, before, scan, sealing, fEpoch, cut, 
                                ffs, sst, op, seq, tr, snap, rfs, rows, racked >>

PRead(self) == /\ pc[self] = "PRead"
               /\ IF failed[self]
                     THEN /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                          /\ UNCHANGED << snap, rfs, rows, racked >>
                     ELSE /\ /\ racked' = [racked EXCEPT ![self] = acked]
                             /\ rfs' = [rfs EXCEPT ![self] = flushedSeq]
                             /\ rows' = [rows EXCEPT ![self] = {s \in mem : flushedSeq < s /\ s <= lastSeq}]
                             /\ snap' = [snap EXCEPT ![self] = lastSeq]
                          /\ pc' = [pc EXCEPT ![self] = "PManifest"]
               /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                               marks, alarmSet, mEpoch, mSealed, mL0Seq, mData, 
                               sealedAt, phase, pEpoch, pSealed, cnt, gen, 
                               committing, commitLock, flushLock, initLock, 
                               failed, rEpoch, rSealed, committed, sealTried, 
                               acked, opsLeft, writesStarted, evictions, 
                               interrupts, failures, seals, txnBad, readBad, 
                               stack, before, scan, stEpoch, stSealed, sealing, 
                               fEpoch, cut, ffs, sst, op, seq, tr >>

PManifest(self) == /\ pc[self] = "PManifest"
                   /\ IF rfs[self] <= mL0Seq /\ mL0Seq <= snap[self]
                         THEN /\ IF ~(racked[self] \subseteq rows[self] \cup {s \in mData : s <= snap[self]})
                                    THEN /\ readBad' = TRUE
                                    ELSE /\ TRUE
                                         /\ UNCHANGED readBad
                              /\ pc' = [pc EXCEPT ![self] = "Cleanup"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "PReady"]
                              /\ UNCHANGED readBad
                   /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                   marks, alarmSet, mEpoch, mSealed, mL0Seq, 
                                   mData, sealedAt, phase, pEpoch, pSealed, 
                                   cnt, gen, committing, commitLock, flushLock, 
                                   initLock, failed, rEpoch, rSealed, 
                                   committed, sealTried, acked, opsLeft, 
                                   writesStarted, evictions, interrupts, 
                                   failures, seals, txnBad, stack, before, 
                                   scan, stEpoch, stSealed, sealing, fEpoch, 
                                   cut, ffs, sst, op, seq, tr, snap, rfs, rows, 
                                   racked >>

Cleanup(self) == /\ pc[self] = "Cleanup"
                 /\ IF /\ op[self] = "seal" /\ commitLock = self /\ Broken # "sealKeepsPhase" /\ phase = "ready"
                       /\ (sealTried[self] \/ Broken \in {"sealForgetsUntried", "beforeD115"})
                       THEN /\ phase' = "uninit"
                       ELSE /\ TRUE
                            /\ phase' = phase
                 /\ IF commitLock = self
                       THEN /\ commitLock' = None
                       ELSE /\ TRUE
                            /\ UNCHANGED commitLock
                 /\ IF flushLock = self
                       THEN /\ flushLock' = None
                       ELSE /\ TRUE
                            /\ UNCHANGED flushLock
                 /\ IF initLock = self
                       THEN /\ initLock' = None
                       ELSE /\ TRUE
                            /\ UNCHANGED initLock
                 /\ IF op[self] \in {"write", "txn"} /\ Broken # "interruptedWrite"
                       THEN /\ IF cnt # {} \/ (committing /\ Broken \notin {"alarmBeforeMark", "schedulerIgnoresCommit"})
                                  THEN /\ alarmSet' = TRUE
                                  ELSE /\ TRUE
                                       /\ UNCHANGED alarmSet
                       ELSE /\ TRUE
                            /\ UNCHANGED alarmSet
                 /\ /\ failed' = [failed EXCEPT ![self] = FALSE]
                    /\ sealTried' = [sealTried EXCEPT ![self] = FALSE]
                 /\ /\ op' = [op EXCEPT ![self] = "none"]
                    /\ racked' = [racked EXCEPT ![self] = {}]
                    /\ rfs' = [rfs EXCEPT ![self] = 0]
                    /\ rows' = [rows EXCEPT ![self] = {}]
                    /\ seq' = [seq EXCEPT ![self] = 0]
                    /\ snap' = [snap EXCEPT ![self] = 0]
                    /\ tr' = [tr EXCEPT ![self] = 0]
                 /\ pc' = [pc EXCEPT ![self] = "Idle"]
                 /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, 
                                 marks, mEpoch, mSealed, mL0Seq, mData, 
                                 sealedAt, pEpoch, pSealed, cnt, gen, 
                                 committing, rEpoch, rSealed, committed, acked, 
                                 opsLeft, writesStarted, evictions, interrupts, 
                                 failures, seals, txnBad, readBad, stack, 
                                 before, scan, stEpoch, stSealed, sealing, 
                                 fEpoch, cut, ffs, sst >>

r(self) == Idle(self) \/ WLock(self) \/ WReady(self) \/ WCheck(self)
              \/ THorizon(self) \/ TQuery(self) \/ THorizonAgain(self)
              \/ WAlarmEarly(self) \/ WMark(self) \/ WAlarm(self)
              \/ WSql(self) \/ WEnd(self) \/ WDone(self) \/ FReady(self)
              \/ FGo(self) \/ FDone(self) \/ SLock(self) \/ SReady(self)
              \/ SCheck(self) \/ SFlush(self) \/ SAfter(self)
              \/ SCommit(self) \/ SSet(self) \/ PReady(self) \/ PRead(self)
              \/ PManifest(self) \/ Cleanup(self)

AIdle == /\ pc[A] = "AIdle"
         /\ alarmSet
         /\ alarmSet' = FALSE
         /\ pc' = [pc EXCEPT ![A] = "AMeta"]
         /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                         mEpoch, mSealed, mL0Seq, mData, sealedAt, phase, 
                         pEpoch, pSealed, cnt, gen, committing, commitLock, 
                         flushLock, initLock, failed, rEpoch, rSealed, 
                         committed, sealTried, acked, opsLeft, writesStarted, 
                         evictions, interrupts, failures, seals, txnBad, 
                         readBad, stack, before, scan, stEpoch, stSealed, 
                         sealing, fEpoch, cut, ffs, sst, op, seq, tr, snap, 
                         rfs, rows, racked >>

AMeta == /\ pc[A] = "AMeta"
         /\ \/ /\ CanFail
               /\ failures' = failures + 1
               /\ IF Broken # "noAlarmRetry"
                     THEN /\ alarmSet' = TRUE
                     ELSE /\ TRUE
                          /\ UNCHANGED alarmSet
               /\ pc' = [pc EXCEPT ![A] = "AIdle"]
            \/ /\ TRUE
               /\ pc' = [pc EXCEPT ![A] = "AReady"]
               /\ UNCHANGED <<alarmSet, failures>>
         /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                         mEpoch, mSealed, mL0Seq, mData, sealedAt, phase, 
                         pEpoch, pSealed, cnt, gen, committing, commitLock, 
                         flushLock, initLock, failed, rEpoch, rSealed, 
                         committed, sealTried, acked, opsLeft, writesStarted, 
                         evictions, interrupts, seals, txnBad, readBad, stack, 
                         before, scan, stEpoch, stSealed, sealing, fEpoch, cut, 
                         ffs, sst, op, seq, tr, snap, rfs, rows, racked >>

AReady == /\ pc[A] = "AReady"
          /\ stack' = [stack EXCEPT ![A] = << [ procedure |->  "EnsureReady",
                                                pc        |->  "ADue",
                                                stEpoch   |->  stEpoch[A],
                                                stSealed  |->  stSealed[A] ] >>
                                            \o stack[A]]
          /\ stEpoch' = [stEpoch EXCEPT ![A] = 0]
          /\ stSealed' = [stSealed EXCEPT ![A] = FALSE]
          /\ pc' = [pc EXCEPT ![A] = "ErCheck"]
          /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                          alarmSet, mEpoch, mSealed, mL0Seq, mData, sealedAt, 
                          phase, pEpoch, pSealed, cnt, gen, committing, 
                          commitLock, flushLock, initLock, failed, rEpoch, 
                          rSealed, committed, sealTried, acked, opsLeft, 
                          writesStarted, evictions, interrupts, failures, 
                          seals, txnBad, readBad, before, scan, sealing, 
                          fEpoch, cut, ffs, sst, op, seq, tr, snap, rfs, rows, 
                          racked >>

ADue == /\ pc[A] = "ADue"
        /\ IF failed[A]
              THEN /\ pc' = [pc EXCEPT ![A] = "AFail"]
                   /\ UNCHANGED << stack, sealing, fEpoch, cut, ffs, sst >>
              ELSE /\ IF cnt # {}
                         THEN /\ /\ sealing' = [sealing EXCEPT ![A] = FALSE]
                                 /\ stack' = [stack EXCEPT ![A] = << [ procedure |->  "FlushThen",
                                                                       pc        |->  "ASched",
                                                                       fEpoch    |->  fEpoch[A],
                                                                       cut       |->  cut[A],
                                                                       ffs       |->  ffs[A],
                                                                       sst       |->  sst[A],
                                                                       sealing   |->  sealing[A] ] >>
                                                                   \o stack[A]]
                              /\ fEpoch' = [fEpoch EXCEPT ![A] = 0]
                              /\ cut' = [cut EXCEPT ![A] = 0]
                              /\ ffs' = [ffs EXCEPT ![A] = 0]
                              /\ sst' = [sst EXCEPT ![A] = {}]
                              /\ pc' = [pc EXCEPT ![A] = "FlLock"]
                         ELSE /\ pc' = [pc EXCEPT ![A] = "ASched"]
                              /\ UNCHANGED << stack, sealing, fEpoch, cut, ffs, 
                                              sst >>
        /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                        alarmSet, mEpoch, mSealed, mL0Seq, mData, sealedAt, 
                        phase, pEpoch, pSealed, cnt, gen, committing, 
                        commitLock, flushLock, initLock, failed, rEpoch, 
                        rSealed, committed, sealTried, acked, opsLeft, 
                        writesStarted, evictions, interrupts, failures, seals, 
                        txnBad, readBad, before, scan, stEpoch, stSealed, op, 
                        seq, tr, snap, rfs, rows, racked >>

ASched == /\ pc[A] = "ASched"
          /\ IF failed[A]
                THEN /\ pc' = [pc EXCEPT ![A] = "AFail"]
                     /\ UNCHANGED alarmSet
                ELSE /\ IF cnt # {} \/ (committing /\ Broken \notin {"alarmBeforeMark", "schedulerIgnoresCommit"})
                           THEN /\ alarmSet' = TRUE
                           ELSE /\ TRUE
                                /\ UNCHANGED alarmSet
                     /\ pc' = [pc EXCEPT ![A] = "AEnd"]
          /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                          mEpoch, mSealed, mL0Seq, mData, sealedAt, phase, 
                          pEpoch, pSealed, cnt, gen, committing, commitLock, 
                          flushLock, initLock, failed, rEpoch, rSealed, 
                          committed, sealTried, acked, opsLeft, writesStarted, 
                          evictions, interrupts, failures, seals, txnBad, 
                          readBad, stack, before, scan, stEpoch, stSealed, 
                          sealing, fEpoch, cut, ffs, sst, op, seq, tr, snap, 
                          rfs, rows, racked >>

AFail == /\ pc[A] = "AFail"
         /\ IF phase # "fenced"
               THEN /\ alarmSet' = TRUE
               ELSE /\ TRUE
                    /\ UNCHANGED alarmSet
         /\ pc' = [pc EXCEPT ![A] = "AEnd"]
         /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                         mEpoch, mSealed, mL0Seq, mData, sealedAt, phase, 
                         pEpoch, pSealed, cnt, gen, committing, commitLock, 
                         flushLock, initLock, failed, rEpoch, rSealed, 
                         committed, sealTried, acked, opsLeft, writesStarted, 
                         evictions, interrupts, failures, seals, txnBad, 
                         readBad, stack, before, scan, stEpoch, stSealed, 
                         sealing, fEpoch, cut, ffs, sst, op, seq, tr, snap, 
                         rfs, rows, racked >>

AEnd == /\ pc[A] = "AEnd"
        /\ IF flushLock = A
              THEN /\ flushLock' = None
              ELSE /\ TRUE
                   /\ UNCHANGED flushLock
        /\ IF initLock = A
              THEN /\ initLock' = None
              ELSE /\ TRUE
                   /\ UNCHANGED initLock
        /\ failed' = [failed EXCEPT ![A] = FALSE]
        /\ pc' = [pc EXCEPT ![A] = "AIdle"]
        /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, 
                        alarmSet, mEpoch, mSealed, mL0Seq, mData, sealedAt, 
                        phase, pEpoch, pSealed, cnt, gen, committing, 
                        commitLock, rEpoch, rSealed, committed, sealTried, 
                        acked, opsLeft, writesStarted, evictions, interrupts, 
                        failures, seals, txnBad, readBad, stack, before, scan, 
                        stEpoch, stSealed, sealing, fEpoch, cut, ffs, sst, op, 
                        seq, tr, snap, rfs, rows, racked >>

alarm == AIdle \/ AMeta \/ AReady \/ ADue \/ ASched \/ AFail \/ AEnd

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == alarm
           \/ (\E self \in ProcSet:  \/ Refresh(self) \/ EnsureReady(self)
                                     \/ FlushThen(self))
           \/ (\E self \in Req: r(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

---------------------------------------------------------------------------
\* Every procedure and request local back at its initial value.
ResetLocals(P) ==
  /\ before' = [p \in ProcSet |-> IF p \in P THEN 0 ELSE before[p]]
  /\ scan' = [p \in ProcSet |-> IF p \in P THEN {} ELSE scan[p]]
  /\ stEpoch' = [p \in ProcSet |-> IF p \in P THEN 0 ELSE stEpoch[p]]
  /\ stSealed' = [p \in ProcSet |-> IF p \in P THEN FALSE ELSE stSealed[p]]
  /\ sealing' = [p \in ProcSet |-> IF p \in P THEN defaultInitValue ELSE sealing[p]]
  /\ fEpoch' = [p \in ProcSet |-> IF p \in P THEN 0 ELSE fEpoch[p]]
  /\ cut' = [p \in ProcSet |-> IF p \in P THEN 0 ELSE cut[p]]
  /\ ffs' = [p \in ProcSet |-> IF p \in P THEN 0 ELSE ffs[p]]
  /\ sst' = [p \in ProcSet |-> IF p \in P THEN {} ELSE sst[p]]
  /\ stack' = [p \in ProcSet |-> IF p \in P THEN << >> ELSE stack[p]]

\* Eviction or a crash of the isolate: every in-memory value (the phase and its manifest view, the
\* counters, the locks, every request in flight) is lost; SQLite, R2, and the alarm stay. An alarm
\* run cut short is run again by the platform.
Evict ==
  /\ evictions < MaxEvictions
  /\ evictions' = evictions + 1
  /\ phase' = "uninit" /\ pEpoch' = 0 /\ pSealed' = FALSE
  /\ cnt' = {} /\ gen' = 0 /\ committing' = FALSE
  /\ commitLock' = None /\ flushLock' = None /\ initLock' = None
  /\ failed' = [p \in ProcSet |-> FALSE]
  /\ rEpoch' = [p \in ProcSet |-> 0]
  /\ rSealed' = [p \in ProcSet |-> FALSE]
  /\ committed' = [p \in ProcSet |-> FALSE]
  /\ sealTried' = [p \in ProcSet |-> FALSE]
  /\ alarmSet' = (alarmSet \/ pc[A] # "AIdle")
  /\ pc' = [p \in ProcSet |-> IF p = A THEN "AIdle" ELSE "Idle"]
  /\ op' = [p \in Req |-> "none"] /\ seq' = [p \in Req |-> 0] /\ tr' = [p \in Req |-> 0]
  /\ snap' = [p \in Req |-> 0] /\ rfs' = [p \in Req |-> 0]
  /\ rows' = [p \in Req |-> {}] /\ racked' = [p \in Req |-> {}]
  /\ ResetLocals(ProcSet)
  /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, mEpoch, mSealed, mL0Seq,
                  mData, sealedAt, acked, opsLeft, writesStarted, interrupts, failures, seals, txnBad,
                  readBad >>

\* The commit's mark, SQLite commit, and the counters' update are one uninterruptible region.
Uninterruptible == IF Broken = "interruptedWrite" THEN {} ELSE {"WAlarm", "WSql", "WEnd"}

\* A request's caller hangs up at an await: its fiber is interrupted and runs its finalizers
\* (Cleanup: the locks, the seal's onExit, the write's ensuring(scheduleFlush)).
Interrupt(p) ==
  /\ interrupts < MaxInterrupts
  /\ pc[p] \notin {"Idle", "Cleanup"} \cup Uninterruptible
  /\ interrupts' = interrupts + 1
  /\ pc' = [pc EXCEPT ![p] = "Cleanup"]
  /\ ResetLocals({p})
  /\ UNCHANGED << lastSeq, flushedSeq, retained, mem, keyOf, marks, alarmSet, mEpoch, mSealed,
                  mL0Seq, mData, sealedAt, phase, pEpoch, pSealed, cnt, gen, committing,
                  commitLock, flushLock, initLock, failed, rEpoch, rSealed, committed, sealTried, acked,
                  writesStarted, opsLeft, evictions, failures, seals, txnBad, readBad,
                  op, seq, tr, snap, rfs, rows, racked >>

Symmetry == Permutations(Req)

FullNext == Next \/ Evict \/ \E p \in Req : Interrupt(p)

FullSpec == Init /\ [][FullNext]_vars

\* Every started step of a request runs (lock waits strongly fair); the alarm fires once due
\* (time is abstract) and its run proceeds. Starting a request is never forced.
ProcStep(p) == Refresh(p) \/ EnsureReady(p) \/ FlushThen(p)
LiveSpec ==
  /\ FullSpec
  /\ \A p \in Req : SF_vars((r(p) \/ ProcStep(p)) /\ pc[p] # "Idle")
  /\ SF_vars(alarm \/ ProcStep(A))

---------------------------------------------------------------------------
\* No acknowledged write is lost: it is in SQLite or in the newest manifest's SSTs.
NoAckedWriteLost == acked \subseteq mem \cup mData

\* A Latest read returns every write acknowledged before its probe.
LatestSeesAcked == ~readBad

\* A transaction commits only when no commit after its readSeq wrote its key.
Serializable == ~txnBad

\* No write is acknowledged after the seal's manifest commit: every acknowledged write is in the
\* sealed manifest.
NoAckAfterSeal == mSealed => \A s \in acked : s <= sealedAt

\* Once no seal is running, a sealed manifest means a sealed (or restarting) writer, never an open
\* one.
SealNotHalf == (mSealed /\ phase = "ready" /\ \A p \in Req : op[p] # "seal") => pSealed

\* The counters are never below SQLite while the writer is ready, except inside a commit between
\* its SQLite commit and its counters' update.
CountersCover ==
  (phase = "ready" /\ \A p \in Req : pc[p] # "WEnd") => Unflushed \subseteq cnt

\* With nothing running, an unflushed row has its alarm, after any eviction too (D131).
Quiescent == (\A p \in Req : pc[p] = "Idle") /\ pc[A] = "AIdle"
AlarmCovers == (Quiescent /\ Unflushed # {}) => alarmSet

\* One object runs one writer: nothing but a newer instance of itself can fence it, so this
\* instance must never mark itself fenced (a fenced instance refuses every request until evicted).
NeverSelfFenced == phase # "fenced"

\* Liveness: every acknowledged write reaches the manifest.
AckedFlushed == \A s \in 1..MaxSeq : (s \in acked) ~> (s \in mData)

\* Liveness: the alarm stops firing (no alarm that fires forever without progress, D74).
AlarmQuiets == <>[](~alarmSet /\ pc[A] = "AIdle")

=============================================================================
