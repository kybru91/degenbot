//! THE one detached/in-cycle solve-arm machine (P37YJG, epic SRQEK5 lineage).
//!
//! The detached/in-cycle solve arm of [`crate::arb_engine::ArbitrageEngine`]
//! is ONE conceptual per-cycle machine. This module is its single owner: the
//! per-cycle states (`Unopened → Open → Saturated`), the merge pipe
//! open/take, the outstanding-gauge pair, the seq counters, the
//! outcome-ledger door, the disposition counters, the fan-in undercount
//! tripwire, and the ONE sidecar spawn. The construction-stamped
//! `detached_solving` boot flag is NOT machine state — it reads into
//! [`DetachedCycle::begin_cycle`] as the stance input.
//!
//! # States
//!
//! - [`CycleArm::Unopened`] — no detached cycle ever issued; the pipe is
//!   closed. The stance-off (or never-yet-detached) engine lives here.
//! - [`CycleArm::Open`] — detached cycles issuing; the merge pipe is open
//!   and exactly one `Receiver` parks until the sidecar takes it.
//! - [`CycleArm::Saturated`] — the in-flight cap ([`DETACHED_INFLIGHT_CAP`])
//!   was reached at the last begin: the cycle DEGRADED to the in-cycle arm
//!   (backpressure via fallback — a lagging sidecar must not accumulate
//!   unbounded stragglers). The cap verdict is re-derived from the live
//!   gauge at every begin; the state records the last verdict.
//!
//! # Transition discipline
//!
//! One total legal-transition table ([`transition`]) + the sized
//! [`ALL_CYCLE_ARMS`] const + the conformance walk in the test module
//! (house pattern: `degenbot-workers` `slot.rs` T1–T9 +
//! `bot_core::stage_handlers::ALL_STAGES`). Illegal sequences become typed
//! rejections ([`RejectedTransition`]) where callers can react; the panics
//! and aborts that exist today stay verbatim (ADR-042 §10 deadlock-ledger /
//! loud-stop discipline: stranded merge pipe, vanished pipe — same log
//! wording, same `std::process::abort`).
//!
//! # Lock order (preserved verbatim)
//!
//! The engine mutex stays the OUTER lock; the ledger mutex
//! ([`DetachedCycle::outcome_ledger`]) is always an inner lock — never the
//! reverse (no ABBA ordering). See the verbatim note on
//! `executor::outcome_ledger::OutcomeLedger::claim`.
//!
//! _Avoid_: "detached arm plumbing", "sidecar state" (CONTEXT.md).

// ---------------------------------------------------------------------------
// The machine surface — states, verbs, the total transition table
// ---------------------------------------------------------------------------

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::executor::outcome_ledger::OutcomeLedger;
use super::executor::LaneOutcome;

/// Design-locked in-flight cap (~8): more than this many un-merged detached
/// results outstanding degrades the issuing cycle to the pre-epic in-cycle
/// path (backpressure via fallback — a lagging sidecar must not accumulate
/// unbounded stragglers). The A/B probe measured healthy detached cycles
/// draining inside the merge makespan, so the cap is a safety valve, not the
/// steady-state controller. (P37YJG: moved verbatim from `solver_dispatch.rs`
/// — the cap consult is the machine's.)
pub(crate) const DETACHED_INFLIGHT_CAP: u64 = 8;

/// THE persistent machine state (P37YJG). `Saturated` records the LAST
/// begin's cap verdict — the verdict itself is re-derived from the live
/// gauge at every [`DetachedCycle::begin_cycle`]; no row ever returns to
/// [`CycleArm::Unopened`] (a pipe, once open, stays open until teardown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CycleArm {
    /// No detached cycle ever issued; the merge pipe is closed.
    Unopened,
    /// Detached cycles issuing; the pipe is open (one parked `Receiver`).
    Open,
    /// The in-flight cap was reached at the last begin: the cycle degraded
    /// to the in-cycle arm (backpressure via fallback).
    Saturated,
}

/// Every machine state, in cycle order — the sized ALL-states const the
/// conformance walk drives (house pattern: `stage_handlers::ALL_STAGES` /
/// `slot.rs` `ALL_ROLES`). Adding a [`CycleArm`] variant without extending
/// the table + the walk fails the test module's exhaustive match.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the ALL-states const is the conformance walk's driver — the walk is test-declared only (house discipline)"
    )
)]
pub(crate) const ALL_CYCLE_ARMS: [CycleArm; 3] =
    [CycleArm::Unopened, CycleArm::Open, CycleArm::Saturated];

/// One terminal disposition of ONE detached outcome (the machine's
/// disposition counters + the pipeline meters they feed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// The straggler merged (apply-if-unchanged): `applied`.
    Applied,
    /// Q1a stale drop (a pool ticked during the solve): `dropped_stale`.
    DroppedStale,
    /// The path deregistered (or a typed `Failed` record landed — a genuine
    /// final drop): `dropped_deregistered`.
    DroppedDeregistered,
    /// The exactness fuse tripped (a duplicate `(solve_seq, pid)` delivery
    /// was refused): `duplicate_outcomes`.
    Duplicate,
}

/// A machine verb — the drivers' complete surface, one row family each in
/// [`transition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transition {
    /// [`DetachedCycle::begin_cycle`] — the arm decision. Carries the
    /// construction stamp and the cap verdict the live gauge supplied.
    BeginCycle {
        /// The construction-stamped `detached_solving` stance (NOT machine
        /// state — the engine owns the flag and reads it in here).
        detached_stance: bool,
        /// `outstanding >= DETACHED_INFLIGHT_CAP` at the begin read.
        cap_saturated: bool,
    },
    /// [`DetachedCycle::tick_in_cycle`] — the in-cycle arm's seq tick.
    TickInCycle,
    /// [`DetachedCycle::gauge_hook`] fired — one Solved outcome's
    /// send-success bump (the ISSUE half of the gauge pair).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "a state-transparent G-row: the bump is a cross-thread atomic event, constructed only by the conformance walk"
        )
    )]
    OutcomeSent,
    /// [`DetachedCycle::disposition`] — one terminal disposition.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "a state-transparent D-row: the counters are process-cumulative, constructed only by the conformance walk"
        )
    )]
    Disposition(Disposition),
}

/// Guards the table consults; the driver supplies them (house pattern:
/// `slot.rs` `TransitionContext`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CycleCtx {
    /// THIS cycle's begin decision (the per-cycle latch): true iff the last
    /// `begin_cycle` issued the detached arm. Guards [`Transition::TickInCycle`].
    pub(crate) began_detached: bool,
}

/// A rejected transition: loud by construction, typed for the conformance
/// walk to assert exactly WHICH row was violated (house wording:
/// `slot.rs::RejectedTransition`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("rejected transition {from:?} --{transition:?}--> ({reason})")]
pub(crate) struct RejectedTransition {
    /// The state the move was attempted from.
    pub(crate) from: CycleArm,
    /// The attempted transition.
    pub(crate) transition: Transition,
    /// Which part of the table rejected it.
    pub(crate) reason: RejectionReason,
}

/// Why a transition left the legal table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RejectionReason {
    /// The in-cycle seq tick after THIS cycle's begin already issued the
    /// detached arm — the one-arm-per-cycle law (the wrong-arm drift).
    #[error(
        "this cycle's begin already issued the detached arm — the in-cycle tick is the wrong arm"
    )]
    WrongArm,
}
/// THE total legal-transition table (P37YJG; house pattern:
/// `degenbot-workers` `slot.rs::transition`):
///
/// ```text
///              Begin{stance off} Begin{on, < cap} Begin{on, >= cap} TickInCycle(began_in_cycle) OutcomeSent / Disposition(k)
/// Unopened  →  Unopened          Open             Saturated         Unopened                    unchanged
/// Open      →  Open              Open             Saturated         Open                        unchanged
/// Saturated →  Saturated         Open             Saturated         Saturated                   unchanged
///
/// TickInCycle with began_detached = true: REJECTED (WrongArm) from EVERY
/// state — the one-arm-per-cycle law.
/// ```
///
/// Any move not covered by a row is a loud, typed rejection — never a
/// silent re-wrap. The B-rows are TOTAL over (state, stance, verdict):
/// every solve cycle begins, from any state. The G/D-rows are
/// state-transparent ON PURPOSE: the gauge pair (send-success bump ⟺
/// merge receipt decrement) and the process-cumulative disposition
/// counters are cross-thread events the persistent state does not gate
/// (the sidecar lands items in whatever state the engine is in; the
/// direct-merge test harness drives dispositions on dormant machines).
///
/// # Errors
/// A [`RejectedTransition`] whenever ~(from, t, ctx)~ is off the table.
#[must_use = "a rejected transition is a conformance event, not a suggestion"]
#[expect(
    clippy::match_same_arms,
    reason = "the rows are DISTINCT semantic families (dormant begin / state-transparent gauge + dispositions / in-cycle tick) whose state effect coincides — merging the patterns would blur the review artifact; the conformance walk pins each family separately"
)]
pub(crate) fn transition(
    from: CycleArm,
    t: Transition,
    ctx: CycleCtx,
) -> Result<CycleArm, RejectedTransition> {
    let reject = |reason: RejectionReason| {
        Err::<CycleArm, RejectedTransition>(RejectedTransition {
            from,
            transition: t,
            reason,
        })
    };
    match (from, t) {
        // B-rows — the arm decision. Total over (state, stance, verdict).
        (
            _,
            Transition::BeginCycle {
                detached_stance: false,
                ..
            },
        ) => Ok(from),
        (
            _,
            Transition::BeginCycle {
                detached_stance: true,
                cap_saturated: false,
            },
        ) => Ok(CycleArm::Open),
        (
            _,
            Transition::BeginCycle {
                detached_stance: true,
                cap_saturated: true,
            },
        ) => Ok(CycleArm::Saturated),
        // G/D-rows — cross-thread gauge + disposition events.
        (_, Transition::OutcomeSent | Transition::Disposition(_)) => Ok(from),
        // T-row — the in-cycle seq tick: legal iff THIS cycle's begin went
        // in-cycle (the driver's Arm match and this guard agree by
        // construction; a tick after a detached issue is the drift).
        // Everything else is off the legal table.
        (_, Transition::TickInCycle) => {
            if ctx.began_detached {
                reject(RejectionReason::WrongArm)
            } else {
                Ok(from)
            }
        }
    }
}

/// The per-cycle begin decision: `Detached` carries the machine-issued seq
/// (THE ledger key half for this cycle's detached claims — the ONE
/// `(solve_seq, pid)` key zone) and the merge-pipe `Sender` clone for the
/// 'static bin threads. `InCycle` keeps/degrades to the synchronous arm;
/// the caller then draws its seq via [`DetachedCycle::tick_in_cycle`].
#[derive(Debug)]
pub(crate) enum Arm {
    Detached {
        /// The seq this detached cycle was stamped with (`solve_seq_ctr`
        /// after the tick — BOTH arms draw from the ONE counter, 43E3H3).
        cycle_seq: u64,
        /// A clone of the merge pipe's `Sender` (opened once, on the first
        /// detached cycle).
        merge_tx: std::sync::mpsc::Sender<LaneOutcome>,
    },
    InCycle,
}

/// The drain's counter aggregate (fold of the in-cycle drain's locals +
/// the sidecar's per-item consumption). P37YJG: the machine owns the
/// disposition bookkeeping, so the aggregate lives here.
#[derive(Default)]
pub(crate) struct LaneDrainCounts {
    pub(crate) solved: usize,
    pub(crate) suppressed: usize,
    pub(crate) failed: usize,
}

/// The in-cycle drain's fan-in tally (P37YJG fold of the former inline
/// locals): the machine owns the undercount tripwire — the drain records
/// per-item dispositions and the cycle asserts exact totals at fan-in end.
#[derive(Default)]
pub(crate) struct FanInTally {
    solved: usize,
    suppressed: usize,
    failed: usize,
}

impl FanInTally {
    /// Record one drained item's dispositions.
    pub(crate) fn record(&mut self, counts: &LaneDrainCounts) {
        self.solved += counts.solved;
        self.suppressed += counts.suppressed;
        self.failed += counts.failed;
    }

    /// The tally's solved count (the merge span's `merge.paths` record).
    #[must_use]
    pub(crate) fn solved(&self) -> usize {
        self.solved
    }

    /// THE fan-in undercount tripwire (QR3NUS/LW-T7): the merged drain
    /// ASSERTS exact totals — outcomes == submissions, failures and all.
    /// The assert IS the gate: a mismatch fails the cycle thread loudly.
    /// (P37YJG: moved verbatim from the in-cycle drain — same message,
    /// same loud failure.)
    pub(crate) fn assert_exact(self, submitted: usize) {
        let Self {
            solved,
            suppressed,
            failed,
        } = self;
        let drained = solved + suppressed + failed;
        assert_eq!(drained, submitted, "[solve-merge] outcome accounting undercount — exactness fuse tripped (QR3NUS/LW-T7): solved {solved} + suppressed {suppressed} + failed {failed} != submitted {submitted}");
    }
}
// ---------------------------------------------------------------------------
// THE machine
// ---------------------------------------------------------------------------

/// THE one detached/in-cycle solve-arm machine (P37YJG): the single owner
/// of the scattered per-cycle fields this module's doc header names. The
/// engine holds ONE of these; the construction-stamped `detached_solving`
/// boot flag stays on the engine and reads into [`Self::begin_cycle`].
///
/// Lock order: the engine mutex (which guards this whole struct) is the
/// OUTER lock; [`Self::outcome_ledger`]'s mutex is always an inner lock —
/// never the reverse (no ABBA ordering). The gauge atomics are lock-free.
pub(crate) struct DetachedCycle {
    /// The persistent state (see [`CycleArm`]).
    state: CycleArm,
    /// THIS cycle's begin decision latch (see [`CycleCtx::began_detached`]).
    began_detached: bool,
    /// Monotonic counter bumped per issued SOLVE cycle (43E3H3: BOTH arms —
    /// the detached arm's enqueue AND the in-cycle arm's entry tick it; it
    /// is THE ledger's seq half). The sidecar's straggler-age telemetry
    /// still reads `detached_issued_seq` (detached-only) against it.
    solve_seq_ctr: u64,
    /// The seq of the most recently issued detached cycle.
    detached_issued_seq: u64,
    /// Sender half of the UNBOUNDED mpsc merge pipe; `Some` from the first
    /// detached enqueue until teardown. Each enqueue clones it into the
    /// per-bin bin jobs. Carries the unified [`executor::LaneOutcome`]
    /// (QR3NUS 43E3H3) — BOTH arms submit through it.
    merge_tx: Option<std::sync::mpsc::Sender<LaneOutcome>>,
    /// Receiver parked until `EngineStages::solve_dirty` spawns the merge
    /// sidecar (taken once via [`Self::take_merge_rx`]). `Mutex`-wrapped so
    /// the engine stays `Sync` (the parked Receiver behind the worker-only
    /// guard is touched exactly once, by the spawner thread).
    merge_rx: parking_lot::Mutex<Option<std::sync::mpsc::Receiver<LaneOutcome>>>,
    /// LW-T9 note-(a) carry: the duplicate-outcome fuse counter (the QR3NUS
    /// exactness assert). 43E3H3: BOTH arms feed it — the in-cycle drain's
    /// ledger claims and the sidecar's — one process-cumulative count.
    pub(crate) duplicate_outcomes: std::sync::atomic::AtomicU64,
    /// THE exactness ledger (LW-T9 note (a) -> QR3NUS 43E3H3: ONE ledger
    /// for BOTH solve arms): one outcome per (`solve_seq`, path)
    /// EXACTLY once — keyed (`solve_seq`, pid); the in-cycle drain claims
    /// under its cycle's seq, the sidecar under the enqueue-stamped
    /// `cycle_seq`. Held on the ENGINE (`parking_lot` Mutex) — the fuse is
    /// stateful across sidecar restarts (the pipe outlives any one
    /// sidecar thread) and shared across arms (ONE key zone, design §3.3).
    /// P37YJG: the machine OWNS and drives the field (via [`Self::claim`]);
    /// the TYPE stays `executor::outcome_ledger::OutcomeLedger`.
    pub(crate) outcome_ledger: parking_lot::Mutex<OutcomeLedger>,
    /// In-flight gauge: detached results SENT but not yet dispositioned.
    /// `Arc` because the enqueue half's bin threads bump it at send time
    /// ([`Self::gauge_hook`]) and the sidecar decrements it per Solved
    /// receipt ([`Self::solved_received`]). At cycle start a count >=
    /// [`DETACHED_INFLIGHT_CAP`] degrades that cycle to the in-cycle path
    /// (backpressure via fallback).
    pub(crate) outstanding: Arc<std::sync::atomic::AtomicU64>,
    /// Detached straggler outcome counters (applied / stale-dropped /
    /// deregistered-dropped); T2 wires the `detached.*` metrics from these.
    pub(crate) applied: std::sync::atomic::AtomicU64,
    pub(crate) dropped_stale: std::sync::atomic::AtomicU64,
    pub(crate) dropped_deregistered: std::sync::atomic::AtomicU64,
}

impl Default for DetachedCycle {
    fn default() -> Self {
        Self::new()
    }
}

impl DetachedCycle {
    /// The pre-cycle init: dormant (`Unopened`), pipe closed, counters at 0.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: CycleArm::Unopened,
            began_detached: false,
            solve_seq_ctr: 0,
            detached_issued_seq: 0,
            merge_tx: None,
            merge_rx: parking_lot::Mutex::new(None),
            duplicate_outcomes: std::sync::atomic::AtomicU64::new(0),
            outcome_ledger: parking_lot::Mutex::new(OutcomeLedger::default()),
            outstanding: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            applied: std::sync::atomic::AtomicU64::new(0),
            dropped_stale: std::sync::atomic::AtomicU64::new(0),
            dropped_deregistered: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The persistent machine state (diagnostics + the conformance walk).
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the state probe is the conformance walk's + diagnostics' view; production reads the arm decision, not the state"
        )
    )]
    pub(crate) fn state(&self) -> CycleArm {
        self.state
    }

    /// The most recently issued detached cycle's seq — the sidecar's
    /// straggler-age telemetry anchor (`detached_issued_seq`): in-cycle
    /// advances deliberately do NOT move it (design §3.3.1, N3's negative
    /// half).
    #[must_use]
    pub(crate) fn issued_seq(&self) -> u64 {
        self.detached_issued_seq
    }

    /// THE arm decision (one machine verb): consult the construction stamp
    /// and the live in-flight gauge against [`DETACHED_INFLIGHT_CAP`], tick
    /// the ONE seq counter on a detached issue, open the merge pipe once,
    /// and hand back the `Sender` clone for the 'static bin threads.
    ///
    /// The B-rows are total over (state, stance, verdict) — the table
    /// cannot reject this; the `Err` arm is the unreachable-guard against
    /// a lost row (log + conservative in-cycle degrade).
    pub(crate) fn begin_cycle(&mut self, detached_stance: bool) -> Arm {
        let cap_saturated = self.outstanding.load(Ordering::Relaxed) >= DETACHED_INFLIGHT_CAP;
        match transition(
            self.state,
            Transition::BeginCycle {
                detached_stance,
                cap_saturated,
            },
            CycleCtx {
                began_detached: self.began_detached,
            },
        ) {
            Ok(to) => self.state = to,
            Err(rejected) => {
                // Unreachable: the B-rows cover every (state, stance,
                // verdict) cell — the conformance walk pins all of them.
                tracing::error!(
                    target: crate::telemetry::DIAGNOSTIC_TARGET,
                    from = ?rejected.from,
                    transition = ?rejected.transition,
                    reason = %rejected.reason,
                    "[detached-cycle] begin_cycle REJECTED — off the machine table; degrading to in-cycle"
                );
                self.began_detached = false;
                return Arm::InCycle;
            }
        }
        if !detached_stance || cap_saturated {
            self.began_detached = false;
            return Arm::InCycle;
        }
        // THE DETACHED ISSUE: tick the ONE counter (both arms draw from it)
        // and stamp the detached-only telemetry anchor.
        self.solve_seq_ctr += 1;
        self.detached_issued_seq = self.solve_seq_ctr;
        self.began_detached = true;
        // The merge pipe: open ONCE (the first detached cycle). The sidecar
        // thread is spawned by EngineStages::solve_dirty right after this
        // enqueue half returns; the Receiver parks in the machine until
        // then.
        if self.merge_tx.is_none() {
            let (merge_tx, merge_rx) = std::sync::mpsc::channel();
            self.merge_tx = Some(merge_tx);
            *self.merge_rx.lock() = Some(merge_rx);
        }
        // Clone the Sender out so the 'static bin threads never borrow the
        // engine (they outlive the call). A vanished pipe would strand
        // every result, so die loudly (ADR-042 §10 — verbatim).
        let merge_tx = if let Some(existing) = &self.merge_tx {
            existing.clone()
        } else {
            // unreachable-by-construction (opened above); a vanished
            // pipe would strand every result, so die loudly.
            tracing::error!(
                target: crate::telemetry::DIAGNOSTIC_TARGET,
                "[detached] merge pipe vanished between open and clone — aborting"
            );
            std::process::abort();
        };
        Arm::Detached {
            cycle_seq: self.detached_issued_seq,
            merge_tx,
        }
    }

    /// THE in-cycle arm's seq tick (one machine verb): legal iff THIS
    /// cycle's begin went in-cycle (the per-cycle latch guard). A tick
    /// after a detached issue is the wrong-arm drift — the typed rejection;
    /// the caller logs it and skips the in-cycle dispatch (the driver
    /// structure makes this unreachable — see the conformance walk).
    ///
    /// In-cycle advances deliberately do NOT move `detached_issued_seq`
    /// (design §3.3.1, N3's negative half).
    pub(crate) fn tick_in_cycle(&mut self) -> Result<u64, RejectedTransition> {
        transition(
            self.state,
            Transition::TickInCycle,
            CycleCtx {
                began_detached: self.began_detached,
            },
        )?;
        self.solve_seq_ctr += 1;
        Ok(self.solve_seq_ctr)
    }

    /// The ISSUE half of the gauge pair (contract 1, REV 2 Defect 1): ONE
    /// `Arc` hook per bin, fired on a `Solved` item's SEND SUCCESS only —
    /// never for `Suppressed`/`Failed` (those never bump, so they may never
    /// decrement). The hook is 'static (bin threads outlive the cycle; an
    /// `Arc`-shared atomic carries the bump).
    #[must_use]
    pub(crate) fn gauge_hook(&self) -> Arc<dyn Fn() + Send + Sync> {
        let outstanding = Arc::clone(&self.outstanding);
        Arc::new(move || {
            outstanding.fetch_add(1, Ordering::Relaxed);
        })
    }

    /// The RECEIPT half of the gauge pair: ONE Solved item arrived at the
    /// merge — decrement the in-flight gauge exactly once and publish both
    /// meters. ONLY the Solved arm calls this (only it was ever bumped).
    /// Returns the post-decrement count for the caller's logs.
    pub(crate) fn solved_received(&self) -> u64 {
        let outstanding_now = self
            .outstanding
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        if let Some(p) = crate::instruments::pipeline() {
            p.set_detached_in_flight(outstanding_now);
        }
        hotpath::gauge!("detached_solve_in_flight").set(f64::from(
            u32::try_from(outstanding_now).unwrap_or(u32::MAX),
        ));
        outstanding_now
    }

    /// Publish the in-flight gauge to both meters (the enqueue half's
    /// post-submit read).
    pub(crate) fn publish_gauge(&self) {
        let outstanding_now = self.outstanding.load(Ordering::Relaxed);
        if let Some(p) = crate::instruments::pipeline() {
            p.set_detached_in_flight(outstanding_now);
        }
        hotpath::gauge!("detached_solve_in_flight").set(f64::from(
            u32::try_from(outstanding_now).unwrap_or(u32::MAX),
        ));
    }

    /// ONE terminal disposition: land it on the machine's counter (+ the
    /// pipeline meter it feeds). The per-item LOG LINES stay at the call
    /// sites (they carry item fields — path id, seq, age — and their span
    /// parents).
    pub(crate) fn disposition(&self, kind: Disposition) {
        match kind {
            Disposition::Applied => {
                self.applied.fetch_add(1, Ordering::Relaxed);
                if let Some(p) = crate::instruments::pipeline() {
                    p.count_detached_applied();
                }
            }
            Disposition::DroppedStale => {
                self.dropped_stale.fetch_add(1, Ordering::Relaxed);
                if let Some(p) = crate::instruments::pipeline() {
                    p.count_detached_stale_dropped();
                }
            }
            Disposition::DroppedDeregistered => {
                self.dropped_deregistered.fetch_add(1, Ordering::Relaxed);
                if let Some(p) = crate::instruments::pipeline() {
                    p.count_detached_stale_dropped();
                }
            }
            Disposition::Duplicate => {
                self.duplicate_outcomes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// THE one ledger door (P37YJG): the machine drives the ledger — every
    /// arm's claim runs through here with the machine-issued seq half.
    /// Callers hold the engine mutex across `claim` (the ledger mutex is
    /// always an inner lock — never the reverse: no ABBA ordering; see the
    /// verbatim note on `OutcomeLedger::claim`).
    pub(crate) fn claim(&self, k: (u64, u64)) -> Result<(), (u64, u64)> {
        self.outcome_ledger.lock().claim(k)
    }

    /// Hand the parked merge-pipe Receiver to the spawner (epic SRQEK5
    /// WV62TX): `EngineStages::solve_dirty` takes it ONCE, at the FIRST
    /// detached enqueue, and owns it inside the sidecar thread. `None` = the
    /// sidecar is already running (or no detached cycle ever enqueued).
    pub(crate) fn take_merge_rx(&mut self) -> Option<std::sync::mpsc::Receiver<LaneOutcome>> {
        self.merge_rx.lock().take()
    }
}

// ---------------------------------------------------------------------------
// THE ONE sidecar spawn (P37YJG): both former engine_stages spawn sites
// delegate here.
// ---------------------------------------------------------------------------

/// The detached merge sidecar's thread name (the sidecar IS the fleet
/// `Merge` role — the pinned T4 seat's named thread pattern; the historical
/// legacy name retired at the LW-T9 cutover).
#[must_use]
pub(crate) fn merge_sidecar_thread_name() -> String {
    degenbot_workers::role::WorkerRole::Merge
        .thread_name()
        .replace("{n}", "1")
}

/// The sidecar's worker-census row: the fleet `Merge` role's row (census
/// resource `fleet_merge_slots`, exactly one pinned seat) — the only
/// posture since the LW-T9 cutover.
#[must_use]
pub(crate) fn merge_sidecar_census_entry() -> degenbot_core::worker_census::WorkerCensusEntry {
    let role = degenbot_workers::role::WorkerRole::Merge;
    degenbot_core::worker_census::WorkerCensusEntry {
        resource: role.census_resource(),
        kind: role.census_kind(),
        count: 1,
        thread_name: role.thread_name(),
        sizing: role.census_sizing(),
        binding: "pinned",
    }
}

/// Spawn the detached merge sidecar for the parked receiver (epic SRQEK5
/// WV62TX; P37YJG: THE ONE spawn — both former `engine_stages` sites call
/// this). The take-once is [`DetachedCycle::take_merge_rx`], done by the
/// caller under whatever engine hold it already owns; this fn registers the
/// pinned seat and starts the named thread. A spawn failure LOUDLY ABORTS:
/// a stranded merge pipe would silently orphan every detached result
/// (ADR-042 §10 — wording + abort verbatim).
pub(crate) fn spawn_merge_sidecar(
    engine: &std::sync::Arc<parking_lot::Mutex<super::ArbitrageEngine>>,
    merge_rx: std::sync::mpsc::Receiver<LaneOutcome>,
) {
    let engine_arc = std::sync::Arc::clone(engine);
    // PE4FPM: self-register the pinned merge sidecar (the fleet
    // Merge role — the only posture since the LW-T9 cutover).
    degenbot_core::worker_census::register(merge_sidecar_census_entry());
    if let Err(err) = std::thread::Builder::new()
        .name(merge_sidecar_thread_name())
        .spawn(move || {
            // AQV6EF: production uses the process posture owner (None).
            super::solver_dispatch::detached_merge_sidecar(&engine_arc, merge_rx, None);
        })
    {
        // LOUD abort: a stranded merge pipe would silently orphan
        // every detached result.
        tracing::error!(
            error = %err,
            "detached merge sidecar spawn failed — aborting (stranded merge pipe)"
        );
        std::process::abort();
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::arb_engine::executor;

    const CTX_IN_CYCLE: CycleCtx = CycleCtx {
        began_detached: false,
    };
    const CTX_AFTER_DETACHED: CycleCtx = CycleCtx {
        began_detached: true,
    };

    /// The sized ALL-states const is exhaustive and duplicate-free; adding
    /// a `CycleArm` variant without extending the table + this walk fails
    /// the exhaustive match below at compile time (house pattern:
    /// `stage_handlers::ALL_STAGES` / `slot.rs` `ALL_ROLES`).
    #[test]
    fn all_cycle_arms_covers_every_state_exactly_once() {
        assert_eq!(ALL_CYCLE_ARMS.len(), 3, "the machine declares 3 states");
        for (i, s) in ALL_CYCLE_ARMS.iter().enumerate() {
            assert!(
                !ALL_CYCLE_ARMS[i + 1..].contains(s),
                "duplicate state {s:?} in ALL_CYCLE_ARMS"
            );
        }
        for s in ALL_CYCLE_ARMS {
            // Exhaustive: a new variant breaks this match at compile time.
            match s {
                CycleArm::Unopened | CycleArm::Open | CycleArm::Saturated => {}
            }
        }
        assert_eq!(
            ALL_CYCLE_ARMS,
            [CycleArm::Unopened, CycleArm::Open, CycleArm::Saturated]
        );
    }

    /// THE CONFORMANCE WALK (P37YJG): every legal (state × transition ×
    /// ctx) cell lands on its table successor, and every illegal cell is a
    /// typed rejection carrying the exact violated row. Mirrors the
    /// `degenbot-workers` slot T-table conformance harness.
    #[test]
    fn conformance_walks_every_legal_transition_and_rejects_every_illegal_one() {
        for from in ALL_CYCLE_ARMS {
            // BeginCycle — the arm decision. Total over (state, stance,
            // verdict): every solve cycle begins, from any state.
            for cap_saturated in [false, true] {
                for ctx in [CTX_IN_CYCLE, CTX_AFTER_DETACHED] {
                    assert_eq!(
                        transition(
                            from,
                            Transition::BeginCycle {
                                detached_stance: false,
                                cap_saturated,
                            },
                            ctx,
                        ),
                        Ok(from),
                        "B-row (stance off) from {from:?}: dormant/degraded arm, no state move"
                    );
                    assert_eq!(
                        transition(
                            from,
                            Transition::BeginCycle {
                                detached_stance: true,
                                cap_saturated: false,
                            },
                            ctx,
                        ),
                        Ok(CycleArm::Open),
                        "B-row (stance on, below cap) from {from:?}: the detached issue opens"
                    );
                    assert_eq!(
                        transition(
                            from,
                            Transition::BeginCycle {
                                detached_stance: true,
                                cap_saturated: true,
                            },
                            ctx,
                        ),
                        Ok(CycleArm::Saturated),
                        "B-row (stance on, at cap) from {from:?}: degradation records the verdict"
                    );
                }
            }
            // TickInCycle — legal iff THIS cycle's begin went in-cycle.
            assert_eq!(
                transition(from, Transition::TickInCycle, CTX_IN_CYCLE),
                Ok(from),
                "T-row from {from:?}: the in-cycle tick after an in-cycle begin"
            );
            // Cross-thread gauge + disposition events: state-transparent
            // (the pair — bump ⟺ receipt — and the process-cumulative
            // counters are NOT gated by the persistent state; the sidecar
            // lands items in whatever state the engine is in).
            for ctx in [CTX_IN_CYCLE, CTX_AFTER_DETACHED] {
                assert_eq!(
                    transition(from, Transition::OutcomeSent, ctx),
                    Ok(from),
                    "G-row from {from:?}: the send-success bump is state-transparent"
                );
                for kind in [
                    Disposition::Applied,
                    Disposition::DroppedStale,
                    Disposition::DroppedDeregistered,
                    Disposition::Duplicate,
                ] {
                    assert_eq!(
                        transition(from, Transition::Disposition(kind), ctx),
                        Ok(from),
                        "D-row {kind:?} from {from:?}: dispositions are state-transparent"
                    );
                }
            }
            // THE illegal row family: the in-cycle tick after THIS cycle's
            // begin already issued the detached arm — the one-arm-per-cycle
            // law (the wrong-arm drift).
            let rejected = transition(from, Transition::TickInCycle, CTX_AFTER_DETACHED)
                .expect_err("a tick after a detached issue is the wrong-arm drift");
            assert_eq!(
                rejected.from, from,
                "the rejection names the violated from-state"
            );
            assert_eq!(
                rejected.transition,
                Transition::TickInCycle,
                "the rejection names the violated transition"
            );
            assert_eq!(rejected.reason, RejectionReason::WrongArm);
        }
    }

    // ---- machine-level conformance (a real machine drives the script) ----

    #[test]
    fn machine_opens_the_pipe_once_and_takes_it_once() {
        let mut m = DetachedCycle::new();
        assert_eq!(m.state(), CycleArm::Unopened);
        let Arm::Detached {
            cycle_seq,
            merge_tx,
        } = m.begin_cycle(true)
        else {
            panic!("first below-cap begin with the stance ON must issue detached");
        };
        assert_eq!(cycle_seq, 1, "the ONE counter ticks on the detached issue");
        assert_eq!(m.state(), CycleArm::Open, "Unopened → Open");
        assert!(
            m.take_merge_rx().is_some(),
            "the first detached begin parks the receiver"
        );
        assert!(m.take_merge_rx().is_none(), "the receiver is take-ONCE");
        // Consecutive detached cycles: same pipe (no re-open), seq advances.
        let Arm::Detached {
            cycle_seq: seq2, ..
        } = m.begin_cycle(true)
        else {
            panic!("an Open machine below cap keeps issuing detached");
        };
        assert_eq!(seq2, 2);
        assert_eq!(
            m.state(),
            CycleArm::Open,
            "Open → Open (consecutive cycles)"
        );
        drop(merge_tx);
    }

    #[test]
    fn machine_degrades_at_the_cap_and_recovers_below_it() {
        let mut m = DetachedCycle::new();
        m.outstanding
            .store(DETACHED_INFLIGHT_CAP, Ordering::Relaxed);
        assert!(
            matches!(m.begin_cycle(true), Arm::InCycle),
            "at-cap cycles must degrade to the in-cycle arm"
        );
        assert_eq!(m.state(), CycleArm::Saturated, "Unopened → Saturated");
        assert!(
            m.take_merge_rx().is_none(),
            "no pipe was opened — no receiver parked"
        );
        // The in-cycle tick is legal from Saturated and ticks the ONE counter.
        assert_eq!(
            m.tick_in_cycle().expect("the tick is legal from Saturated"),
            1
        );
        // The cap verdict re-derives from the LIVE gauge: drain below the
        // cap and the machine re-opens.
        m.outstanding
            .store(DETACHED_INFLIGHT_CAP - 1, Ordering::Relaxed);
        assert!(matches!(m.begin_cycle(true), Arm::Detached { .. }));
        assert_eq!(m.state(), CycleArm::Open, "Saturated → Open (below cap)");
    }

    #[test]
    fn machine_stance_off_never_opens_and_ticks_in_cycle() {
        let mut m = DetachedCycle::new();
        assert!(matches!(m.begin_cycle(false), Arm::InCycle));
        assert_eq!(
            m.state(),
            CycleArm::Unopened,
            "stance OFF: the machine is dormant"
        );
        assert_eq!(
            m.tick_in_cycle()
                .expect("the stance-off arm ticks in-cycle"),
            1
        );
        // Even at/above the cap the stance-off machine never opens...
        m.outstanding
            .store(DETACHED_INFLIGHT_CAP + 5, Ordering::Relaxed);
        assert!(matches!(m.begin_cycle(false), Arm::InCycle));
        assert_eq!(m.state(), CycleArm::Unopened);
        // ...and the in-cycle tick stays legal.
        assert_eq!(m.tick_in_cycle().expect("legal"), 2);
    }

    #[test]
    fn machine_rejects_the_in_cycle_tick_after_a_detached_issue() {
        let mut m = DetachedCycle::new();
        assert!(matches!(
            m.begin_cycle(true),
            Arm::Detached { cycle_seq: 1, .. }
        ));
        let rejected = m
            .tick_in_cycle()
            .expect_err("tick after a detached issue is the wrong-arm drift");
        assert_eq!(rejected.reason, RejectionReason::WrongArm);
        assert_eq!(rejected.from, CycleArm::Open);
        // The REFUSED tick never moved the counter — no key-zone corruption.
        // A fresh begin resets the per-cycle latch; the next legal tick
        // draws the next seq.
        assert!(matches!(m.begin_cycle(false), Arm::InCycle));
        assert_eq!(m.tick_in_cycle().expect("fresh cycle ticks"), 2);
    }

    #[test]
    fn machine_gauge_pair_and_disposition_counters() {
        let m = DetachedCycle::new();
        // The ISSUE half (the lane's send-success hook): each Solved send
        // bumps exactly once.
        let hook = m.gauge_hook();
        hook();
        hook();
        assert_eq!(m.outstanding.load(Ordering::Relaxed), 2);
        // The RECEIPT half: ONE Solved item arrived at the merge.
        assert_eq!(m.solved_received(), 1);
        assert_eq!(m.outstanding.load(Ordering::Relaxed), 1);
        // The terminal dispositions land on their counters.
        m.disposition(Disposition::Applied);
        m.disposition(Disposition::DroppedStale);
        m.disposition(Disposition::DroppedDeregistered);
        m.disposition(Disposition::Duplicate);
        m.disposition(Disposition::Duplicate);
        assert_eq!(m.applied.load(Ordering::Relaxed), 1);
        assert_eq!(m.dropped_stale.load(Ordering::Relaxed), 1);
        assert_eq!(m.dropped_deregistered.load(Ordering::Relaxed), 1);
        assert_eq!(m.duplicate_outcomes.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn machine_ledger_door_claims_once_and_prunes_past_age() {
        let m = DetachedCycle::new();
        m.claim((5, 1)).expect("first sighting claims");
        assert!(
            m.claim((5, 1)).is_err(),
            "the duplicate is the fuse key — typed Err"
        );
        // The prune anchors on the CURRENT claim's seq (LEDGER_AGE = 64).
        m.claim((5 + executor::outcome_ledger::LEDGER_AGE + 1, 2))
            .expect("a far-future claim");
        assert!(
            !m.outcome_ledger.lock().contains((5, 1)),
            "rows past LEDGER_AGE prune on the next claim"
        );
    }

    #[test]
    fn fan_in_tally_asserts_exact_totals() {
        let mut tally = FanInTally::default();
        tally.record(&LaneDrainCounts {
            solved: 2,
            suppressed: 1,
            failed: 0,
        });
        tally.record(&LaneDrainCounts {
            solved: 0,
            suppressed: 0,
            failed: 1,
        });
        assert_eq!(tally.solved(), 2);
        tally.assert_exact(4);
    }

    #[test]
    #[should_panic(expected = "outcome accounting undercount")]
    fn fan_in_tally_trips_loud_on_an_undercount() {
        let mut tally = FanInTally::default();
        tally.record(&LaneDrainCounts {
            solved: 1,
            suppressed: 0,
            failed: 0,
        });
        tally.assert_exact(3);
    }
}
