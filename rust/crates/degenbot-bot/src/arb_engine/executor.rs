//! THE Executor seam (parking-lot decision, LW-T8 JI275C): the name is
//! **`Executor`** — the LANEWARDEN vocabulary finalizes here. This module
//! owns the ONE global executor token, and re-exports the shared seam
//! types (mirroring the degenbot-workers placement of shared types — no
//! pyo3 in any signature).

use degenbot_workers::dispatcher::SubmitError;
use degenbot_workers::lane::LaneCtx;
use degenbot_workers::posture::ThrottleSample;

pub(crate) use degenbot_workers::dispatcher::SubmitReceipt;

/// One escalated work item handed through a seat's `LaneCtx` port.
pub(crate) type SubmitWork = Box<dyn FnOnce(&LaneCtx) + Send + 'static>;

/// The ONE executor seam (LW-T8): `boot/bin_count/submit` + the
/// `LaneCtx`/`EscalationPort` contracts. Non-posture executors ABSORB
/// throttle samples by contract (the default no-op body below); the
/// posture-refusal surface lives on `submit` of posture-capable executors.
pub(crate) trait Executor: Send + Sync {
    /// The structural seat count bins bind at (P6YXA6).
    fn bin_count(&self) -> usize;

    /// Submit one LPT bin job; the unit body receives the seat's `LaneCtx`.
    /// Never drops a unit (advisory receipt; ADR-042 §10).
    fn submit(&self, bin: usize, work: SubmitWork) -> Result<SubmitReceipt, SubmitError>;

    /// Posture observation (the production poller feed). Absorbed by
    /// non-posture executors.
    fn observe_throttle(&self, now_ms: u64, sample: ThrottleSample) {
        let _ = (now_ms, sample);
    }
}

/// The ONE global token (LW-T8): every call site submits through here —
/// the fleet-hosted executor is the SOLE executor since the LW-T9 cutover
/// (the tokio stance is deleted; there is no stance parameter).
pub(crate) fn global_executor() -> &'static dyn Executor {
    crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor()
}
// ---------------------------------------------------------------------------
// THE solve lane (QR3NUS 43E3H3): the one outcome-carrier module both solve
// arms submit through. Folded here from the fleet solve executor's
// provisional lane module (JI275C) — the placement is the one JI275C named.
// WITNESS + CARRIER + LEDGER live together on purpose: the lane knows the
// pids it owes, the carrier carries what the merge consumes, and the ledger
// asserts the one-outcome-per-path-per-cycle law across BOTH arms.
// ---------------------------------------------------------------------------

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc;

use degenbot_workers::dispatcher::{PanicAction, PanicVerdict};

use crate::arb_engine::inline_sim::SimulatedPathResult;
use crate::arb_engine::{BlockMetadata, SolvePathResult};

/// Why one or more of a unit's paths never delivered an outcome to the
/// result pipe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaneFailure {
    /// The bin closure panicked mid-unit; the seat survives (QR3NUS
    /// decision A) and every undelivered path becomes one of these.
    SeatPanic {
        /// The host-tracked unit id that panicked.
        unit: u64,
        /// The seat (slot) that was executing the unit.
        seat: u64,
        /// The panic payload when it is a string.
        message: Option<String>,
    },
}

/// The unified solved payload: the solved arm's
/// `(pid, result, worker_clamp_twins, payload)` data PLUS the
/// issuing-cycle identity (the exactness-ledger key material + the Q1a
/// oracle) PLUS the per-merge `merge_one_result` arguments the sidecar
/// reads off the item. The detached arm fills `cycle_seq`/`update_stamp`/
/// `solve_span` per enqueue; the in-cycle arm fills them with inert values
/// its drain never reads (`cycle_seq` 0 — its claims use the cycle's own seq).
pub(crate) struct SolveOutcome {
    pub pid: u64,
    pub result: SolvePathResult,
    pub worker_clamp_twins: u64,
    pub payload: Option<SimulatedPathResult>,
    /// The solve block the result was computed against. The in-cycle drain
    /// asserts it equals its own cycle value (a mismatch is a wrong-arm
    /// delivery — loud in debug).
    pub solve_block: u64,
    /// Cycle metadata for the streaming emission (Copy).
    pub metadata: BlockMetadata,
    /// Issuing cycle's solve sequence (the ledger's cross-cycle key).
    /// 0 on the in-cycle arm (its claims stamp the cycle's own seq).
    pub cycle_seq: u64,
    /// Per-hop `pool_update_block` snapshot at the enqueue resolve — the Q1a
    /// staleness oracle. Empty on the in-cycle arm (its stamps are live by
    /// construction: the cycle holds the Mutex through the merge).
    pub update_stamp: Vec<u64>,
    /// The enqueue-time solve span the sidecar re-enters per item
    /// (MQUKB6-T2). `Span::none()` on the in-cycle arm and in tests.
    pub solve_span: tracing::Span,
}

/// One drained per-path outcome: EXACTLY one per submitted path.
#[expect(clippy::large_enum_variant)]
// Deliberate: the Solved arm carries the full merge payload inline
// (~560B) — Boxing it would add a per-solve-path heap hop on the hot
// path, and the enum's other arms are deliberately tiny (the accounting
// records are counters, not payloads). The solve arms are throughput-
// bound per seat, and the LaneOutcome is consumed eagerly by the drain
// (no long-lived enum storage), so the variant-size asymmetry is fine.
pub(crate) enum LaneOutcome {
    /// A real solve result, stamped with issuing-cycle identity.
    Solved(SolveOutcome),
    /// The worker's None arm (failed / filtered solve): never merges, but
    /// is still an outcome the accounting must count.
    Suppressed { pid: u64 },
    /// A path whose outcome never landed because its unit panicked.
    Failed { pid: u64, failure: LaneFailure },
}

/// The lane WITNESS for one bin: it owes the pipe exactly one outcome per
/// submitted pid — delivered ones as they happen, undelivered ones patched
/// as typed `Failed` records after a panic (the seat survives).
pub(crate) struct SolveLane {
    unit: u64,
    seat: u64,
    pids: Vec<u64>,
    emitted: BTreeSet<u64>,
    tx: mpsc::Sender<LaneOutcome>,
    /// 43E3H3: the DETACHED arm's in-flight gauge hook, fired on a
    /// `Solved` item's SEND SUCCESS only (never for Suppressed/Failed —
    /// REV 2 Defect 1: those never bump, so they may never decrement).
    /// `None` on the in-cycle arm (which has no in-flight gauge).
    on_solved_send: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}

impl SolveLane {
    /// Build a lane for one bin: `unit`/`seat` name the accounting identity
    /// the panic records will carry; `pids` are the paths the bin's work
    /// owed the pipe.
    pub(crate) fn new(unit: u64, seat: u64, pids: Vec<u64>, tx: mpsc::Sender<LaneOutcome>) -> Self {
        Self {
            unit,
            seat,
            pids,
            emitted: BTreeSet::new(),
            tx,
            on_solved_send: None,
        }
    }

    /// Install the detached arm's gauge hook (fired on `Solved` send
    /// success). MUST be called before `run_solve_lane` drives the bin.
    pub(crate) fn set_on_solved_send(
        &mut self,
        on_solved_send: std::sync::Arc<dyn Fn() + Send + Sync>,
    ) {
        self.on_solved_send = Some(on_solved_send);
    }

    /// Deliver one real arm outcome (the worker's `Some` arm). The lane's
    /// `emitted` set is the DOUBLE-DELIVERY guard: every pid released this
    /// way is excluded from the post-panic patch, so a flushed
    /// outcome is never ALSO patched as Failed (the over-disposition the
    /// breaker suite catches).
    pub(crate) fn solved(&mut self, item: SolveOutcome) {
        self.emitted.insert(item.pid);
        if self.tx.send(LaneOutcome::Solved(item)).is_ok() {
            if let Some(gauge) = self.on_solved_send.as_ref() {
                gauge();
            }
        }
    }

    /// Deliver the worker's `None` arm — still an outcome (counted).
    pub(crate) fn suppressed(&mut self, pid: u64) {
        self.emitted.insert(pid);
        let _ = self.tx.send(LaneOutcome::Suppressed { pid });
    }

    /// Patch one typed per-path failure onto the pipe (decision A):
    /// exactly one outcome for `pid`, carrying unit + seat.
    pub(crate) fn failed(&mut self, pid: u64, failure: LaneFailure) {
        self.emitted.insert(pid);
        let _ = self.tx.send(LaneOutcome::Failed { pid, failure });
    }

    /// The pids this bin still owed the pipe.
    pub(crate) fn unemitted(&self) -> Vec<u64> {
        self.pids
            .iter()
            .copied()
            .filter(|pid| !self.emitted.contains(pid))
            .collect()
    }
}

/// Drive one bin body under the lane witness: a panic is caught (the seat
/// backstop also survives), the `PanicVerdict` is consulted, and every
/// still-unemitted path is patched onto the pipe as exactly one typed
/// `Failed(LaneFailure::SeatPanic)` record — so BOTH arms satisfy
/// "one outcome per submitted path" even through a panic.
pub(crate) fn run_solve_lane(
    lane: &mut SolveLane,
    verdict: &dyn PanicVerdict,
    work: impl FnOnce(&mut SolveLane),
) {
    let outcome = AssertUnwindSafe(|| work(lane));
    let outcome = std::panic::catch_unwind(outcome);
    let Err(payload) = outcome else {
        return;
    };
    let message = if let Some(text) = payload.downcast_ref::<&str>() {
        Some((*text).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    };
    match verdict.on_unit_panic(lane.unit, lane.seat) {
        PanicAction::RecordAndContinue => {
            let unemitted = lane.unemitted();
            let patched = unemitted.len();
            for pid in unemitted {
                lane.failed(
                    pid,
                    LaneFailure::SeatPanic {
                        unit: lane.unit,
                        seat: lane.seat,
                        message: message.clone(),
                    },
                );
            }
            tracing::error!(
                target: "degenbot::fleet",
                unit = lane.unit,
                seat = lane.seat,
                message = ?message,
                patched,
                "[fleet-solve] bin job panicked — seat survives with typed failure records (QR3NUS decision A)"
            );
        }
        PanicAction::Abort => {
            crate::arb_engine::fleet_solve_executor::abort_executor(
                "lane panic verdict: Abort",
                &message.unwrap_or_default(),
            );
        }
    }
}

/// THE outcome ledger (QR3NUS 43E3H3): ONE implementation asserting one
/// typed outcome per path per cycle exactly once, across BOTH solve arms
/// (the in-cycle drain and the detached merge sidecar). Keyed
/// `(solve_seq, pid)`; the prune-age constant is carried unchanged from
/// the former sidecar ledger age (LW-T9 note (a)).
pub(crate) mod outcome_ledger {
    /// How many recent solve cycles the ledger spans before pruning
    /// (carried unchanged from the former sidecar ledger age constant;
    /// the boot
    /// in-flight cap bounds meaningful straggler age at ~8, so 64 keeps a
    /// duplicate permanently longer than any straggler can live — the
    /// LW-T9 note (a) invariant, now uniform across both arms).
    pub(crate) const LEDGER_AGE: u64 = 64;

    /// The seen-outcome ledger. One key spelling for BOTH arms:
    /// `(solve_seq, pid)`. The seq comes from the engine's monotone
    /// `solve_seq_ctr` (both arms tick it). Pruning anchors on the CURRENT
    /// claim's seq.
    #[derive(Default)]
    pub(crate) struct OutcomeLedger {
        seen: std::collections::HashSet<(u64, u64)>,
    }

    impl OutcomeLedger {
        /// ONE check-and-claim: prune older cycles, then claim `(seq, pid)`.
        /// `Ok(())` = first sighting; `Err(k)` = duplicate — the fuse.
        /// Callers hold the engine mutex across `claim` (the ledger mutex is
        /// always an inner lock — never the reverse: no ABBA ordering).
        pub(crate) fn claim(&mut self, k: (u64, u64)) -> Result<(), (u64, u64)> {
            self.prune(k.0);
            if self.seen.contains(&k) {
                return Err(k);
            }
            self.seen.insert(k);
            Ok(())
        }

        /// Direct row membership — the tests' ledger-inspect surface
        /// (production code paths use [`claim`] only).
        #[cfg(test)]
        pub(crate) fn contains(&self, k: (u64, u64)) -> bool {
            self.seen.contains(&k)
        }

        fn prune(&mut self, cycle_seq: u64) {
            self.seen
                .retain(|(seq, _)| *seq >= cycle_seq.saturating_sub(LEDGER_AGE));
        }
    }
}
