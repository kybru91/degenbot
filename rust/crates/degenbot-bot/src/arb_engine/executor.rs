//! THE Executor seam (parking-lot decision, LW-T8 JI275C): the name is
//! **`Executor`** — the LANEWARDEN vocabulary finalizes here. This module
//! owns the ONE global token hiding BOTH stance globals, and re-exports the
//! shared seam types (mirroring the degenbot-workers placement of shared
//! types — no pyo3 in any signature).

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

/// The ONE global token (LW-T8): hides BOTH stance `OnceLock`s — call sites
/// never see `global_fleet_solve_executor` / `global_solve_executor`.
pub(crate) fn global_executor(fleet_hosted: bool) -> &'static dyn Executor {
    if fleet_hosted {
        crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor()
    } else {
        crate::arb_engine::solve_executor::global_solve_executor()
    }
}
