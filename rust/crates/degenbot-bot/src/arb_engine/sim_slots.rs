//! Budget-derived in-flight cap for pipelined inline sims (7LV6VN T5).
//!
//! Two-runtime contract (the tokio CPU/I-O split): the ambient I/O runtime
//! (pump, websocket, dispatch, delivery) always keeps workers available,
//! and the CPU-bound runtimes - solve bins, rayon resolve, and the eager
//! sim drivers - must share the REMAINDER of the effective CPU budget
//! without oversubscribing it. The cap makes that share explicit and
//! environment-adaptive.
//!
//! T5 measured basis: an unbounded pipelined arm raised per-cycle CPU
//! demand 1021 -> 1508 ms against the 8-core cgroup quota and throttling
//! starved the walks themselves. The synchronous sim join it replaces had
//! been pacing demand as a side effect; the cap provides that pacing by
//! construction, sized from `cpu_budget` rather than from bin-block latency.

use std::sync::Arc;
use std::sync::OnceLock;

/// In-flight capacity for the global sim-slot pool (budget-derived).
/// `DEGENBOT_SOLVE_SIM_INFLIGHT` (1..=64) is terminal when set; otherwise
/// the leftover of the effective CPU budget after the solve bins take
/// theirs (`effective_cpu_budget` - `solve_worker_count`, floor 1).
fn sim_slot_capacity() -> usize {
    /// Sims are I/O-dominant: a slot is mostly an RPC/storage await, not a
    /// core. Allow the leftover budget x I/O oversubscribe so the sim
    /// pipeline stays saturated without stacking CPU demand past the
    /// quota. T5 window data (48-cycle hotpath windows): cap = leftover (2)
    /// starves the pipeline (`solve_dirty` avg 425 ms); unbounded re-runs
    /// the 1508 ms/cycle throttle story. 2x measured best of the three.
    const SIM_IO_OVERSUBSCRIBE: usize = 2;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        // KAHU5W: typed schema override (`solve.solve_sim_inflight`).
        if let Some(n) = crate::bot_core::stance::config().solve.solve_sim_inflight {
            return n.clamp(1, 64);
        }
        degenbot_core::cpu_budget::leftover_worker_budget().saturating_mul(SIM_IO_OVERSUBSCRIBE)
    })
}

/// The process-global slot pool. One pool for all cycles and arms - the
/// cap bounds TOTAL concurrent sim demand, including detached-arm
/// straggler bins whose sims outlive their enqueueing cycle.
pub(crate) fn sim_slots_global() -> Arc<SimSlots> {
    static SLOTS: OnceLock<Arc<SimSlots>> = OnceLock::new();
    Arc::clone(SLOTS.get_or_init(|| Arc::new(SimSlots::new())))
}

/// Counting semaphore over the leftover CPU budget, dedicated to pipelined
/// sim drivers (acquire blocks the scheduling bin - the deliberate pacing
/// that keeps walk + sim demand under the quota).
pub(crate) struct SimSlots {
    free: parking_lot::Mutex<usize>,
    cv: parking_lot::Condvar,
}

impl SimSlots {
    fn new() -> Self {
        Self {
            free: parking_lot::Mutex::new(sim_slot_capacity()),
            cv: parking_lot::Condvar::new(),
        }
    }

    /// Acquire one in-flight slot, blocking the scheduling bin while all
    /// slots are taken. A bin with no free slot stops producing walk work
    /// until the sim pipeline drains a result - the explicit replacement
    /// for the pacing the synchronous sim join used to provide.
    /// Self-correcting: budgets with a large leftover rarely block.
    pub(crate) fn acquire(&self) {
        let mut free = self.free.lock();
        while *free == 0 {
            self.cv.wait(&mut free);
        }
        *free -= 1;
    }

    pub(crate) fn release(&self) {
        *self.free.lock() += 1;
        self.cv.notify_one();
    }
}

/// RAII slot: release on drop. The guard MOVES into the sim driver thread
/// so the slot returns exactly when the sim finishes executing (the cap
/// bounds concurrent sims, not unconsumed receipts), and any early exit
/// (spawn failure, bin abort) still returns it through the same code path.
pub(crate) struct SlotGuard {
    slots: Option<Arc<SimSlots>>,
}

impl SlotGuard {
    pub(crate) fn acquired(slots: Arc<SimSlots>) -> Self {
        Self { slots: Some(slots) }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some(slots) = self.slots.take() {
            slots.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic counting-semaphore behavior on a local pool (not the
    /// global one - the suite shares that pool and must not starve).
    #[test]
    fn sim_slots_acquire_release_round_trip() {
        let slots = SimSlots {
            free: parking_lot::Mutex::new(2),
            cv: parking_lot::Condvar::new(),
        };
        slots.acquire();
        slots.acquire();
        slots.release();
        slots.release();
        // Balanced: two acquires succeed again without blocking forever.
        slots.acquire();
        slots.acquire();
        slots.release();
        slots.release();
    }

    #[test]
    fn slot_guard_releases_on_drop() {
        let slots = Arc::new(SimSlots {
            free: parking_lot::Mutex::new(1),
            cv: parking_lot::Condvar::new(),
        });
        {
            let _guard: SlotGuard = SlotGuard {
                slots: Some(Arc::clone(&slots)),
            };
            // dropped here -> released
        }
        // Capacity 1 is available again (the acquire below would block if
        // the guard had leaked the slot).
        slots.acquire();
        slots.release();
    }
}
