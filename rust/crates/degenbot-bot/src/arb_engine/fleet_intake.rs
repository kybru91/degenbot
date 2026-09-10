//! `fleet_intake` — the PRG-3 port: the crate's ONLY public surface into the
//! pooled fleet intake. The port (`FleetIntake`) is the pub NAME; the
//! executors implementing it stay crate-internal (zero exported types).
//! No pyo3 in any signature (`executor.rs` doc rule); the unit body stays
//! `Box<dyn FnOnce() + Send + 'static>` — the `Python::attach` rides
//! degenbot-python's closure body, never a port item. HARD RATCHET: the
//! surface is this port + (after commit 2) 2 pub fns — add nothing.

/// The pooled work unit: the seat threads' existing box shape, pinned as
/// an alias (concrete, object-safe — never a generic on the port).
pub type InnerWork = Box<dyn FnOnce() + Send + 'static>;

/// The port: fire-and-dispatch a pooled unit; no response from the
/// executor (the unit self-reports through its own closure channel).
pub trait FleetIntake: Send + Sync {
    /// Submit one pooled unit; slots are the budget's per-role cap and
    /// never overflow the executor (host-side backlog spill is fail-loud).
    /// The unit body carries its own completion channel (per-request
    /// receipt semantics). Lost-key (no pin affinity): the seat picks
    /// the unit up as soon as a slot frees.
    fn spawn(&self, work: InnerWork);
}

/// The sim-side sibling (one line: the fleet sim executor upcast); consumed
/// by `executor.rs::global_sim_executor()` so the §3.1 re-route reads
/// through ONE module; crate-internal because only the sim dispatch route
/// needs it.
#[must_use]
pub(crate) fn sim_intake() -> &'static dyn FleetIntake {
    crate::arb_engine::fleet_sim_executor::global_fleet_sim_executor()
}

#[cfg(test)]
// The loud-expect fixture style mirrors the executor fixture modules; the
// module-level expect is the documented-permitted form.
#[expect(clippy::expect_used)]
mod tests {
    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::FleetBoot;
    use degenbot_workers::posture::PosturePolicy;

    use super::{FleetIntake, InnerWork};

    // Copied from fleet_registration_executor.rs — the module's existing
    // fixture kit, module-local (no new helpers; the design's fixture note).
    fn hermetic_boot() -> FleetBoot {
        FleetBoot {
            quota_cpus: 8.0,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        }
    }

    /// T3 `pooled_spawn_failure_modes_stay_pinned`: two compile-level pins.
    /// (i) An exhaustive `match` over the private `try_send`'s
    /// `Result<(), ()>` — re-widening the in-crate close modeling breaks
    /// compile until this match is updated deliberately. (ii) The
    /// object-safety let-binding — a generic parameter leaking into
    /// `FleetIntake::spawn` makes the port NOT dyn-compatible and this
    /// binding (hence the whole facade return type) stops compiling.
    #[test]
    fn pooled_spawn_failure_modes_stay_pinned() {
        let executor =
            crate::arb_engine::fleet_registration_executor::FleetRegistrationExecutor::boot(
                hermetic_boot(),
            )
            .expect("fleet intake boot");
        // The object-safety pin: the binding compiles only while the port
        // stays dyn-compatible (the concrete alias parameter, never a
        // generic).
        let port: &dyn FleetIntake = &executor;
        let (tx, rx) = std::sync::mpsc::channel::<u64>();
        port.spawn(Box::new(move || {
            let _ = tx.send(1);
        }));
        let got = rx.recv_timeout(std::time::Duration::from_secs(10));
        assert_eq!(got, Ok(1), "the port delivered one unit");
        // The failure-vocabulary pin: the in-crate close modeling is exactly
        // `Result<(), ()>` — exhaustive over BOTH arms, no third shape.
        let sent: InnerWork = Box::new(|| {});
        match executor.try_send(sent) {
            Ok(()) => {}
            Err(()) => unreachable!("a live executor's host channel is open"),
        }
    }
}
