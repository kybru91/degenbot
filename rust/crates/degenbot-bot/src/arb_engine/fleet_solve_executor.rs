//! Fleet-hosted solve executor (ADR-042 F3): Solver-role hosting of the
//! LPT solve bins — the fleet becomes the sole executor of solve bins under
//! the `fleet.stance=Fleet` migration stance.
//!
//! Replaces the private `solve_executor` runtime fleet on this axis, with
//! the degenbot-workers `FleetHost` FSM driving dispatch: per-bin worker
//! pinning is a keyed pin (Solver pin per LPT bin, T3/T6 across cycles —
//! warm L1/L2 + allocator arenas, RAYPAR T3 no-split/no-steal carried
//! over); per-path result streaming is the callers' existing per-path
//! mpsc sends (not per-bin) and is unchanged. The deadlock ledger carries
//! over verbatim (design doc §10): a submitted unit whose results feed a
//! pipe is never dropped — the per-role queue's loud ADR-021 overflow is
//! backed by an unbounded host-side backlog (the legacy mpsc was
//! unbounded), and every seat/host failure is a loud abort.
//!
//! Bin keys reuse the LPT bin index offset by [`SOLVE_BIN_KEY_BASE`] so
//! the merge pin key `0` stays unique: first sight claims a pin from an
//! idle Solver seat (T1→T2→T3); every later unit for that bin continues
//! on the SAME seat (T6).

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, OnceLock};

use degenbot_workers::dispatcher::{BootError, FleetBoot, FleetHost, Unit};
use degenbot_workers::role::WorkerRole;
use degenbot_workers::slot::PinKey;

/// Bin keys are the LPT bin index offset by one: the merge pin owns key 0
/// (`degenbot_workers::slot::MERGE_PIN_KEY`) and a Solver key must never
/// collide with it.
pub(crate) const SOLVE_BIN_KEY_BASE: PinKey = 1;

/// Loud, unrecoverable executor failure (mirror of `solve_executor.rs`'s
/// abort discipline): a dead host would strand in-flight per-path result
/// sends in pipes nobody drains, so swallowing the error is never an
/// option.
fn abort_executor(context: &str, err: &str) -> ! {
    tracing::error!(
        context = %context,
        error = %err,
        "[fleet-solve] unrecoverable — aborting (stranded result pipe)"
    );
    std::process::abort();
}

/// One bin job as the host hands it to a Solver seat.
struct SeatJob {
    /// The host-tracked unit id (dispatch bookkeeping).
    unit: u64,
    /// The work payload (the 'static + Send `run_bin` closure — the fleet
    /// crate has no pyo3 and simulation never round-trips Python, design
    /// doc §8).
    work: Box<dyn FnOnce() + Send>,
}

/// Host-bound message: a submitted bin unit, or a seat reporting its unit
/// done (completion drives T3 — the seat re-pins warm for the next cycle).
enum HostMsg {
    Enqueue(Unit),
    SeatDone { seat: u64 },
}

/// The fleet-hosted solve executor. Shared by all engine cycles (the
/// global static hands out `&'static`, mirroring the incumbent executor's
/// construction-once contract: persistent seats keep warm L1/L2 +
/// allocator arenas across cycles).
pub(crate) struct FleetSolveExecutor {
    tx: mpsc::Sender<HostMsg>,
    unit_seq: AtomicU64,
    /// Seat count (read via [`FleetSolveExecutor::bin_count`] — the
    /// dispatch arms bin at exactly this count so every bin has a home).
    solver_seats: usize,
}

impl FleetSolveExecutor {
    /// Boot the executor from a `FleetBoot` (quota + overrides + posture):
    /// boot the [`FleetHost`], spawn one persistent named seat thread per
    /// Solver pin, and run the dispatch loop on the host thread. Fail-loud
    /// (the typed `BootError`) when the declared shares cannot host the
    /// quota — oversubscription is a configuration bug surfaced at boot,
    /// never a runtime throttle storm (design doc §5).
    ///
    /// # Errors
    /// [`BootError`] — the fleet budget sum check or a boot invariant.
    pub(crate) fn boot(boot: FleetBoot) -> Result<Self, BootError> {
        let host = FleetHost::boot(boot)?;
        let solver_seats = host.budget().solver_pin_count;

        let (tx, rx) = mpsc::channel::<HostMsg>();
        // Per-seat mailboxes: a seat is a persistent keyed pin — one unit
        // at a time, warm arenas across cycles (RAYPAR T3, design doc §3.4).
        let mut seat_senders = Vec::with_capacity(solver_seats);
        let mut seat_mailboxes = Vec::with_capacity(solver_seats);
        for _seat in 0..solver_seats {
            let (stx, srx) = mpsc::channel::<SeatJob>();
            seat_senders.push(stx);
            seat_mailboxes.push(srx);
        }
        // Census: `FleetHost::boot` registered every v1 role row. The seats
        // are the runtime behind the solver pins, named per
        // `WorkerRole::Solver::thread_name()` (work-fleet-solver-{n}).
        // Seat completions ride the SAME host channel as submissions: a
        // single message queue cannot deadlock (a separate completion
        // channel would need select() to drain while blocking on rx).
        for (seat, srx) in seat_mailboxes.into_iter().enumerate() {
            let done = tx.clone();
            let spawned = std::thread::Builder::new()
                .name(
                    WorkerRole::Solver
                        .thread_name()
                        .replace("{n}", &seat.to_string()),
                )
                .spawn(move || seat_loop(u64::try_from(seat).unwrap_or(u64::MAX), srx, &done));
            if let Err(err) = spawned {
                // A missing seat strands its pinned bins' results — loud.
                abort_executor("solver seat spawn", &format!("{err:?}"));
            }
        }
        let spawned = std::thread::Builder::new()
            .name("work-fleet-solver-host".to_string())
            .spawn(move || host_loop(rx, host, &seat_senders));
        if let Err(err) = spawned {
            abort_executor("fleet host thread spawn", &format!("{err:?}"));
        }
        Ok(Self {
            tx,
            unit_seq: AtomicU64::new(0),
            solver_seats,
        })
    }

    /// Solver seats = the budget's structural LPT bin count. The dispatch
    /// arms bin at THIS count (P6YXA6 reconciliation): pins and bins are
    /// the same number, so every bin owns a warm keyed seat across cycles.
    #[must_use]
    pub(crate) fn bin_count(&self) -> usize {
        self.solver_seats
    }

    /// Submit one LPT bin job keyed to its bin. Never drops: the host-side
    /// backlog preserves the legacy unbounded-mpsc semantics; a closed host
    /// channel (executor died) is a LOUD abort — a lost bin would strand
    /// its paths' per-path result sends forever (stranded pipe, §10).
    pub(crate) fn spawn(&self, bin: usize, job: impl FnOnce() + Send + 'static) {
        let key = SOLVE_BIN_KEY_BASE.saturating_add(u64::try_from(bin).unwrap_or(u64::MAX));
        let unit = Unit::new(
            self.unit_seq.fetch_add(1, Ordering::Relaxed),
            WorkerRole::Solver,
            Some(key),
            // The bin's per-path result sends feed the merge pipe.
            true,
            Box::new(job),
        );
        if self.tx.send(HostMsg::Enqueue(unit)).is_err() {
            abort_executor("bin submission", "fleet host channel closed");
        }
    }
}

/// One Solver seat (a persistent keyed pin): execute units one at a time —
/// no yield mid-unit — and report completion so the host applies T3 (the
/// seat re-pins warm; arenas are never live across a role switch).
#[expect(
    clippy::needless_pass_by_value,
    reason = "the seat's mailbox Receiver is owned by the seat thread — a borrow cannot cross the thread boundary"
)]
fn seat_loop(seat: u64, rx: mpsc::Receiver<SeatJob>, done: &mpsc::Sender<HostMsg>) {
    while let Ok(job) = rx.recv() {
        // A panicking bin closure must not kill the seat (its pinned bins
        // would strand): keep the seat alive, log loudly, report done.
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| (job.work)()));
        if outcome.is_err() {
            tracing::error!(
                target: "degenbot::fleet",
                seat,
                unit = job.unit,
                "[fleet-solve] bin job panicked — the seat survives, the failure is loud"
            );
        }
        if done.send(HostMsg::SeatDone { seat }).is_err() {
            // The host is gone (executor dropped — tests): the seat retires.
            break;
        }
    }
}

/// The dispatch loop (design doc §4): enqueue → precedence-grant →
/// execute. Owns the `FleetHost` exclusively; every FSM transition runs
/// here. Exits when the submission channel closes (all executor handles
/// dropped — process teardown), letting seats drain their mailboxes.
///
/// `rx` is taken by value under an explicit lint expectation: the
/// Receiver's ownership moves into the spawned host thread — a borrow
/// cannot cross the thread boundary.
#[expect(clippy::needless_pass_by_value)]
fn host_loop(rx: mpsc::Receiver<HostMsg>, mut host: FleetHost, seats: &[mpsc::Sender<SeatJob>]) {
    let mut backlog: VecDeque<Unit> = VecDeque::new();
    while let Ok(msg) = rx.recv() {
        apply_host_msg(&mut host, &mut backlog, msg);
        pump(&mut host, &mut backlog, seats);
    }
}

/// Apply one submission or completion (both arrive on the single host
/// channel — completions can never starve behind a blocking recv).
fn apply_host_msg(host: &mut FleetHost, backlog: &mut VecDeque<Unit>, msg: HostMsg) {
    match msg {
        HostMsg::Enqueue(unit) => {
            // Pre-check capacity INSTEAD of failing enqueue: the host
            // thread owns every queue mutation, so the check is exact.
            // Units that do not fit spill to the backlog (unbounded, like
            // the legacy mpsc) and drain FIRST on the next pump — never
            // dropped (§10 ledger).
            if host.queue_len(WorkerRole::Solver) >= host.queue_cap(WorkerRole::Solver) {
                backlog.push_back(unit);
            } else if let Err(err) = host.enqueue(unit) {
                // v1-active Solver units cannot hit RoleNotActive /
                // MergeNeverQueued / PostureHeld; any such error is a
                // broken invariant, not a drop.
                abort_executor("solver enqueue", &err.to_string());
            }
        }
        HostMsg::SeatDone { seat } => {
            if let Err(err) = host.complete(seat) {
                abort_executor("seat completion (T3)", &err.to_string());
            }
        }
    }
}

/// The one precedence grant loop pass (design doc §4): backlog first, then
/// dispatch grants onto seats. Grants apply T2 (start) at grant time — the
/// seat's mailbox send IS the claim — and completion arrives via
/// [`HostMsg::SeatDone`] (T3).
fn pump(host: &mut FleetHost, backlog: &mut VecDeque<Unit>, seats: &[mpsc::Sender<SeatJob>]) {
    // Backlog drains FIRST (FIFO across the loud-overflow seam).
    while backlog.front().is_some() {
        if host.queue_len(WorkerRole::Solver) >= host.queue_cap(WorkerRole::Solver) {
            break;
        }
        let Some(unit) = backlog.pop_front() else {
            break;
        };
        if let Err(err) = host.enqueue(unit) {
            abort_executor("backlog drain", &err.to_string());
        }
    }
    loop {
        let grants = host.dispatch();
        if grants.is_empty() {
            break;
        }
        for (grant, unit) in grants {
            if let Err(err) = host.start(grant.slot, &unit) {
                abort_executor("grant start (T2)", &err.to_string());
            }
            let Some(seat_tx) = seats.get(usize::try_from(grant.slot).unwrap_or(usize::MAX)) else {
                abort_executor("dispatch grant", "unknown seat");
            };
            let job = SeatJob {
                unit: grant.unit,
                work: unit.work,
            };
            if seat_tx.send(job).is_err() {
                // A dead seat cannot drain its pinned bins' results —
                // stranded pipe (§10).
                abort_executor("seat mailbox send", "seat thread is gone");
            }
        }
    }
}

static FLEET_SOLVE_BOOT: OnceLock<FleetBoot> = OnceLock::new();
static FLEET_EXECUTOR: OnceLock<FleetSolveExecutor> = OnceLock::new();

/// Install the typed boot descriptor (fleet quota + overrides + posture)
/// parsed ONCE at engine construction from the config; the global executor
/// lazily consumes it on first fleet-stance solve. Never overrides an
/// installed value (first engine wins, like the other stance statics).
pub(crate) fn install_boot(boot: FleetBoot) {
    let _ = FLEET_SOLVE_BOOT.set(boot);
}

/// Hermetic fallback when no engine installed a boot descriptor: detect
/// the fractional cgroup quota, no overrides, doc-default posture.
fn fallback_boot() -> FleetBoot {
    FleetBoot {
        quota_cpus: degenbot_workers::quota::fractional_cpu_budget(),
        overrides: degenbot_workers::budget::BudgetOverrides::default(),
        posture: degenbot_workers::posture::PosturePolicy::doc_defaults(),
    }
}

/// The process-wide fleet solve executor, built lazily on the first
/// fleet-stance solve and persisting for the process lifetime.
pub(crate) fn global_fleet_solve_executor() -> &'static FleetSolveExecutor {
    FLEET_EXECUTOR.get_or_init(|| {
        let boot = FLEET_SOLVE_BOOT
            .get()
            .copied()
            .unwrap_or_else(fallback_boot);
        match FleetSolveExecutor::boot(boot) {
            Ok(executor) => executor,
            Err(err) => abort_executor("fleet budget boot", &err.to_string()),
        }
    })
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use degenbot_solvers::mixed::SolvePathResult;
    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::FleetBoot;
    use degenbot_workers::posture::PosturePolicy;

    use super::super::solver_dispatch::executor_ab_probe::{
        load_corpus_fixture, probe_ctx, prod_lpt_bins,
    };
    use super::super::solver_dispatch::solve_one_path;
    use super::{FleetSolveExecutor, SOLVE_BIN_KEY_BASE};

    fn hermetic_boot() -> FleetBoot {
        FleetBoot {
            quota_cpus: 8.0,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        }
    }

    fn submit_bins(
        executor: &FleetSolveExecutor,
        bins: &[Vec<usize>],
        items: &[Arc<::degenbot_solvers::mixed::ResolvedMixedPath>],
        ctx: &Arc<crate::arb_engine::solver_dispatch::SolveCycleShared>,
    ) -> Vec<(u64, SolvePathResult)> {
        let (tx, rx) = std::sync::mpsc::channel::<(u64, SolvePathResult)>();
        for (bin_idx, bin) in bins.iter().enumerate() {
            let tx = tx.clone();
            let ctx = Arc::clone(ctx);
            let items: Vec<_> = bin.iter().map(|&i| Arc::clone(&items[i])).collect();
            let keys: Vec<u64> = bin
                .iter()
                .map(|&i| u64::try_from(i).unwrap_or(u64::MAX))
                .collect();
            executor.spawn(bin_idx, move || {
                // Per-path send (not per-bin): the existing per-path result
                // streaming — byte-for-byte what the run_bin bodies do.
                for (key, item) in keys.into_iter().zip(items) {
                    if let Some((pid, r)) = solve_one_path(&ctx, &tracing::Span::none(), key, &item)
                    {
                        let _ = tx.send((pid, r));
                    }
                }
            });
        }
        drop(tx);
        let mut results: Vec<(u64, SolvePathResult)> = rx.into_iter().collect();
        results.sort_unstable_by_key(|(pid, _)| *pid);
        results
    }

    /// Parity fixture (BCA77G): the fleet-hosted solve executor is
    /// byte-equal with the legacy private-runtime executor on the committed
    /// heavy-CL capture fixture. Both arms drive the SAME `solve_one_path`
    /// per path over the SAME LPT bins; the only difference is the
    /// dispatch machinery (the port).
    #[test]
    fn fleet_executor_is_result_parity_with_legacy_executor_on_capture_fixture() {
        let items = load_corpus_fixture();
        let ctx = probe_ctx();
        let bins = prod_lpt_bins(&items, degenbot_core::cpu_budget::solve_worker_count());

        // Legacy arm: the incumbent private-runtime executor (BXUSGL T1).
        let legacy = {
            let (tx, rx) = std::sync::mpsc::channel::<(u64, SolvePathResult)>();
            let exec = crate::arb_engine::solve_executor::SolveExecutor::new(
                "parity-legacy-exec",
                bins.len(),
            );
            for bin in &bins {
                let tx = tx.clone();
                let ctx = Arc::clone(&ctx);
                let items: Vec<_> = bin.iter().map(|&i| Arc::clone(&items[i])).collect();
                let keys: Vec<u64> = bin
                    .iter()
                    .map(|&i| u64::try_from(i).unwrap_or(u64::MAX))
                    .collect();
                exec.spawn(move || {
                    for (key, item) in keys.into_iter().zip(items) {
                        if let Some((pid, r)) =
                            solve_one_path(&ctx, &tracing::Span::none(), key, &item)
                        {
                            let _ = tx.send((pid, r));
                        }
                    }
                });
            }
            drop(tx);
            let mut results: Vec<(u64, SolvePathResult)> = rx.into_iter().collect();
            results.sort_unstable_by_key(|(pid, _)| *pid);
            results
        };
        assert!(!legacy.is_empty(), "fixture must produce results");

        // Fleet arm: same bins, same jobs, fleet-hosted Solver pins.
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let fleet = submit_bins(&executor, &bins, &items, &ctx);

        assert_eq!(
            fleet, legacy,
            "fleet-hosted solve results must be byte-equal with the legacy executor"
        );
    }

    /// Solver bin keys never collide with the merge pin key (the FSM's
    /// keyed-pin invariant, ADR-042 §3.4).
    #[test]
    fn solve_bin_keys_never_collide_with_the_merge_pin_key() {
        assert_ne!(SOLVE_BIN_KEY_BASE, degenbot_workers::slot::MERGE_PIN_KEY);
    }

    /// Pinning fixture (BCA77G): per-bin worker pinning — every bin's
    /// units ride the same seat across cycles (T3/T6), matching the
    /// RAYPAR T3 one-persistent-worker-per-bin contract.
    ///
    /// P6YXA6 sizing reconciliation: the fleet seats are the
    /// STRUCTURAL LPT bin count — at a hermetic Q = 8 boot,
    /// floor(8) − the default solve headroom (2) = 6 seats — not the
    /// retired sharesx2 multiple (8). The dispatch arms bin at this same
    /// count, so every bin owns a warm keyed seat across cycles (T6).
    #[test]
    fn solver_seats_equal_the_structural_lpt_bin_count() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        assert_eq!(executor.bin_count(), 6);
    }

    #[test]
    fn bins_stay_pinned_to_one_seat_across_cycles() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let bins = executor.bin_count().clamp(2, 4);
        let observed: Arc<parking_lot::Mutex<Vec<(u64, std::thread::ThreadId)>>> = Arc::default();
        for _cycle in 0..3 {
            for bin in 0..bins {
                let bin_key = u64::try_from(bin).unwrap_or(u64::MAX);
                let observed = Arc::clone(&observed);
                executor.spawn(bin, move || {
                    observed.lock().push((bin_key, std::thread::current().id()));
                });
            }
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while observed.lock().len() < (bins * 3) {
            assert!(
                std::time::Instant::now() < deadline,
                "fleet seats did not drain all bin units in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let observed = observed.lock().clone();
        for bin in 0..bins {
            let bin_key = u64::try_from(bin).unwrap_or(u64::MAX);
            let seats: std::collections::HashSet<_> = observed
                .iter()
                .filter(|(k, _)| *k == bin_key)
                .map(|(_, t)| *t)
                .collect();
            assert_eq!(
                seats.len(),
                1,
                "bin {bin} must pin to exactly one seat across cycles"
            );
        }
    }
}
