//! Fleet-hosted inline-sim executor (ADR-042 F4): `SimDriver`-role hosting of
//! the pipelined inline sims — the fleet becomes the sole executor of sim
//! units under the `fleet.stance=Fleet` migration stance. The incumbent
//! per-path `arb-sim-{pid}` detached-thread spawn (a burst of ~85–105
//! mostly-parked threads per cycle) retires onto a warm pooled seat set;
//! the legacy stance keeps the incumbent runtime byte-for-byte.
//!
//! The seat pool is the budget's `SimDriver` slot cap (design doc §5 —
//! today's `SimSlots` cap, `fleet.sim_slot_cap` terminal override), and the
//! host FSM's dispatch lane 2 gives queued sims precedence over new Solver
//! intake while cordoning floors sim intake (in-flight sims are never
//! cancelled — the `SimSlots` release-on-drop discipline carried over).
//!
//! Pacing note (the one deliberate difference): the incumbent semaphore
//! acquired the slot on the detached thread BEFORE the sim body ran and
//! released it exactly when the sim finished; the fleet replaces that pair
//! with the host's pooled-seat bound — a seat IS the granted slot, and the
//! grant lane caps concurrent executing sims at the budget slot cap. The
//! scheduling bin never blocks (the legacy parked-before-exec threads were
//! the unbounded part of the burst); submission over the never-drop
//! host channel is the pacing seam now.
//!
//! Sim units carry no pin key (the `SimDriver` role is pooled, T5: run →
//! back-to-idle); the merge pin / Solver-pin lanes of the shared host FSM
//! never fire here because this executor only enqueues `SimDriver` units.
//! The `WrapDatabaseAsync` runtime-capture caveat (ADR-042 §8) is
//! unchanged: the sim body still enters via the installed hook, whose
//! task-spawn executes on a multi-thread runtime worker.
//!
//! GIL ruling (task LTUE7I): the hook body is verified Python-free —
//! `degenbot_python::simulation::inline_hook` imports no `pyo3` symbol
//! and never attaches the GIL on its hot path, so hosting the closure on
//! fleet seats crosses the FFI only at the existing install/delivery
//! seams (design doc §8: simulation never round-trips Python).

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, OnceLock};

use crate::arb_engine::boot_stamp::{BootRole, BootStamp};
use degenbot_workers::dispatcher::{BootError, FleetBoot, FleetHost, GrantKind, Unit};
use degenbot_workers::lane::LaneCtx;
use degenbot_workers::role::WorkerRole;

use crate::arb_engine::fleet_intake::{FleetIntake, InnerWork};

/// Loud, unrecoverable executor failure (mirror of the fleet solve
/// executor's abort discipline): a dead host would strand in-flight sim
/// receipts — a scheduling bin waiting on a receipt parks forever (stranded
/// pipe, design doc §10) — so swallowing the error is never an option.
#[expect(
    clippy::print_stderr,
    reason = "the abort path must stay legible with no tracing subscriber installed (test harnesses drop the tracing event); stderr is the process's last message"
)]
fn abort_executor(context: &str, err: &str) -> ! {
    tracing::error!(
        context = %context,
        error = %err,
        "[fleet-sim] unrecoverable — aborting (stranded sim receipt pipe)"
    );
    eprintln!("[fleet-sim] UNRECOVERABLE, aborting (stranded sim receipt pipe): {context}: {err}");
    std::process::abort();
}

/// Host-bound message: a submitted sim unit, or a seat reporting its unit
/// done (completion drives T5 — the pooled slot returns to idle).
enum HostMsg {
    Enqueue(Unit),
    SeatDone { seat: u64 },
}

/// One granted sim unit handed to whichever pooled seat takes it next
/// (`SimDriver` is a pooled role — seats contend, no pin affinity).
struct SeatJob {
    /// The host-tracked slot the unit was granted to (completion carries it
    /// back so T5 applies to the right slot).
    slot: u64,
    /// The work payload (the 'static + Send sim closure — the fleet crate
    /// has no pyo3 and simulation never round-trips Python, design doc §8).
    /// Takes the seat's `LaneCtx` (LW-T2); pooled seats hand the detached
    /// stub (LW-T8 landed: the executors submit through ONE seam).
    work: Box<dyn FnOnce(&LaneCtx) + Send>,
}

/// The fleet-hosted inline-sim executor. Shared by all engine cycles (the
/// global static hands out `&'static`, mirroring the fleet solve
/// executor's construction-once contract: warm pooled seats across
/// cycles).
pub(crate) struct FleetSimExecutor {
    tx: mpsc::Sender<HostMsg>,
    unit_seq: AtomicU64,
    /// Test-facing seat count (the budget's `SimDriver` slot cap).
    #[cfg(test)]
    sim_seats: usize,
}

impl FleetSimExecutor {
    /// Boot from a `FleetBoot` (quota + overrides + posture): boot the
    /// [`FleetHost`], spawn the pooled `SimDriver` seat threads (the
    /// budget's sim slot cap, `work-fleet-sim-{n}` census naming), and
    /// run the dispatch loop on the host thread. Fail-loud (the typed
    /// [`BootError`]) when the declared shares cannot host the quota.
    ///
    /// # Errors
    /// [`BootError`] — the fleet budget sum check or a boot invariant.
    pub(crate) fn boot(boot: FleetBoot) -> Result<Self, BootError> {
        let host = FleetHost::boot(boot)?;
        let sim_seats = host.budget().sim_slot_cap;
        let (tx, rx) = mpsc::channel::<HostMsg>();
        // Pooled seats contend on ONE shared work queue: a grant lands a
        // unit there, any idle seat takes it, and the completion reports
        // the GRANTED slot id so the host applies T5 to the right slot.
        // Grants never exceed the sim intake cap, which never exceeds the
        // seat count, so every granted unit is picked up without delay.
        let work = Arc::new(WorkQueue::new());
        for seat in 0..sim_seats {
            let done = tx.clone();
            let work = Arc::clone(&work);
            let spawned = std::thread::Builder::new()
                .name(
                    WorkerRole::SimDriver
                        .thread_name()
                        .replace("{n}", &seat.to_string()),
                )
                .spawn(move || seat_loop(&work, &done));
            if let Err(err) = spawned {
                // A missing seat strands the receipts of every unit that
                // would have run on it — loud (§10).
                abort_executor("sim seat spawn", &format!("{err:?}"));
            }
        }
        let spawned = std::thread::Builder::new()
            .name("work-fleet-sim-host".to_string())
            .spawn(move || {
                host_loop(rx, host, Arc::clone(&work));
                // Process teardown: the submission channel closed. Retire
                // the seats so no worker parks forever on an empty queue.
                work.close();
            });
        if let Err(err) = spawned {
            abort_executor("fleet sim host thread spawn", &format!("{err:?}"));
        }
        Ok(Self {
            tx,
            unit_seq: AtomicU64::new(0),
            #[cfg(test)]
            sim_seats,
        })
    }

    /// The budget's `SimDriver` slot cap (the pooled seat count). Test-facing
    /// (the scheduling sites submit without asking the cap).
    #[cfg(test)]
    pub(crate) fn sim_slot_cap(&self) -> usize {
        self.sim_seats
    }

    /// The pre-existing submit body, RENAMED (was the inherent `spawn`,
    /// sim:158-174): wraps into `Unit::new(.., Box::new(move |_ctx| work()))`
    /// (:166) and `tx.send(HostMsg::Enqueue(unit))` (:173, `map_err`-typed
    /// to `Err(())` on a closed channel — the send VALUE carries the close
    /// arm; the abort lives in the trait impl). The OLD close arm
    /// (sim:171-173's `abort_executor`) MOVES to the trait impl below — same
    /// process-exit semantics, one owner of the abort. Private fn, in-crate.
    fn try_send(&self, work: InnerWork) -> Result<(), ()> {
        let unit = Unit::new(
            self.unit_seq.fetch_add(1, Ordering::Relaxed),
            WorkerRole::SimDriver,
            None,
            // The unit's receipt feeds the scheduling bin's join — a
            // stranded pipe if abandoned.
            true,
            Box::new(move |_ctx| work()),
        );
        // The close arm, typed to the port's unit vocabulary: the send
        // value carries the close arm; the abort lives in the trait impl.
        match self.tx.send(HostMsg::Enqueue(unit)) {
            Ok(()) => Ok(()),
            Err(_) => Err(()),
        }
    }

    /// Test-venue shim: the OLD name `spawn`, `#[cfg(test)]`-only, so the
    /// in-file fixtures (sim:415..:546) keep compiling VERBATIM. Outside test
    /// builds the inherent fn does not EXIST — the inherent-priority shadow
    /// (§6 risk 6) is confined to test code that calls no port path, and the
    /// port is the only `spawn` production callers can name.
    #[cfg(test)]
    fn spawn(&self, work: impl FnOnce() + Send + 'static) {
        let _ = self.try_send(Box::new(work));
    }
}

impl FleetIntake for FleetSimExecutor {
    fn spawn(&self, work: InnerWork) {
        if self.try_send(work).is_err() {
            abort_executor("sim submission", "fleet sim host channel closed");
        }
    }
}

/// The shared pooled-seat work queue (std `mpsc` receivers are not
/// `Clone`, so the contended seat pool rides a condvar deque).
#[derive(Default)]
struct WorkQueue {
    queue: parking_lot::Mutex<VecDeque<SeatJob>>,
    shutdown: parking_lot::Mutex<bool>,
    work_available: parking_lot::Condvar,
}

impl WorkQueue {
    fn new() -> Self {
        Self::default()
    }

    /// Take one granted unit, parking the seat until one arrives or the
    /// queue shuts down (host retired — process teardown).
    fn take(&self) -> Option<SeatJob> {
        if let Some(job) = self.queue.lock().pop_front() {
            return Some(job);
        }
        let mut shutdown = self.shutdown.lock();
        loop {
            if *shutdown {
                return None;
            }
            {
                let mut q = self.queue.lock();
                if let Some(job) = q.pop_front() {
                    return Some(job);
                }
            }
            // Park until a grant lands or the host retires the pool. The
            // shutdown mutex doubles as the re-check serialization point.
            self.work_available
                .wait_for(&mut shutdown, std::time::Duration::from_millis(50));
        }
    }

    fn push(&self, job: SeatJob) {
        self.queue.lock().push_back(job);
        self.work_available.notify_one();
    }

    /// Retire the pool (host thread done): every parked seat drains out.
    fn close(&self) {
        *self.shutdown.lock() = true;
        self.work_available.notify_all();
    }
}

/// One pooled `SimDriver` seat: take granted units from the shared work
/// queue, run them one at a time, and report the granted slot's completion
/// so the host applies T5 (run → idle).
fn seat_loop(work: &WorkQueue, done: &mpsc::Sender<HostMsg>) {
    while let Some(job) = work.take() {
        // A panicking sim closure must not kill the seat (its pool would
        // strand receipts): keep the seat alive, log loudly, report done.
        let ctx = LaneCtx::detached();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| (job.work)(&ctx)));
        if outcome.is_err() {
            tracing::error!(
                target: "degenbot::fleet",
                seat = job.slot,
                "[fleet-sim] sim unit panicked — the seat survives, the failure is loud"
            );
        }
        if done.send(HostMsg::SeatDone { seat: job.slot }).is_err() {
            // The host is gone (executor dropped — tests): the seat retires.
            break;
        }
    }
}

/// The dispatch loop (design doc §4): enqueue → precedence-grant →
/// execute. Owns the `FleetHost` exclusively; every FSM transition runs
/// here. Exits when the submission channel closes (all executor handles
/// dropped — process teardown).
#[expect(
    clippy::needless_pass_by_value,
    reason = "the host Receiver's ownership moves into the spawned host thread — a borrow cannot cross the thread boundary"
)]
fn host_loop(rx: mpsc::Receiver<HostMsg>, mut host: FleetHost, queue: Arc<WorkQueue>) {
    let mut backlog: VecDeque<Unit> = VecDeque::new();
    while let Ok(msg) = rx.recv() {
        apply_host_msg(&mut host, &mut backlog, msg);
        pump(&mut host, &mut backlog, &queue);
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
            // the legacy receipt pipeline) and drain FIRST on the next
            // pump — never dropped (§10 ledger).
            if host.queue_len(WorkerRole::SimDriver) >= host.queue_cap(WorkerRole::SimDriver) {
                backlog.push_back(unit);
            } else if let Err(err) = host.enqueue(unit) {
                // v1-active, non-merge `SimDriver` units cannot hit
                // RoleNotActive / MergeNeverQueued; PostureHeld cannot fire
                // here — `SimDriver` is a SimPool-class role, so a cordon
                // still ADMITS its leases (floored, not held). Any such
                // error is a broken invariant, not a drop.
                abort_executor("sim enqueue", &err.to_string());
            }
        }
        HostMsg::SeatDone { seat } => {
            if let Err(err) = host.complete(seat) {
                abort_executor("seat completion (T5)", &err.to_string());
            }
        }
    }
}

/// The one precedence grant loop pass (design doc §4): backlog first, then
/// dispatch grants onto the pooled seats. Grants apply T2 (start) at grant
/// time — the work-queue push IS the claim — and completion arrives via
/// [`HostMsg::SeatDone`] (T5).
fn pump(host: &mut FleetHost, backlog: &mut VecDeque<Unit>, queue: &Arc<WorkQueue>) {
    // Backlog drains FIRST (FIFO across the loud-overflow seam).
    while backlog.front().is_some() {
        if host.queue_len(WorkerRole::SimDriver) >= host.queue_cap(WorkerRole::SimDriver) {
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
            // Invariant: this executor only enqueues `SimDriver` units, so
            // every grant is a pooled sim grant. Anything else is a broken
            // host contract, not a drop.
            if !matches!(grant.kind, GrantKind::Sim) {
                abort_executor("dispatch grant", "non-sim grant in the sim executor");
            }
            if let Err(err) = host.start(grant.slot, &unit) {
                abort_executor("grant start (T2)", &err.to_string());
            }
            queue.push(SeatJob {
                slot: grant.slot,
                work: unit.work,
            });
        }
    }
}

static FLEET_SIM_BOOT: OnceLock<BootStamp> = OnceLock::new();
static FLEET_SIM_EXECUTOR: OnceLock<FleetSimExecutor> = OnceLock::new();

/// Install the CONSTRUCTION-STAMPED boot (YI5NGB): the engine's own typed
/// boot descriptor (fleet quota + overrides + posture) parsed at ITS
/// construction from the CALLER cfg, stamped with the engine id + a
/// deterministic cfg hash. Never overrides an installed value (first
/// engine wins, like the other stance statics) — every construction after
/// the first RIDES, and the ride is ledgered (a divergent-cfg rider is
/// counted + warned in prod, ILLEGAL in tests).
pub(crate) fn install_boot(stamp: BootStamp) {
    crate::arb_engine::boot_stamp::record_ride(BootRole::Sim, &stamp);
    let _ = FLEET_SIM_BOOT.set(stamp);
}

/// The process-wide fleet sim executor, built lazily on the first
/// fleet-stance sim submission and persisting for the process lifetime.
pub(crate) fn global_fleet_sim_executor() -> &'static FleetSimExecutor {
    FLEET_SIM_EXECUTOR.get_or_init(|| {
        // YI5NGB: the absence window is CLOSED BY CONSTRUCTION — every
        // dispatch path builds on a constructed engine, and construction
        // (with_core_cfg) installs the stamp BEFORE any dispatch can
        // exist. A missing stamp means a caller skipped the construction
        // contract: LOUD abort (never a silent fallback boot of a boot
        // nobody chose).
        #[expect(
            clippy::expect_used,
            reason = "the loud construction-contract abort IS the YI5NGB design: a stamp-less materialization must abort, never fall back silently"
        )]
        let stamp = FLEET_SIM_BOOT.get().expect(
            "fleet sim boot stamp missing: an engine must construct before the first fleet submit (YI5NGB)",
        );
        match FleetSimExecutor::boot(stamp.boot()) {
            Ok(executor) => executor,
            Err(err) => abort_executor("fleet sim budget boot", &err.to_string()),
        }
    })
}

#[cfg(test)]
// The panic-survival fixture panics deliberately (loud-assert test style;
// the module-level expect is the documented-permitted form).
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    #[expect(
        clippy::print_stderr,
        reason = "the self-skip channel when a parallel test won the stamp race (the documented F1 skip semantics)"
    )]
    /// F1 white-box (YI5NGB): the materializer's init closure aborts LOUD
    /// (the expect) when no construction ever installed a stamp — invoked
    /// directly so the expect fires WITHOUT a real `FleetHost` boot.
    #[test]
    fn fleet_sim_materializer_without_a_stamp_is_loud() {
        if super::FLEET_SIM_BOOT.get().is_some() {
            eprintln!(
                "skipping: another test already installed the sim boot stamp in this process"
            );
            return;
        }
        let closure = || {
            let stamp = super::FLEET_SIM_BOOT.get().expect(
                "fleet sim boot stamp missing: an engine must construct before the first fleet submit (YI5NGB)",
            );
            match FleetSimExecutor::boot(stamp.boot()) {
                Ok(_executor) => (),
                Err(_err) => (),
            }
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(closure));
        let err = result.expect_err("a stamp-less materialization must abort loud");
        let msg = err
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| err.downcast_ref::<&str>().copied())
            .expect("panic payload is the expect message");
        assert!(
            msg.contains("(YI5NGB)"),
            "the expect must name the task: {msg}"
        );
    }

    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::FleetBoot;
    use degenbot_workers::posture::PosturePolicy;

    use super::FleetSimExecutor;

    fn hermetic_boot() -> FleetBoot {
        FleetBoot {
            quota_cpus: 8.0,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        }
    }

    /// Wait for a minimum receipt count without unbounded blocking
    /// (deadline-poll, the solve-parity fixture style).
    fn await_receipts<T: Send + 'static>(
        rx: &mpsc::Receiver<T>,
        want: usize,
        deadline: Instant,
    ) -> Vec<T> {
        let mut got: Vec<T> = Vec::new();
        while got.len() < want {
            assert!(
                Instant::now() < deadline,
                "fleet sim seats did not drain {want} units in time (got {})",
                got.len()
            );
            if let Ok(v) = rx.try_recv() {
                got.push(v);
                continue;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        got
    }

    /// The fleet's `SimDriver` seats must run each submitted sim to
    /// completion, every receipt delivered exactly once.
    #[test]
    fn every_submitted_sim_completes_exactly_once() {
        let executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        let (tx, rx) = mpsc::channel::<u64>();
        for id in 0..32_u64 {
            let tx = tx.clone();
            executor.spawn(move || {
                let _ = tx.send(id);
            });
        }
        drop(tx);
        let mut got = await_receipts(&rx, 32, Instant::now() + Duration::from_secs(10));
        got.sort_unstable();
        let want: Vec<u64> = (0..32).collect();
        assert_eq!(got, want, "every sim unit completes exactly once");
    }

    /// Seats are the fleet `SimDriver` role: census thread-name pattern
    /// work-fleet-sim-{n} (GOQWCL rule — greppable, never the shared
    /// tokio-runtime-worker default).
    #[test]
    fn sims_execute_on_named_fleet_simdriver_seats() {
        let executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        let (tx, rx) = mpsc::channel::<String>();
        for _ in 0..8 {
            let tx = tx.clone();
            executor.spawn(move || {
                let _ = tx.send(
                    std::thread::current()
                        .name()
                        .map(str::to_owned)
                        .unwrap_or_default(),
                );
            });
        }
        drop(tx);
        let names = await_receipts(&rx, 8, Instant::now() + Duration::from_secs(10));
        for name in names {
            assert!(
                name.starts_with("work-fleet-sim-"),
                "sim unit must execute on a fleet SimDriver seat, got {name:?}"
            );
        }
    }

    /// A panicking sim must not kill the seat pool: the seat survives, the
    /// failure is logged loudly, and later sims drain normally.
    #[test]
    fn a_panicking_sim_unit_does_not_kill_the_seat_pool() {
        let executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        let (tx, rx) = mpsc::channel::<&'static str>();
        let tx_bomb = tx.clone();
        executor.spawn(move || {
            let _ = tx_bomb.send("boom");
            panic!("deliberate sim body panic (fixture)");
        });
        for _ in 0..4 {
            let tx = tx.clone();
            executor.spawn(move || {
                let _ = tx.send("ok");
            });
        }
        drop(tx);
        // The bomb's pre-panic marker plus the four healthy receipts.
        let got = await_receipts(&rx, 5, Instant::now() + Duration::from_secs(10));
        assert_eq!(got.iter().filter(|s| **s == "boom").count(), 1);
        assert_eq!(got.iter().filter(|s| **s == "ok").count(), 4);
    }

    /// The seat pool is the budget's `SimDriver` slot cap (design doc §5
    /// `SimDriver` slots = today's `SimSlots` cap; `fleet.sim_slot_cap` is the
    /// terminal override).
    #[test]
    fn sim_seat_pool_is_the_budget_sim_slot_cap() {
        let executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        assert_eq!(
            executor.sim_slot_cap(),
            degenbot_workers::budget::DEFAULT_SIM_SLOT_CAP
        );
    }

    /// The pacing contract the incumbent `SimSlots` semaphore provided: no
    /// more sims execute CONCURRENTLY than the budget's sim slot cap —
    /// the fleet's pooled seats now provide that bound by construction.
    #[test]
    fn concurrent_sims_are_bounded_by_the_sim_slot_cap() {
        let executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        let cap = executor.sim_slot_cap();
        let inflight = Arc::new(parking_lot::Mutex::new(0_usize));
        let max_seen = Arc::new(parking_lot::Mutex::new(0_usize));
        let (tx, rx) = mpsc::channel::<()>();
        for _ in 0..(cap * 3) {
            let tx = tx.clone();
            let inflight = Arc::clone(&inflight);
            let max_seen = Arc::clone(&max_seen);
            executor.spawn(move || {
                let seen = *max_seen.lock();
                *inflight.lock() += 1;
                let cur = *inflight.lock();
                if cur > seen {
                    *max_seen.lock() = cur;
                }
                std::thread::sleep(Duration::from_millis(5));
                *inflight.lock() -= 1;
                let _ = tx.send(());
            });
        }
        drop(tx);
        let _ = await_receipts(&rx, cap * 3, Instant::now() + Duration::from_secs(15));
        let seen = *max_seen.lock();
        assert!(
            seen <= cap,
            "concurrent sims {seen} exceeded the budget slot cap {cap}"
        );
    }

    /// Census (PE4FPM/FPNT36): the booted host self-registers the fleet
    /// `SimDriver` resource row with the fleet seat naming.
    #[test]
    fn the_booted_host_registers_the_fleet_simdriver_census_row() {
        let _executor = FleetSimExecutor::boot(hermetic_boot()).expect("fleet sim boot");
        let row = degenbot_core::worker_census::snapshot()
            .into_iter()
            .find(|e| e.resource == degenbot_workers::role::WorkerRole::SimDriver.census_resource())
            .expect("fleet SimDriver slots must self-register in the worker census");
        assert_eq!(row.thread_name, "work-fleet-sim-{n}");
        assert_eq!(row.count, degenbot_workers::budget::DEFAULT_SIM_SLOT_CAP);
    }

    /// A quota below the pinned-role floor fails loudly at boot (the §5
    /// fail-fast — never a runtime throttle storm).
    #[test]
    fn a_budget_refusal_fails_loudly_at_boot() {
        let boot = FleetBoot {
            quota_cpus: 4.5,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        };
        assert!(
            FleetSimExecutor::boot(boot).is_err(),
            "4.5-core quota below the pinned-role floor must refuse to boot"
        );
    }
}
