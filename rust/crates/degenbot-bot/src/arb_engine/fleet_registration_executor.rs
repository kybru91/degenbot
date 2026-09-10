//! Fleet-hosted registration intake executor (PRG-3 / ADR-042 F2):
//! `PoolStateUpdater`-role hosting of the registration crawl's pool-build
//! consumers — the fleet becomes the unit pool for the crawl's build work
//! under the `fleet.stance=Fleet` migration stance. The incumbent private
//! `ThreadPoolExecutor` (its own sizing rule — the per-era pile ADR-042 kills) retires onto the fleet's declared duty-counted
//! `PoolStateUpdater` slots; the legacy stance keeps the incumbent runtime
//! byte-for-byte.
//!
//! The seat pool is the budget's `pool_state_updater_slots` (default 4,
//! `fleet.pool_state_updater_slots` terminal override — the `SimDriver`
//! billing model exactly: duty-counted, spendable from the fractional
//! remainder, never part of the declared integer sum). Grants run strictly
//! BEHIND solve/sim/resolve precedence (host dispatch lane 5); a cordon
//! HOLDS intake entirely (Deferrable cordon class — `enqueue` refuses
//! while cordoned) and in-flight units are never cancelled.
//!
//! Unit bodies are the crawl's pool-build callables: they ride the FFI at
//! the seat boundary (`Python::attach` in the closure), release the GIL
//! through the existing `py.detach` seams inside the Rust builders, and
//! return receipts over the caller's channel — the identical behavior the
//! legacy worker threads had (parity gate), now bounded by the fleet's
//! declared slots and visible in the worker census as
//! `fleet_pool_state_updater_slots`.
//!
//! Units carry no pin key (the role is pooled, T5: run → back-to-idle);
//! keyed admission is ENGINE-side (PRG-1 build flights), so the host FSM's
//! pin/merge lanes never fire here. The `WrapDatabaseAsync` runtime-capture
//! caveat (ADR-042 §8) is unchanged: build callables already enter the
//! installed hooks via the existing seams.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, OnceLock};

use degenbot_workers::dispatcher::{BootError, FleetBoot, FleetHost, GrantKind, Unit};
use degenbot_workers::lane::LaneCtx;
use degenbot_workers::role::WorkerRole;

use crate::arb_engine::fleet_intake::{FleetIntake, InnerWork};

/// Loud, unrecoverable executor failure (mirror of the fleet sim/solve
/// executors' abort discipline): a dead host would strand in-flight build
/// receipts — the crawl worker awaiting one parks forever (stranded pipe,
/// design doc §10) — so swallowing the error is never an option.
#[expect(
    clippy::print_stderr,
    reason = "the abort path must stay legible with no tracing subscriber installed (test harnesses drop the tracing event); stderr is the process's last message"
)]
fn abort_executor(context: &str, err: &str) -> ! {
    tracing::error!(
        context = %context,
        error = %err,
        "[fleet-reg] unrecoverable — aborting (stranded intake receipt pipe)"
    );
    eprintln!(
        "[fleet-reg] UNRECOVERABLE, aborting (stranded intake receipt pipe): {context}: {err}"
    );
    std::process::abort();
}

/// Host-bound message: a submitted intake unit, or a seat reporting its
/// unit done (completion drives T5 — the pooled slot returns to idle).
enum HostMsg {
    Enqueue(Unit),
    SeatDone { seat: u64 },
}

/// One granted intake unit handed to whichever pooled seat takes it next
/// (the role is pooled — seats contend, no pin affinity).
struct SeatJob {
    /// The host-tracked slot the unit was granted to (completion carries it
    /// back so T5 applies to the right slot).
    slot: u64,
    /// The work payload (`Send + 'static` — the `c_api` closure carries the
    /// Python callable handle and its receipt channel). Takes the seat's
    /// `LaneCtx` (LW-T2); pooled seats hand the detached stub (LW-T8
    /// landed: the executors submit through ONE seam).
    work: Box<dyn FnOnce(&LaneCtx) + Send>,
}

/// The fleet-hosted registration intake executor. Shared by the whole
/// process (the global static hands out `&'static`, mirroring the fleet
/// sim/solve executors' construction-once contract: warm pooled seats for
/// the process lifetime).
pub struct FleetRegistrationExecutor {
    tx: mpsc::Sender<HostMsg>,
    unit_seq: AtomicU64,
    /// The budget's `PoolStateUpdater` slot cap (the pooled seat count).
    seats: usize,
}

impl FleetRegistrationExecutor {
    /// Boot from a `FleetBoot` (quota + overrides + posture): boot the
    /// [`FleetHost`], spawn the pooled `PoolStateUpdater` seat threads (the
    /// budget's station slot cap, `work-fleet-poolupd-{n}` census naming),
    /// and run the dispatch loop on the host thread. Fail-loud (the typed
    /// [`BootError`]) when the declared shares cannot host the quota.
    ///
    /// # Errors
    /// [`BootError`] — the fleet budget sum check or a boot invariant.
    pub fn boot(boot: FleetBoot) -> Result<Self, BootError> {
        let host = FleetHost::boot(boot)?;
        let seats = host.budget().pool_state_updater_slots;
        let (tx, rx) = mpsc::channel::<HostMsg>();
        // Pooled seats contend on ONE shared work queue: a grant lands a
        // unit there, any idle seat takes it, and the completion reports
        // the GRANTED slot id so the host applies T5 to the right slot.
        // Grants never exceed the station slot cap, which never exceeds the
        // seat count, so every granted unit is picked up without delay.
        let work = Arc::new(WorkQueue::new());
        for seat in 0..seats {
            let done = tx.clone();
            let work = Arc::clone(&work);
            let spawned = std::thread::Builder::new()
                .name(
                    WorkerRole::PoolStateUpdater
                        .thread_name()
                        .replace("{n}", &seat.to_string()),
                )
                .spawn(move || seat_loop(&work, &done));
            if let Err(err) = spawned {
                // A missing seat strands the receipts of every unit that
                // would have run on it — loud (§10).
                abort_executor("intake seat spawn", &format!("{err:?}"));
            }
        }
        let spawned = std::thread::Builder::new()
            .name("work-fleet-poolupd-host".to_string())
            .spawn(move || {
                host_loop(rx, host, Arc::clone(&work));
                // Process teardown: the submission channel closed. Retire
                // the seats so no worker parks forever on an empty queue.
                work.close();
            });
        if let Err(err) = spawned {
            abort_executor("fleet intake host thread spawn", &format!("{err:?}"));
        }
        Ok(Self {
            tx,
            unit_seq: AtomicU64::new(0),
            seats,
        })
    }

    /// The budget's `PoolStateUpdater` slot cap (the pooled seat count).
    /// Test-facing (the intake submits without asking the cap).
    #[must_use]
    pub fn seat_count(&self) -> usize {
        self.seats
    }

    /// Submit one intake unit. Never drops: the host-side backlog preserves
    /// the legacy unbounded-submission semantics; a closed host channel
    /// (executor died) is a LOUD abort — a lost build strands its awaiting
    /// crawl worker forever (stranded pipe, §10).
    pub fn spawn(&self, work: impl FnOnce() + Send + 'static) {
        let _ = self.try_send(Box::new(work));
    }

    /// The port's unit body, factored for the `FleetIntake` impl (the
    /// inherent `spawn` above delegates here): wraps the `InnerWork` unit
    /// and enqueues it over the host channel, typed to the port's
    /// `Result<(), ()>` close vocabulary. The channel-open arm returns
    /// `Ok`; the CLOSE arm's abort lives in the trait impl. `pub(crate)`
    /// fn, in-crate.
    pub(crate) fn try_send(&self, work: InnerWork) -> Result<(), ()> {
        let unit = Unit::new(
            self.unit_seq.fetch_add(1, Ordering::Relaxed),
            WorkerRole::PoolStateUpdater,
            None,
            // The unit's receipt feeds the awaiting crawl worker — a
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
}

impl FleetIntake for FleetRegistrationExecutor {
    fn spawn(&self, work: InnerWork) {
        if self.try_send(work).is_err() {
            abort_executor("intake submission", "fleet intake host channel closed");
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

/// One pooled `PoolStateUpdater` seat: take granted units from the shared
/// work queue, run them one at a time, and report the granted slot's
/// completion so the host applies T5 (run → idle).
fn seat_loop(work: &WorkQueue, done: &mpsc::Sender<HostMsg>) {
    while let Some(job) = work.take() {
        // A panicking build closure must not kill the seat (its pool would
        // strand receipts): keep the seat alive, log loudly, report done.
        let ctx = LaneCtx::detached();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| (job.work)(&ctx)));
        if outcome.is_err() {
            tracing::error!(
                target: "degenbot::fleet",
                seat = job.slot,
                "[fleet-reg] intake unit panicked — the seat survives, the failure is loud"
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
            if host.queue_len(WorkerRole::PoolStateUpdater)
                >= host.queue_cap(WorkerRole::PoolStateUpdater)
            {
                backlog.push_back(unit);
            } else if host.posture() == degenbot_workers::posture::FleetPosture::Cordoned {
                // A cordon HOLDs Deferrable intake (unlike the sim arm):
                // the unit waits in the backlog and re-queues when the
                // fleet exits the cordon (the legacy crawl threads were
                // never cancelled either; this is the deferrable stance,
                // §6). The posture machine is host-owned (single host
                // thread), so the check cannot race a cordon onset.
                backlog.push_back(unit);
            } else if let Err(err) = host.enqueue(unit) {
                abort_executor("intake enqueue", &err.to_string());
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
    // Backlog drains FIRST (FIFO across the loud-overflow seam). A backed
    // backlog that cannot enqueue (cordon holds intake, full per-role
    // queue) parks here until the next host message — the awaiting
    // callers already submitted, and retrying on every wake matches the
    // legacy worker semantics (work waits, never drops).
    while backlog.front().is_some() {
        let next_role = backlog
            .front()
            .map_or(WorkerRole::PoolStateUpdater, |u| u.role);
        if host.queue_len(next_role) >= host.queue_cap(next_role) {
            break;
        }
        // Still cordoned — the backlog head stays; retrying on the next
        // host message (submissions keep arriving; no busy-spin — pump
        // only runs on a message). The posture machine is host-owned
        // (single host thread), so the check cannot race a cordon onset.
        if host.posture() == degenbot_workers::posture::FleetPosture::Cordoned {
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
            // Invariant: this executor only enqueues `PoolStateUpdater`
            // units, so every grant is a pooled intake grant. Anything else
            // is a broken host contract, not a drop.
            if !matches!(grant.kind, GrantKind::PoolStateUpdate) {
                abort_executor("dispatch grant", "non-intake grant in the intake executor");
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

static FLEET_REGISTRATION_BOOT: OnceLock<FleetBoot> = OnceLock::new();
static FLEET_REGISTRATION_EXECUTOR: OnceLock<FleetRegistrationExecutor> = OnceLock::new();

/// Install the typed boot descriptor (fleet quota + overrides + posture)
/// parsed ONCE at engine construction from the config; the global executor
/// lazily consumes it on first fleet-stance intake submission. Never
/// overrides an installed value (first engine wins, like the other stance
/// statics).
pub fn install_boot(boot: FleetBoot) {
    let _ = FLEET_REGISTRATION_BOOT.set(boot);
}

/// Whether an engine installed a fleet boot descriptor — the intake is
/// hosted ONLY under the fleet stance (the legacy stance keeps the
/// incumbent `ThreadPoolExecutor` byte-for-byte).
#[must_use]
pub fn boot_installed() -> bool {
    FLEET_REGISTRATION_BOOT.get().is_some()
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

/// The process-wide fleet registration intake executor, built lazily on the
/// first fleet-stance intake submission and persisting for the process
/// lifetime.
pub fn global_fleet_registration_executor() -> &'static FleetRegistrationExecutor {
    FLEET_REGISTRATION_EXECUTOR.get_or_init(|| {
        let boot = FLEET_REGISTRATION_BOOT
            .get()
            .copied()
            .unwrap_or_else(fallback_boot);
        match FleetRegistrationExecutor::boot(boot) {
            Ok(executor) => executor,
            Err(err) => abort_executor("fleet intake budget boot", &err.to_string()),
        }
    })
}

#[cfg(test)]
// The panic-survival fixture panics deliberately (loud-assert test style;
// the module-level expect is the documented-permitted form). Mirror of the
// fleet sim executor's fixture set.
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::FleetBoot;
    use degenbot_workers::posture::PosturePolicy;

    use super::FleetRegistrationExecutor;

    fn hermetic_boot() -> FleetBoot {
        FleetBoot {
            quota_cpus: 8.0,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        }
    }

    fn await_receipts<T: Send + 'static>(
        rx: &mpsc::Receiver<T>,
        want: usize,
        deadline: Instant,
    ) -> Vec<T> {
        let mut got: Vec<T> = Vec::new();
        while got.len() < want {
            assert!(
                Instant::now() < deadline,
                "fleet intake seats did not drain {want} units in time (got {})",
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

    /// The intake seats must run each submitted build unit to completion,
    /// every receipt delivered exactly once — the never-drop contract the
    /// legacy crawl worker threads provided.
    #[test]
    fn every_submitted_intake_unit_completes_exactly_once() {
        let executor = FleetRegistrationExecutor::boot(hermetic_boot()).expect("fleet intake boot");
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
        assert_eq!(got, want, "every intake unit completes exactly once");
    }

    /// Seats are the fleet `PoolStateUpdater` role: census thread-name
    /// pattern work-fleet-poolupd-{n} (GOQWCL rule).
    #[test]
    fn intake_units_execute_on_named_fleet_poolupd_seats() {
        let executor = FleetRegistrationExecutor::boot(hermetic_boot()).expect("fleet intake boot");
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
                name.starts_with("work-fleet-poolupd-"),
                "intake unit must execute on a fleet PoolStateUpdater seat, got {name:?}"
            );
        }
    }

    /// A panicking build closure must not kill its seat (the pool would
    /// strand the awaiting crawl workers' receipts): the surviving seat
    /// still drains later units.
    #[test]
    fn a_panicking_build_leaves_the_seat_alive() {
        let executor = FleetRegistrationExecutor::boot(hermetic_boot()).expect("fleet intake boot");
        // Panic in the middle of the flood.
        executor.spawn(move || panic!("deliberate build failure"));
        let (tx, rx) = mpsc::channel::<u64>();
        for id in 0..8_u64 {
            let tx = tx.clone();
            executor.spawn(move || {
                let _ = tx.send(id);
            });
        }
        drop(tx);
        let got = await_receipts(&rx, 8, Instant::now() + Duration::from_secs(10));
        assert_eq!(got.len(), 8, "the seat pool survived the panic and drained");
    }

    /// The cordon HOLDS Deferrable intake, but submitted units are never
    /// dropped: they wait and drain when the seat pool next gets capacity.
    /// (In-process cordon orchestration is the posture crate's fixture
    /// domain — `dispatcher::tests::intake_units_admit_nominal_and_held_...`
    /// proves the FSM hold; this executor-level test proves the backlog
    /// preserves units across a queue-full spill.)
    #[test]
    fn a_full_per_role_queue_never_drops_units() {
        let executor = FleetRegistrationExecutor::boot(hermetic_boot()).expect("fleet intake boot");
        let cap = executor.seat_count();
        let (tx, rx) = mpsc::channel::<u64>();
        // A flood far past the seat pool AND the 2x queue bound.
        for id in 0..(4 * cap + 8) as u64 {
            let tx = tx.clone();
            executor.spawn(move || {
                let _ = tx.send(id);
            });
        }
        drop(tx);
        let want = (4 * cap + 8) as u64;
        let got = await_receipts(
            &rx,
            usize::try_from(want).unwrap_or(usize::MAX),
            Instant::now() + Duration::from_secs(30),
        );
        assert_eq!(
            got.len() as u64,
            want,
            "no unit dropped across the backlog spill"
        );
    }
}
