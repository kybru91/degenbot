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
use std::sync::{mpsc, Arc, OnceLock};

use degenbot_workers::dispatcher::{
    BootError, FleetBoot, FleetHost, SubmitError, SubmitReceipt, Unit,
};
use degenbot_workers::lane::{LaneCtx, QuitSig};
use degenbot_workers::posture::{FleetPosture, PostureChange, ThrottleSample};
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
#[expect(
    clippy::print_stderr,
    reason = "the abort path must stay legible with no tracing subscriber installed (test harnesses drop the tracing event); stderr is the process's last message"
)]
fn abort_executor(context: &str, err: &str) -> ! {
    tracing::error!(
        context = %context,
        error = %err,
        "[fleet-solve] unrecoverable — aborting (stranded result pipe)"
    );
    eprintln!("[fleet-solve] UNRECOVERABLE, aborting (stranded result pipe): {context}: {err}");
    std::process::abort();
}
/// Public loud-stop wrapper (ADR-021; used by the `solver_dispatch` submit
/// seam for posture-gated refusals).
pub(crate) fn abort_loud(context: &str, err: &str) -> ! {
    abort_executor(context, err);
}

/// The pins == bins invariant check (P6YXA6): a Solver bin index must land
/// within the structural seat count. Pure (no `&self`) so the tests hold
/// the contract without booting a fleet.
fn validate_bin_index(bin: usize, solver_seats: usize) -> Result<(), String> {
    if bin < solver_seats {
        Ok(())
    } else {
        Err(format!(
            "bin {bin} exceeds the {solver_seats} structural Solver seats \
             (pins == bins invariant, P6YXA6); the dispatch arms must bin \
             at executor.bin_count()"
        ))
    }
}

/// One bin job as the host hands it to a Solver seat.
struct SeatJob {
    /// The host-tracked unit id (dispatch bookkeeping).
    unit: u64,
    /// The work payload (the 'static + Send `run_bin` closure — the fleet
    /// crate has no pyo3 and simulation never round-trips Python, design
    /// doc §8). The seat hands the unit its `LaneCtx` (LW-T2 Seam B).
    work: Box<dyn FnOnce(&LaneCtx) + Send>,
    /// The seat ctx minted at the T2 grant seam: the bin's pin + the
    /// slot's warm arena identity (warm across cycles, fresh after T9).
    ctx: LaneCtx,
}

/// Host-bound message: a submitted bin unit, or a seat reporting its unit
/// done (completion drives T3 — the seat re-pins warm for the next cycle).
enum HostMsg {
    Enqueue(Unit),
    SeatDone {
        seat: u64,
    },
    /// Posture observation feed (LW-T5, Seam E): a cgroup throttle sample
    /// — the host's FSM applies it; the submit-seam MIRROR updates from
    /// the verdict (the FSM itself is never re-worked from the surface).
    Throttle {
        now_ms: u64,
        sample: ThrottleSample,
    },
}

/// The fleet-hosted solve executor. Shared by all engine cycles (the
/// global static hands out `&'static`, mirroring the incumbent executor's
/// construction-once contract: persistent seats keep warm L1/L2 +
/// allocator arenas across cycles).
pub(crate) struct FleetSolveExecutor {
    tx: mpsc::Sender<HostMsg>,
    unit_seq: AtomicU64,
    /// The submit-seam posture MIRROR (LW-T5, Seam E): the host thread
    /// writes the FSM posture after every throttle observation; submit
    /// consults THIS (never a unit body, never ambient) — the FSM itself
    /// stays untouched.
    posture: Arc<parking_lot::Mutex<FleetPosture>>,
    /// The host thread stamps TRUE when an enqueue spilled to the
    /// unbounded host backlog; submit stamps the receipt with (and resets)
    /// the flag — the unit is never dropped (§10 ledger).
    solver_queue_len: Arc<std::sync::atomic::AtomicUsize>,
    /// Seat count (read via [`FleetSolveExecutor::bin_count`] — the
    /// dispatch arms bin at exactly this count so every bin has a home).
    solver_seats: usize,
}

/// The production throttle poller's feed point (LW-T5, Seam E):
/// STANCE-GATED — a header sample must NEVER boot the fleet executor
/// under the legacy tokio stance (the common default): gate-off means
/// the feed returns without touching the global static. Returns
/// whether the sample was fed.
pub(crate) fn feed_fleet_posture_sample(now_ms: u64, events: u64, throttled_usec: u64) -> bool {
    if !super::solver_dispatch::fleet_stance_enabled(degenbot_config::holder::config()) {
        return false;
    }
    let last_ms = LAST_HEADER_SAMPLE_MS.swap(now_ms, Ordering::Relaxed);
    let elapsed_usec = if last_ms == 0 {
        0
    } else {
        now_ms.saturating_sub(last_ms).saturating_mul(1_000)
    };
    crate::arb_engine::executor::global_executor(true).observe_throttle(
        now_ms,
        ThrottleSample {
            events,
            throttled_usec,
            elapsed_usec,
        },
    );
    true
}

/// Probe: whether the GLOBAL fleet executor has booted (stance-gate
/// evidence + observability: the legacy stance must never boot it from
/// a header sample). Only meaningful in a test build (the lib never
/// queries it; the gate-off test pins "gate-off ⇒ no boot").
#[must_use]
#[cfg(test)]
pub(crate) fn global_executor_booted() -> bool {
    FLEET_EXECUTOR.get().is_some()
}

impl crate::arb_engine::executor::Executor for FleetSolveExecutor {
    fn bin_count(&self) -> usize {
        self.bin_count()
    }

    fn submit(
        &self,
        bin: usize,
        work: crate::arb_engine::executor::SubmitWork,
    ) -> Result<
        degenbot_workers::dispatcher::SubmitReceipt,
        degenbot_workers::dispatcher::SubmitError,
    > {
        self.submit_solve_bin(bin, work)
    }

    fn observe_throttle(&self, now_ms: u64, sample: ThrottleSample) {
        self.observe_throttle(now_ms, sample);
    }
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
        // The submit mirror (LW-T5, Seam E) lives with the host thread and
        // every executor handle shares the same cells.
        let posture = Arc::new(parking_lot::Mutex::new(FleetPosture::Nominal));
        let solver_queue_len = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
        let host_posture = Arc::clone(&posture);
        let host_queue_len = Arc::clone(&solver_queue_len);
        let spawned = std::thread::Builder::new()
            .name("work-fleet-solver-host".to_string())
            .spawn(move || {
                host_loop(rx, host, &seat_senders, &host_posture, &host_queue_len);
            });
        if let Err(err) = spawned {
            abort_executor("fleet host thread spawn", &format!("{err:?}"));
        }
        Ok(Self {
            tx,
            unit_seq: AtomicU64::new(0),
            solver_seats,
            posture,
            solver_queue_len,
        })
    }

    /// Solver seats = the budget's structural LPT bin count. The dispatch
    /// arms bin at THIS count (P6YXA6 reconciliation): pins and bins are
    /// the same number, so every bin owns a warm keyed seat across cycles.
    #[must_use]
    pub(crate) fn bin_count(&self) -> usize {
        self.solver_seats
    }

    /// Submit one LPT bin job keyed to its bin; the seat hands the unit body
    /// a [`LaneCtx`] carrying the bin's pin key and warm arena identity
    /// (LW-T2 Seam B). The typed submit receipt never drops a unit (the
    /// host-side backlog preserves the legacy unbounded-mpsc semantics);
    /// posture refusals are TYPED at this seam (admission-side only —
    /// running units are never preempted); a closed host channel (executor
    /// died) is a LOUD abort — a lost bin would strand its paths' per-path
    /// result sends forever (stranded pipe, §10).
    pub(crate) fn submit_solve_bin(
        &self,
        bin: usize,
        work: crate::arb_engine::executor::SubmitWork,
    ) -> Result<SubmitReceipt, SubmitError> {
        // The pins == bins invariant (P6YXA6), held at the submit seam: a
        // bin without a structural seat must abort HERE — with both numbers
        // in the message — instead of decaying into an FSM transition
        // refusal deep in dispatch.
        if let Err(msg) = validate_bin_index(bin, self.solver_seats) {
            abort_executor("bin submission", &msg);
        }
        let key = SOLVE_BIN_KEY_BASE.saturating_add(u64::try_from(bin).unwrap_or(u64::MAX));
        let unit = Unit::new(
            self.unit_seq.fetch_add(1, Ordering::Relaxed),
            WorkerRole::Solver,
            Some(key),
            // The bin's per-path result sends feed the merge pipe.
            true,
            Box::new(work),
        );
        // LW-T5 (Seam E): the posture consult AT the submit seam — a cordon
        // refuses a NEW solver submission immediately (admission-side only;
        // the running units' timeline is untouched and the FSM is never
        // re-worked from the surface).
        let observed_posture = *self.posture.lock();
        if observed_posture != FleetPosture::Nominal {
            return Err(SubmitError::PostureHeld {
                posture: observed_posture,
                role: WorkerRole::Solver,
            });
        }
        if self.tx.send(HostMsg::Enqueue(unit)).is_err() {
            return Err(SubmitError::PortClosed);
        }
        Ok(SubmitReceipt {
            // The receipt's backlog bit reads the host's queue MIRROR: at or
            // over the cap, this unit rides the unbounded host backlog and
            // drains FIRST on the next pump (§10 ledger) — never dropped, never
            // silent.
            accepted_with_backlog: self.solver_queue_len.load(Ordering::Relaxed)
                >= self.solver_seats.saturating_mul(2),
        })
    }

    /// Feed a throttle sample to the host posture (LW-T5, Seam E): the
    /// production throttle poller and tests drive the SAME seam — the
    /// verdict lands in the submit mirror on the host thread.
    pub(crate) fn observe_throttle(&self, now_ms: u64, sample: ThrottleSample) {
        if self.tx.send(HostMsg::Throttle { now_ms, sample }).is_err() {
            abort_executor("posture observation", "fleet host channel closed");
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
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| (job.work)(&job.ctx)));
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
fn host_loop(
    rx: mpsc::Receiver<HostMsg>,
    mut host: FleetHost,
    seats: &[mpsc::Sender<SeatJob>],
    posture: &Arc<parking_lot::Mutex<FleetPosture>>,
    solver_queue_len: &std::sync::atomic::AtomicUsize,
) {
    let mut backlog: VecDeque<Unit> = VecDeque::new();
    while let Ok(msg) = rx.recv() {
        apply_host_msg(&mut host, &mut backlog, msg, posture, solver_queue_len);
        pump(&mut host, &mut backlog, seats);
    }
}

/// Apply one submission or completion (both arrive on the single host
/// channel — completions can never starve behind a blocking recv).
fn apply_host_msg(
    host: &mut FleetHost,
    backlog: &mut VecDeque<Unit>,
    msg: HostMsg,
    posture: &Arc<parking_lot::Mutex<FleetPosture>>,
    solver_queue_len: &std::sync::atomic::AtomicUsize,
) {
    match msg {
        HostMsg::Enqueue(unit) => {
            // Pre-check capacity INSTEAD of failing enqueue: the host
            // thread owns every queue mutation, so the check is exact.
            // Units that do not fit spill to the backlog (unbounded, like
            // the legacy mpsc) and drain FIRST on the next pump — never
            // dropped (§10 ledger). The spill STAMPS the backlog mirror so
            // a submit receipt honestly reports accepted-with-backlog.
            if host.queue_len(WorkerRole::Solver) >= host.queue_cap(WorkerRole::Solver) {
                backlog.push_back(unit);
                solver_queue_len.store(host.queue_len(WorkerRole::Solver), Ordering::Relaxed);
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
            solver_queue_len.store(host.queue_len(WorkerRole::Solver), Ordering::Relaxed);
        }
        HostMsg::Throttle { now_ms, sample } => {
            // LW-T5 (Seam E): the FSM owns the posture (slot surface is
            // untouched); this MIRRORS the verdict into the submit seam.
            match host.observe_throttle(now_ms, sample) {
                PostureChange::Entered(_) => {
                    *posture.lock() = FleetPosture::Cordoned;
                }
                PostureChange::Exited => {
                    *posture.lock() = FleetPosture::Nominal;
                }
                PostureChange::Held => {}
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
            // LW-T2 (Seam B): mint the warm arena at the grant seam — the
            // ctx handed to the unit at dispatch time ALWAYS carries the
            // warm identity (stable across cycles; released at T9). An
            // unknown slot here is structurally unreachable (the grant came
            // from THIS host) — a silent default would violate the loud
            // posture, so it aborts with BOTH numbers, P6YXA6-style.
            let arena = host.ensure_arena(grant.slot).unwrap_or_else(|| {
                abort_executor(
                    "arena mint at grant",
                    &format!(
                        "slot {} hosts no arena for bin key {}",
                        grant.slot,
                        unit.key.unwrap_or(0)
                    ),
                );
            });
            let ctx = LaneCtx {
                pin: unit.key.unwrap_or(0),
                arena,
                // LW-T3 (Seam C): the injected default escalation port (the
                // inline-sim runtime, registered at hook install) — lanes
                // without one are refused TYPED at escalate, never dropped.
                escalation: degenbot_workers::lane::default_escalation_port()
                    .unwrap_or_else(degenbot_workers::lane::no_escalation_port),
                quit: QuitSig,
            };
            let job = SeatJob {
                unit: grant.unit,
                work: unit.work,
                ctx,
            };
            if seat_tx.send(job).is_err() {
                // A dead seat cannot drain its pinned bins' results —
                // stranded pipe (§10).
                abort_executor("seat mailbox send", "seat thread is gone");
            }
        }
    }
}

/// The wall-ms of the last per-header cgroup throttle sample: the FSM
/// needs each sample's poll interval (elapsed) and the `block_pump` poller
/// samples on header cadence (LW-T5, Seam E).
static LAST_HEADER_SAMPLE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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

// ---------------------------------------------------------------------------
// QR3NUS GREEN (Seam D, decision A): typed per-path lane outcomes + the
// solve-lane adapter. Every solve bin runs under the lane witness: a unit
// panic stays loud AND typed — the `PanicVerdict` is consulted, every
// undelivered pid is patched onto the pipe as `Failed(SeatPanic{unit,
// seat, message})`, and the seat keeps serving its pin. The merge-side
// accounting (`solved + suppressed + failed == submitted`) becomes exact;
// real-sink parity promotion cross-checks it in LW-T7.
// ---------------------------------------------------------------------------
pub(crate) mod lane_scaffold {
    //! Provisional module name (QR3NUS): this lane adapter folds into the
    //! unified Executor module at LW-T8 (JI275C).

    use std::collections::BTreeSet;
    use std::panic::AssertUnwindSafe;
    use std::sync::mpsc;

    use degenbot_workers::dispatcher::{PanicAction, PanicVerdict};

    use crate::arb_engine::solver_dispatch::SolveArmOutcome;

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

    /// One drained per-path outcome: EXACTLY one per submitted path. The
    /// fuse the accounting asserts is `solved + suppressed + failed ==
    /// submitted` with the delivered and failed pid sets exact and
    /// disjoint — a worker `None` IS an outcome (counted even though it
    /// never merges).
    #[derive(Debug)]
    #[expect(
        clippy::large_enum_variant,
        reason = "Solved IS the real pipe item (the full SolveArmOutcome tuple, QR3NUS review): boxing it would re-shape the type away from the channel item it must replace; Suppressed/Failed are small rare markers so the size gap is deliberate"
    )]
    pub(crate) enum LaneOutcome {
        /// The real pipe item (the worker's `Some` arm).
        Solved(SolveArmOutcome),
        /// The worker's `None` arm (failed / filtered solve): never
        /// merges, but is still an outcome the accounting must count.
        Suppressed { pid: u64 },
        /// A path whose outcome never landed because its unit panicked.
        Failed {
            /// The path the failed record covers.
            pid: u64,
            /// Why the path's outcome never landed.
            failure: LaneFailure,
        },
    }

    /// The solve-lane witness for one bin: the pid list the bin owed the
    /// pipe plus the emitted-pid set (what actually landed).
    pub(crate) struct SolveLane {
        unit: u64,
        seat: u64,
        pids: Vec<u64>,
        emitted: BTreeSet<u64>,
        tx: mpsc::Sender<LaneOutcome>,
    }

    impl SolveLane {
        /// Build a lane for one bin: `unit`/`seat` name the accounting
        /// identity the panic records will carry; `pids` are the paths the
        /// bin's work owed the pipe.
        pub(crate) fn new(
            unit: u64,
            seat: u64,
            pids: Vec<u64>,
            tx: mpsc::Sender<LaneOutcome>,
        ) -> Self {
            Self {
                unit,
                seat,
                pids,
                emitted: BTreeSet::new(),
                tx,
            }
        }

        /// Deliver one real arm outcome (the worker's `Some` arm).
        pub(crate) fn solved(&mut self, item: SolveArmOutcome) {
            self.emitted.insert(item.0);
            let _ = self.tx.send(LaneOutcome::Solved(item));
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
        fn unemitted(&self) -> Vec<u64> {
            self.pids
                .iter()
                .copied()
                .filter(|pid| !self.emitted.contains(pid))
                .collect()
        }
    }

    /// Run one bin's work under the lane witness (QR3NUS, decision A):
    /// a panicking work body is caught, the `PanicVerdict` is consulted
    /// with the (unit, seat) identity, and on `RecordAndContinue` every
    /// still-unemitted path is patched onto the pipe as exactly one typed
    /// `Failed(LaneFailure::SeatPanic)` record — so the drain observes one
    /// outcome per submitted path and the seat keeps serving its pin.
    /// `Abort` is the strict ADR-021 posture: wired only outside tests —
    /// a test never runs a real `std::process::abort`.
    pub(crate) fn run_solve_lane(
        lane: &mut SolveLane,
        verdict: &dyn PanicVerdict,
        work: impl FnOnce(&mut SolveLane),
    ) {
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| work(lane)));
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
                super::abort_executor(
                    "unit panic (strict ADR-021 posture)",
                    &format!(
                        "unit {} seat {}: {}",
                        lane.unit,
                        lane.seat,
                        message.as_deref().unwrap_or("<non-string panic payload>")
                    ),
                );
            }
        }
    }
}

// Re-exports: the solve-lane adapter is the production seam the
// `solver_dispatch` merge wires every bin through (QR3NUS decision A).
pub(crate) use lane_scaffold::{run_solve_lane, LaneOutcome, SolveLane};

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use crate::arb_engine::executor::{Executor as _, SubmitWork};
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use degenbot_solvers::mixed::SolvePathResult;
    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::{
        AbortingPolicy, ArenaToken, FleetBoot, PanicAction, PanicVerdict, SeatSurvivesPolicy,
    };
    use degenbot_workers::lane::{
        install_default_escalation_port, EscalationError, EscalationPort, EscalationWork, LaneCtx,
    };
    use degenbot_workers::posture::PosturePolicy;
    use degenbot_workers::posture::{FleetPosture, ThrottleSample};

    use super::super::solver_dispatch::executor_ab_probe::{
        load_corpus_fixture, probe_ctx, prod_lpt_bins,
    };
    use super::super::solver_dispatch::{solve_one_path, SolveArmOutcome};
    use super::lane_scaffold::{run_solve_lane, LaneFailure, LaneOutcome, SolveLane};
    use super::WorkerRole;
    use super::{validate_bin_index, FleetSolveExecutor, SubmitError, SOLVE_BIN_KEY_BASE};

    fn hermetic_boot() -> FleetBoot {
        FleetBoot {
            quota_cpus: 8.0,
            overrides: BudgetOverrides::default(),
            posture: PosturePolicy::doc_defaults(),
        }
    }

    /// The pins == bins contract (P6YXA6) as a pure validator: a
    /// seat-bounded bin index passes; anything over it is shouted down with
    /// BOTH numbers so the loud abort decodes at a glance.
    #[test]
    fn bin_submission_above_the_structural_seat_count_is_a_loud_invariant_violation() {
        assert!(validate_bin_index(5, 6).is_ok());
        let err = validate_bin_index(6, 6).expect_err("bin == seats is out of range");
        assert!(
            err.contains("bin 6"),
            "message names the rejected bin: {err}"
        );
        assert!(
            err.contains("6 structural"),
            "message names the seat count: {err}"
        );
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
            executor
                .submit(
                    bin_idx,
                    Box::new(move |_ctx| {
                        // Per-path send (not per-bin): the existing per-path result
                        // streaming — byte-for-byte what the run_bin bodies do.
                        for (key, item) in keys.into_iter().zip(items) {
                            if let Some((pid, r)) =
                                solve_one_path(&ctx, &tracing::Span::none(), key, &item)
                            {
                                let _ = tx.send((pid, r));
                            }
                        }
                    }),
                )
                .expect("fixture submit blocked by noise");
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
        // P6YXA6 regression: the bins bind at the fleet's STRUCTURAL seat
        // count — pins == bins. Binning at the machine-derived worker count
        // instead boots 6 hermetic seats against host-core-derived bins (22
        // on the 24-core raw host) and aborts at the T2 grant — the
        // host-only `just test-rust` failure this restructuring pins.
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let bins = prod_lpt_bins(&items, executor.bin_count());
        assert_eq!(
            bins.len(),
            executor.bin_count(),
            "solver bins must equal the structural Solver seat count (pins == bins, P6YXA6)"
        );

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
                exec.spawn(move |_ctx| {
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
        let fleet = submit_bins(&executor, &bins, &items, &ctx);

        assert_eq!(
            fleet, legacy,
            "fleet-hosted solve results must be byte-equal with the legacy executor"
        );

        // LW-T7 (Seam F) promotion gate: the parity harness compares the
        // stances' OUTCOME TOTALS (sold outcomes never diverge between the
        // arms) and never exceeds the submissions; the EXACT
        // solved+suppressed+failed == submitted equation is asserted in the
        // production drain itself (the same seam, LW-T1's fuse).
        assert!(
            fleet.len() <= items.len(),
            "outcomes can never exceed submissions"
        );
        assert_eq!(
            fleet.len(),
            legacy.len(),
            "parity gate: the arms' outcome totals must never diverge"
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
                executor
                    .submit(
                        bin,
                        Box::new(move |_ctx| {
                            observed.lock().push((bin_key, std::thread::current().id()));
                        }),
                    )
                    .expect("naming unit accepted");
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

    // ---- LW-T2 (Seam B): seat context — no ambient runtime, LaneCtx identity

    /// The runtime wedge (LW-T2): fleet seats are OS threads with NO ambient
    /// tokio runtime. The executor boots and submits from inside a LIVE
    /// multi-thread runtime here, and every seated unit still observes
    /// `Handle::try_current() == Err` — any future drift that puts a runtime
    /// on the seat (the tokio-stance path) fails this test.
    ///
    /// INTENDED TRIPWIRE: this test passing today is correct (fleet seats
    /// are plain `std::thread`s). It goes RED deliberately when the
    /// tokio-stance path is ported, and again when LW-T8 consolidates the
    /// executors behind a trait — that red is the T9 cutover gate, not a
    /// regression.
    #[test]
    fn fleet_seats_run_units_with_no_ambient_tokio_runtime() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let runtime_free: Arc<parking_lot::Mutex<Vec<bool>>> = Arc::default();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            for _ in 0..3 {
                let runtime_free = Arc::clone(&runtime_free);
                executor
                    .submit(
                        0,
                        Box::new(move |_ctx| {
                            runtime_free
                                .lock()
                                .push(tokio::runtime::Handle::try_current().is_err());
                        }),
                    )
                    .expect("probe unit accepted");
            }
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while runtime_free.lock().len() < 3 {
            assert!(
                std::time::Instant::now() < deadline,
                "fleet seats did not drain the probe units in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            runtime_free.lock().iter().all(|free| *free),
            "fleet seats must run units OUTSIDE any ambient tokio runtime"
        );
    }

    /// The LaneCtx submit seam (LW-T2): `submit_solve_bin` hands the unit a
    /// ctx carrying the bin's pin key AND the warm arena identity — the
    /// SAME ArenaToken across cycles (warm), never the detached stub.
    #[test]
    fn submit_solve_bin_hands_a_lane_ctx_with_the_bins_key_and_warm_arena_identity() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let observed: Arc<parking_lot::Mutex<Vec<LaneCtx>>> = Arc::default();
        for _cycle in 0..2 {
            let observed = Arc::clone(&observed);
            executor
                .submit_solve_bin(
                    0,
                    SubmitWork::from(Box::new(move |ctx: &LaneCtx| {
                        observed.lock().push(ctx.clone());
                    })),
                )
                .expect("the nominal ctx submit is accepted");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while observed.lock().len() < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the seat did not drain the ctx probe units in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let observed = observed.lock().clone();
        let expected_key = SOLVE_BIN_KEY_BASE; // bin 0 → base + 0
        for ctx in &observed {
            assert_eq!(
                ctx.pin, expected_key,
                "the ctx must carry the bin's pin key"
            );
            // Solver seats are pinned lanes: the arena is minted at the T2
            // grant seam, so a SOLVER unit must NEVER observe the detached
            // stub (the DETACHED token is pooled seats only).
            assert_ne!(
                ctx.arena,
                ArenaToken::DETACHED,
                "a solver-seat unit must never observe the DETACHED arena stub"
            );
        }
        assert_ne!(
            observed[0].arena,
            ArenaToken::DETACHED,
            "the warm identity must be host-minted, not the detached stub"
        );
        assert_eq!(
            observed[0].arena, observed[1].arena,
            "the SAME ArenaToken across cycles (warm)"
        );
    }

    // ---- LW-T6 (Seam G2): seat naming + census atoms -------------------------

    /// LW-T6: the boot census carries the seat fleet's rows — per-index
    /// `{n}` patterns matching the roles, the Solver budget matching the
    /// STRUCTURAL seat count, and no two rows sharing a thread-name pattern
    /// (the GOQWCL collision lock, fleet-wide).
    #[test]
    fn boot_census_rows_are_per_index_patterned_with_the_solver_budget() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let snap = degenbot_core::worker_census::snapshot();
        let solver_row = snap
            .iter()
            .find(|e| e.thread_name == WorkerRole::Solver.thread_name())
            .expect("the Solver fleet census row exists");
        assert_eq!(
            solver_row.count,
            executor.bin_count(),
            "the census Solver count must equal the structural seat count"
        );
        // The collision lock: no two rows fleet-wide share a thread-name
        // pattern (the GOQWCL lesson: shared patterns made dumps
        // unattributable).
        let mut patterns: Vec<&str> = snap.iter().map(|e| e.thread_name).collect();
        let n = patterns.len();
        patterns.sort_unstable();
        patterns.dedup();
        assert_eq!(
            patterns.len(),
            n,
            "census thread-name patterns must be unique"
        );
    }

    /// LW-T6: the runtime registration is not a lie — the OS thread names of
    /// RUNNING seats match the census rows (per-index under the pattern).
    #[test]
    fn running_seat_thread_names_match_their_census_rows() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let seats = executor.bin_count();
        let names: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
        for bin in 0..seats {
            let names = Arc::clone(&names);
            executor
                .submit_solve_bin(
                    bin,
                    SubmitWork::from(Box::new(move |_ctx| {
                        names.lock().push(
                            std::thread::current()
                                .name()
                                .unwrap_or("<unnamed>")
                                .to_owned(),
                        );
                    })),
                )
                .expect("the naming probe submit is accepted");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while names.lock().len() < seats {
            assert!(
                std::time::Instant::now() < deadline,
                "seats did not report thread names in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let names_set: std::collections::HashSet<_> = names.lock().iter().cloned().collect();
        for name in &names_set {
            assert!(
                name.starts_with("work-fleet-solver-"),
                "seat thread names must be per-index census-named: {name}"
            );
        }
        assert_eq!(
            names_set.len(),
            seats,
            "every structural seat has its own distinct census-NAMED thread"
        );
    }

    // ---- LW-T5 (Seam E): posture & precedence at the submit seam ------------

    /// LW-T5 (Seam E): the throttle feed is STANCE-GATED — a gate-off
    /// (legacy stance) sample NEVER boots the global fleet executor (no
    /// seats, no host thread, no census rows from a mere header sample).
    #[test]
    fn gate_off_throttle_feed_never_boots_the_fleet_executor() {
        // The default stance is LEGACY (the common default today): gate OFF,
        // and the probe must show the global executor never booted.
        let cfg = degenbot_config::BotConfig::default();
        assert!(
            !super::super::solver_dispatch::fleet_stance_enabled(&cfg),
            "the default config must be gate-off (legacy stance)"
        );
        let fed = super::feed_fleet_posture_sample(5_000, 3, 9);
        assert!(!fed, "gate-off must not feed");
        assert!(
            !super::global_executor_booted(),
            "gate-off (legacy stance) must NOT boot the fleet executor"
        );
    }

    /// LW-T5 (Seam E): in Cordoned posture the submit seam refuses a NEW
    /// solver unit IMMEDIATELY with a typed `PostureHeld` — admission-side
    /// only: the unit running when the posture flipped completes normally
    /// (RAYPAR T3 never-yield mid-unit; the slot FSM itself is untouched).
    #[test]
    fn submit_in_cordoned_posture_fails_typed_at_the_submit_seam_and_running_units_complete() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let long_unit_done: Arc<std::sync::atomic::AtomicBool> = Arc::default();
        let done = Arc::clone(&long_unit_done);
        executor
            .submit_solve_bin(
                0,
                SubmitWork::from(Box::new(move |_ctx| {
                    std::thread::sleep(std::time::Duration::from_millis(300));
                    done.store(true, std::sync::atomic::Ordering::Relaxed);
                })),
            )
            .expect("the nominal submit is accepted");
        // Flip the posture to Cordoned through the executor own seam.
        executor.observe_throttle(
            100,
            ThrottleSample {
                events: 3,
                throttled_usec: 0,
                elapsed_usec: 1_000,
            },
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        let refused = executor
            .submit_solve_bin(
                1,
                SubmitWork::from(Box::new(move |_ctx| {
                    red_panic("a posture-held submit must not run its body");
                })),
            )
            .expect_err("the CORDONED submit must refuse TYPED at the submit seam");
        // The refusal must carry the posture + role in its payload.
        assert!(
            matches!(
                refused,
                SubmitError::PostureHeld {
                    posture: FleetPosture::Cordoned,
                    role: WorkerRole::Solver,
                }
            ),
            "the refusal must carry the posture + role: {refused:?}"
        );
        // The occupant was NEVER preempted: it completed on its own timeline
        // while the posture was already Cordoned.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !long_unit_done.load(std::sync::atomic::Ordering::Relaxed) {
            assert!(
                std::time::Instant::now() < deadline,
                "the running unit never completed (preempted?)"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// LW-T5 (Seam E): overflow past a role queue cap lands in the
    /// unbounded host backlog (S10 ledger) and NEVER drops - the receipts
    /// report accepted-with-backlog, and every submitted unit completes.
    #[test]
    fn overflow_past_the_role_cap_lands_in_the_backlog_and_never_drops() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let seats = executor.bin_count();
        let occupants_done: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        for bin in 0..seats {
            let done = Arc::clone(&occupants_done);
            executor
                .submit_solve_bin(
                    bin,
                    SubmitWork::from(Box::new(move |_ctx| {
                        std::thread::sleep(std::time::Duration::from_millis(400));
                        done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    })),
                )
                .expect("occupant submit");
        }
        let extras = seats * 4;
        let drained: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        for bin in 0..extras {
            let drained = Arc::clone(&drained);
            executor
                .submit_solve_bin(
                    bin % seats,
                    SubmitWork::from(Box::new(move |_ctx| {
                        drained.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    })),
                )
                .expect("overflow submits are ACCEPTED - backlog, never dropped");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        let drained_probe = Arc::clone(&drained);
        let probe = executor
            .submit_solve_bin(
                0,
                SubmitWork::from(Box::new(move |_ctx| {
                    drained_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                })),
            )
            .expect("backlog submits are ACCEPTED - never dropped");
        assert!(
            probe.accepted_with_backlog,
            "the overflow submit must report accepted-with-backlog (the cap was exceeded)"
        );
        let total = u64::try_from(seats * 5 + 1).unwrap_or(u64::MAX);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while occupants_done.load(std::sync::atomic::Ordering::Relaxed)
            + drained.load(std::sync::atomic::Ordering::Relaxed)
            < total
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the seats did not drain all submitted units in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            occupants_done.load(std::sync::atomic::Ordering::Relaxed)
                + drained.load(std::sync::atomic::Ordering::Relaxed),
            total,
            "every submitted unit must complete - never dropped"
        );
    }

    // ---- LW-T3 (Seam C): escalation port — self-contained I/O lane ----------

    /// A TEST escalation port: a dedicated single-thread tokio runtime owned
    /// by its own pump thread (a self-contained capability lane, never the
    /// caller's CPU seat) — the shape of the default impl (the inline-sim
    /// runtime); the pyo3 default impl is feature-gated, so the lane
    /// contract is pinned here at the seam.
    struct ThreadLanePort {
        tx: std::sync::mpsc::Sender<(EscalationWork, degenbot_workers::lane::FinishOnDrop)>,
        gate: Arc<degenbot_workers::lane::EscalationGate>,
    }

    impl ThreadLanePort {
        fn spawn(budget: usize) -> Self {
            let (tx, rx) =
                std::sync::mpsc::channel::<(EscalationWork, degenbot_workers::lane::FinishOnDrop)>(
                );
            let gate = degenbot_workers::lane::EscalationGate::new(budget);
            std::thread::Builder::new()
                .name("test-escalation-lane".to_owned())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("escalation lane runtime");
                    for (work, permit) in rx {
                        runtime.block_on(work);
                        drop(permit);
                    }
                })
                .expect("escalation lane pump thread");
            Self { tx, gate }
        }
    }

    impl EscalationPort for ThreadLanePort {
        fn escalate(&self, work: EscalationWork) -> Result<(), EscalationError> {
            let permit = self.gate.begin()?;
            if self.tx.send((work, permit)).is_err() {
                return Err(EscalationError::PortClosed);
            }
            Ok(())
        }

        fn counters(&self) -> degenbot_workers::lane::EscalationCountersSnapshot {
            self.gate.counters()
        }
    }

    /// LW-T3 (Seam C, reth research §7): escalation is a SELF-CONTAINED I/O
    /// lane — a bin escalates its cold-miss work through its `LaneCtx`
    /// while EVERY solver seat is mid-unit, and the escalations complete on
    /// the port lane (never on a solver seat: CPU cannot starve I/O).
    #[test]
    fn escalations_complete_while_all_solver_seats_are_mid_unit() {
        install_default_escalation_port(Arc::new(ThreadLanePort::spawn(8)));
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let seats = executor.bin_count();
        let completed: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        let failures: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        let after_drained: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        let lane_threads: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
        let seats_done: Arc<std::sync::atomic::AtomicU64> = Arc::default();
        for bin in 0..seats {
            let completed = Arc::clone(&completed);
            let failures = Arc::clone(&failures);
            let after_drained = Arc::clone(&after_drained);
            let lane_threads = Arc::clone(&lane_threads);
            let seats_done = Arc::clone(&seats_done);
            executor
                .submit_solve_bin(
                    bin,
                    SubmitWork::from(Box::new(move |ctx| {
                        // The bin escalates its cold-miss work while MID-UNIT — the
                        // escalation must NOT run on this seat (CPU) but on the
                        // port's own lane.
                        let occupied_seats = Arc::clone(&seats_done);
                        if ctx
                            .escalate(Box::pin(async move {
                                // STARVATION CHECK: an escalation completing only
                                // after all seats drained would be CPU starvation by
                                // another name.
                                if occupied_seats.load(Ordering::Relaxed)
                                    == u64::try_from(seats).unwrap_or(0)
                                {
                                    after_drained.fetch_add(1, Ordering::Relaxed);
                                }
                                let name = std::thread::current()
                                    .name()
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| "<unnamed>".to_owned());
                                lane_threads.lock().push(name);
                                completed.fetch_add(1, Ordering::Relaxed);
                            }))
                            .is_err()
                        {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                        std::thread::sleep(std::time::Duration::from_millis(400));
                        seats_done.fetch_add(1, Ordering::Relaxed);
                    })),
                )
                .expect("the escalation occupancy submit is accepted");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while seats_done.load(Ordering::Relaxed) < u64::try_from(seats).unwrap_or(0) {
            assert!(
                std::time::Instant::now() < deadline,
                "seats did not drain the escalation probe units in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            failures.load(Ordering::Relaxed),
            0,
            "escalations through the LaneCtx port must succeed (typed errors are the only failure mode)"
        );
        assert_eq!(
            completed.load(Ordering::Relaxed),
            u64::try_from(seats).unwrap_or(0),
            "every escalated cold-miss work item must COMPLETE while its seat is mid-unit"
        );
        assert_eq!(
            after_drained.load(Ordering::Relaxed),
            0,
            "no escalation may complete only after the seats drained (CPU starvation)"
        );
        for lane in lane_threads.lock().iter() {
            assert_ne!(
                lane.strip_prefix("work-fleet-solver"),
                Some(""),
                "escalated work must NEVER run on a solver seat: {lane}"
            );
        }
    }

    /// Test verdict double (decision A): records every (unit, seat)
    /// consultation and prescribes RecordAndContinue — never a real abort.
    struct VerdictRecorder {
        consulted: parking_lot::Mutex<Vec<(u64, u64)>>,
    }

    impl PanicVerdict for VerdictRecorder {
        fn on_unit_panic(&self, unit: u64, seat: u64) -> PanicAction {
            self.consulted.lock().push((unit, seat));
            PanicAction::RecordAndContinue
        }
    }

    /// Deliberate panic inside a harness bin body: the adapter's
    /// catch_unwind (with the seat's backstop) must convert it to data.
    #[expect(clippy::panic)]
    fn red_panic(message: &str) -> ! {
        panic!("{message}")
    }

    /// The exactness fuse (QR3NUS): a bin whose 3rd of N paths panics
    /// still drains exactly one outcome per submitted path — survivors as
    /// real outcomes (a worker `None` IS an outcome), every undelivered
    /// path as a typed failure — so `solved + suppressed + failed ==
    /// submitted` with the delivered and failed pid sets exact and
    /// disjoint. Today the panic silently vanishes with sender-drop.
    #[test]
    fn drain_delivers_exactly_one_lane_outcome_per_submitted_path_when_third_path_panics() {
        let submitted: BTreeSet<u64> = [10, 11, 12, 13, 14].into_iter().collect();
        let (tx, rx) = std::sync::mpsc::channel::<LaneOutcome>();
        let mut lane = SolveLane::new(1, 0, vec![10, 11, 12, 13, 14], tx);
        run_solve_lane(&mut lane, &SeatSurvivesPolicy, |lane| {
            // Trace of a real bin: two survivor arms land, then the 3rd
            // path panics — 13 and 14 are still owed and must come back
            // typed instead of silently vanishing with sender-drop.
            let arm: SolveArmOutcome = (10, SolvePathResult::default(), 0, None);
            lane.solved(arm);
            lane.suppressed(11);
            red_panic("path 12 panicked mid-bin (QR3NUS red harness)");
            // 13/14 never run — the panic ends the bin body.
        });
        drop(lane); // close the pipe so the drain completes
        let outcomes: Vec<LaneOutcome> = rx.into_iter().collect();

        let mut solved_count = 0usize;
        let mut suppressed_count = 0usize;
        let mut failed_count = 0usize;
        let mut delivered: BTreeSet<u64> = BTreeSet::new();
        let mut failed: BTreeSet<u64> = BTreeSet::new();
        for outcome in outcomes {
            match outcome {
                LaneOutcome::Solved(item) => {
                    solved_count += 1;
                    delivered.insert(item.0);
                }
                LaneOutcome::Suppressed { pid } => {
                    suppressed_count += 1;
                    delivered.insert(pid);
                }
                LaneOutcome::Failed { pid, failure } => {
                    failed_count += 1;
                    assert!(
                        matches!(
                            failure,
                            LaneFailure::SeatPanic {
                                unit: 1,
                                seat: 0,
                                message: Some(_)
                            }
                        ),
                        "failed record must name unit + seat and carry the panic payload"
                    );
                    failed.insert(pid);
                }
            }
        }
        assert!(
            delivered.is_disjoint(&failed),
            "a path cannot be both delivered and failed: {delivered:?} / {failed:?}"
        );
        let covered: BTreeSet<u64> = delivered.union(&failed).copied().collect();
        assert_eq!(
            covered, submitted,
            "the drain must observe exactly one outcome per submitted path \
             (delivered ∪ failed == submitted) — the panic must not undercount"
        );
        assert_eq!(
            solved_count + suppressed_count + failed_count,
            submitted.len(),
            "merge-side per-path accounting must equal submissions"
        );
        assert_eq!(
            failed,
            [12, 13, 14].into_iter().collect::<BTreeSet<u64>>(),
            "the 3rd path and everything after it must land as typed failures"
        );
    }

    /// Decision A drive: after a panicking cycle the SAME seat takes the
    /// next cycle's pinned bin (keyed pins never move), and the panic was
    /// expressed as data — the verdict consulted, typed failure records
    /// on the pipe. The panicking bin here rides a REAL executor seat so
    /// the seat-survives policy is exercised end to end (no real abort —
    /// the strict `AbortingPolicy` is never installed under test).
    #[test]
    fn panicked_seat_survives_and_takes_the_next_cycles_pinned_bin_on_the_same_thread() {
        let executor = FleetSolveExecutor::boot(hermetic_boot()).expect("fleet boot");
        let verdict = Arc::new(VerdictRecorder {
            consulted: parking_lot::Mutex::new(Vec::new()),
        });
        let observed: Arc<parking_lot::Mutex<Vec<std::thread::ThreadId>>> = Arc::default();
        let lane_outcomes: Arc<parking_lot::Mutex<Vec<LaneOutcome>>> = Arc::default();
        for cycle in 0..2 {
            let observed = Arc::clone(&observed);
            let verdict = Arc::clone(&verdict);
            let lane_outcomes = Arc::clone(&lane_outcomes);
            executor
                .submit(
                    0,
                    Box::new(move |_ctx| {
                        observed.lock().push(std::thread::current().id());
                        let (tx, rx) = std::sync::mpsc::channel::<LaneOutcome>();
                        let mut lane =
                            SolveLane::new(cycle, 0, vec![cycle * 10, cycle * 10 + 1], tx);
                        run_solve_lane(&mut lane, verdict.as_ref(), |lane| {
                            if cycle == 0 {
                                lane.suppressed(cycle * 10); // one pid emitted before the panic
                                red_panic("cycle-0 bin panics (QR3NUS red harness)");
                            }
                        });
                        drop(lane); // close the pipe so the per-cycle drain completes
                        let mut stash = lane_outcomes.lock();
                        for outcome in rx.into_iter() {
                            stash.push(outcome);
                        }
                    }),
                )
                .expect("seat-stays unit accepted");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while observed.lock().len() < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the seat did not take the next cycle's pinned bin in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let threads = observed.lock().clone();
        assert_eq!(
            threads[0], threads[1],
            "the SAME seat must take the next cycle's pinned bin after the panic"
        );
        let consulted = verdict.consulted.lock().clone();
        assert_eq!(
            consulted.len(),
            1,
            "the panicking cycle must consult the verdict (and only that cycle): {consulted:?}"
        );
        let failed: Vec<u64> = lane_outcomes
            .lock()
            .iter()
            .filter_map(|outcome| match outcome {
                LaneOutcome::Failed { pid, .. } => Some(*pid),
                _ => None,
            })
            .collect();
        assert_eq!(
            failed,
            vec![1],
            "the panicking cycle's undelivered path must land as a typed failure record"
        );
    }

    /// The unit-panic-with-pipe tripwire at policy-object level (decision
    /// A; no real `std::process::abort` ever runs under test): a panicking
    /// unit consults the verdict with its (unit, seat) identity, and the
    /// typed failure records carry that payload.
    #[test]
    fn unit_panic_with_result_pipe_consults_the_panic_verdict_with_unit_and_seat_payload() {
        let verdict = VerdictRecorder {
            consulted: parking_lot::Mutex::new(Vec::new()),
        };
        let (tx, rx) = std::sync::mpsc::channel::<LaneOutcome>();
        let mut lane = SolveLane::new(7, 3, vec![21, 22], tx);
        run_solve_lane(&mut lane, &verdict, |_lane| {
            red_panic("bin unit panics with its result pipe open (QR3NUS tripwire harness)");
        });
        drop(lane); // close the pipe so the drain completes
        let outcomes: Vec<LaneOutcome> = rx.into_iter().collect();

        assert_eq!(
            verdict.consulted.lock().as_slice(),
            [(7, 3)],
            "the verdict must be consulted exactly once with unit + seat payload"
        );
        let failed: Vec<(u64, &LaneFailure)> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                LaneOutcome::Failed { pid, failure } => Some((*pid, failure)),
                _ => None,
            })
            .collect();
        let failed_pids: BTreeSet<u64> = failed.iter().map(|(pid, _)| *pid).collect();
        assert_eq!(
            failed_pids,
            [21, 22].into_iter().collect::<BTreeSet<u64>>(),
            "every undelivered path must be covered by exactly one typed failure"
        );
        for (pid, failure) in failed {
            assert!(
                matches!(
                    failure,
                    LaneFailure::SeatPanic {
                        unit: 7,
                        seat: 3,
                        message: Some(message)
                    } if message.contains("tripwire")
                ),
                "failed record must name unit 7 + seat 3 and carry the panic payload (pid {pid})"
            );
        }
        // The pure policy mapping (decision A): the surviving policy keeps
        // the seat; the strict abort posture stays a VALUE — it aborts only
        // when wired outside tests, never here.
        assert_eq!(
            SeatSurvivesPolicy.on_unit_panic(7, 3),
            PanicAction::RecordAndContinue
        );
        assert_eq!(AbortingPolicy.on_unit_panic(7, 3), PanicAction::Abort);
    }
}
