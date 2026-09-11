//! `seat_host` — the ONE pooled-seat host for the `WorkQueue` fleet roles
//! (RZEWTX): the byte-identical `WorkQueue` pair, the `seat_loop` pair,
//! the `host_loop`/`apply_host_msg` pair, the `pump` grant loop, and the
//! construction-stamped boot install/global boilerplate fold here ONCE;
//! `fleet_sim_executor` and `fleet_registration_executor` become thin role
//! descriptors ([`SeatRoleDesc`]) over this machinery.
//!
//! # The admission model (design gate — decided BEFORE any code moved)
//!
//! All three fleet roles reconcile under ONE invariant — the never-drop
//! ledger (design doc §10): no admission path drops or refuses a unit. A
//! full per-role queue and a cordon hold BOTH convert to a wait (spill
//! into the unbounded host backlog, drain FIFO); the only failure shape is
//! a closed host channel, and that is a loud abort (a lost unit strands
//! its receipt pipe). What differs per role is the cordon effect at
//! admission and the seat model:
//!
//! - registration (`PoolStateUpdater`, Deferrable cordon class): the
//!   never-drop flood with unbounded backlog; a `Cordoned` posture HOLDS
//!   new intake — held units wait in the backlog, never dropped
//!   (`fleet_intake.rs`; test
//!   `fleet_intake_facade_preserves_the_never_drop_flood`).
//! - sim (`SimDriver`, `SimPool` cordon class): seat-pool pacing — the seat
//!   pool IS the grant lane, capping concurrent executing sims at the
//!   budget's sim slot cap; a `Cordoned` posture still ADMITS sim leases
//!   (floored, not held — the floor is the host FSM's sim-intake lane,
//!   dispatcher-side; tests `sim_seat_pool_is_the_budget_sim_slot_cap`,
//!   `concurrent_sims_are_bounded_by_the_sim_slot_cap`).
//! - solve (`Solver`, `CordonClass::Never`): posture-INVARIANT admission
//!   at the TYPED submit seam — a Cordoned posture must NEVER refuse a
//!   Solver bin (`fleet_solve_executor.rs`; test
//!   `submit_in_cordoned_posture_still_admits_solver_units_and_running_
//!   units_complete`).
//!
//! DECISION (RZEWTX): the host serves the pairwise-compatible `WorkQueue`
//! pair (sim + registration) ONLY — the solve executor does NOT join.
//! Solve is excluded on structure, not convenience: its seat model is
//! per-seat mpsc mailboxes keyed by the Solver pin (T3/T6 warm arenas,
//! `seat_loop(seat, rx, done)`), its host channel carries a
//! `HostMsg::Throttle` posture-feed arm, and its admission lives at the
//! typed submit seam (`Result<SubmitReceipt, SubmitError>`) — a different
//! seam from this host's fire-and-forget `FleetIntake` port. Folding it in
//! would demand a sum-type seat model, a sum-type host message, and a
//! sum-type admission policy to unify two shapes that share no code path
//! (the card's "do not force-misfit the third"). Correspondingly the
//! admission policy here is the honest TWO-arm [`CordonAdmission`] — a
//! descriptor field spanning all three roles would be a sum type in data
//! clothing (one arm dead in every host). Solve follows the same never-drop
//! PRINCIPLE (unbounded backlog, §10) with its own mechanism (the queue-len
//! mirror + typed submit receipt), owned by `fleet_solve_executor.rs`.
//!
//! # Invariants preserved (behavior byte-stable)
//!
//! - Boot/census/thread names: seat threads take `WorkerRole::thread_name()`
//!   with `{n}` substituted (`work-fleet-sim-{n}` /
//!   `work-fleet-poolupd-{n}`), host threads take the descriptor's
//!   `host_thread`; the census rows self-register inside `FleetHost::boot`
//!   — the `BootRole` labels byte-stay (`fleet_simdriver_slots` /
//!   `fleet_pool_state_updater_slots`).
//! - `boot_stamp.rs` logic untouched: every install routes the ride ledger
//!   (`record_ride`) with the descriptor's [`BootRole`], exactly as before.
//! - The `fleet_intake.rs` frozen facade untouched: the port + 2 pub fns
//!   surface is unchanged, and `InnerWork = Box<dyn FnOnce>` carries NO
//!   pyo3 in any host signature (`Python::attach` stays at the pyo3 leaf).
//! - Loud aborts: every seat/host failure funnels through [`abort_executor`],
//!   whose tag (`[fleet-sim]` / `[fleet-reg]`) and stranded-pipe noun stay
//!   byte-identical per role.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, OnceLock};

use crate::arb_engine::boot_stamp::{BootRole, BootStamp};
use degenbot_workers::budget::FleetBudget;
use degenbot_workers::dispatcher::{BootError, FleetBoot, FleetHost, GrantKind, Unit};
use degenbot_workers::lane::LaneCtx;
use degenbot_workers::posture::FleetPosture;
use degenbot_workers::role::WorkerRole;

use crate::arb_engine::fleet_intake::InnerWork;

/// The cordon effect at admission — the design-gate policy arm (RZEWTX).
/// TWO arms, both live: this host serves the `WorkQueue` pair only (sim +
/// registration; solve's posture-invariant typed-submit admission is out —
/// see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CordonAdmission {
    /// `SimPool` class (sim): a `Cordoned` posture still admits — sim leases
    /// are floored, not held; the floor is the host FSM's sim-intake lane,
    /// dispatcher-side (the `PostureHeld` enqueue error cannot fire here).
    Admit,
    /// Deferrable class (registration): a `Cordoned` posture holds new
    /// intake — the unit waits in the unbounded backlog and re-queues when
    /// the fleet exits the cordon (in-flight units are never cancelled).
    Hold,
}

/// The role descriptor: everything that differs between the two pooled
/// `WorkQueue` executors, and nothing else. The executors are THIN over
/// this — the machinery (queue, seat loop, host loop, admission, boot
/// boilerplate) lives once, here.
pub(crate) struct SeatRoleDesc {
    /// The pooled [`WorkerRole`] — drives the seat thread names
    /// (`thread_name()`), the per-role queue len/cap keys, and the
    /// [`Unit`] role stamp.
    pub role: WorkerRole,
    /// The one dispatch [`GrantKind`] this executor produces — the
    /// grant-shape invariant check in [`pump`] (anything else is a broken
    /// host contract, not a drop).
    pub grant: GrantKind,
    /// The admission policy under a Cordoned posture (the design-gate arm).
    pub cordon: CordonAdmission,
    /// The [`BootRole`] ledger row this executor's boot rides
    /// (`boot_stamp::record_ride`).
    pub boot_role: BootRole,
    /// Loud-abort log tag (`"[fleet-sim]"` / `"[fleet-reg]"`) — the
    /// process's last message keeps the per-role tag byte-identical.
    pub abort_tag: &'static str,
    /// Work-noun for the abort contexts and the seat panic log
    /// (`"sim"` / `"intake"`): `"{noun} seat spawn"`,
    /// `"{noun} enqueue"`, `"{noun} unit panicked"`,
    /// `"stranded {noun} receipt pipe"`, ...
    pub noun: &'static str,
    /// The host thread's census name
    /// (`"work-fleet-sim-host"` / `"work-fleet-poolupd-host"`).
    pub host_thread: &'static str,
    /// The YI5NGB stamp-missing panic message (the F1 loud construction
    /// contract — the materializer must abort, never fall back silently).
    pub stamp_missing: &'static str,
    /// The queue-cap source: the budget field this role's seat pool sizes
    /// from (`sim_slot_cap` / `pool_state_updater_slots`).
    pub seats: fn(&FleetBudget) -> usize,
}

/// Host-bound message: a submitted unit, or a seat reporting its unit done
/// (completion drives T5 — the pooled slot returns to idle).
enum HostMsg {
    Enqueue(Unit),
    SeatDone { seat: u64 },
}

/// One granted unit handed to whichever pooled seat takes it next (both
/// roles are pooled — seats contend, no pin affinity).
struct SeatJob {
    /// The host-tracked slot the unit was granted to (completion carries it
    /// back so T5 applies to the right slot).
    slot: u64,
    /// The work payload (`Send + 'static`). Takes the seat's `LaneCtx`
    /// (LW-T2); pooled seats hand the detached stub (LW-T8 landed: the
    /// executors submit through ONE seam). NO pyo3 type crosses this seam —
    /// `Python::attach` stays at the pyo3 leaf.
    work: Box<dyn FnOnce(&LaneCtx) + Send>,
}

/// The shared pooled-seat work queue (std `mpsc` receivers are not
/// `Clone`, so the contended seat pool rides a condvar deque). Folded once
/// from the byte-identical pair (RZEWTX).
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

/// The executor-facing half of the shared seat host: the host channel's
/// submit end + the unit sequence, stamped with the role descriptor.
pub(crate) struct SeatHost {
    desc: &'static SeatRoleDesc,
    tx: mpsc::Sender<HostMsg>,
    unit_seq: AtomicU64,
    /// Test-facing seat count (the role's budget slot cap).
    #[cfg(test)]
    seats: usize,
}

impl SeatHost {
    /// Boot the shared pooled-seat host for `desc`: boot the [`FleetHost`],
    /// spawn the pooled seat threads (the budget's per-role slot cap — the
    /// descriptor's queue-cap source — with the role's census naming), and
    /// run the dispatch loop on the host thread. Fail-loud (the typed
    /// [`BootError`]) when the declared shares cannot host the quota.
    ///
    /// # Errors
    /// [`BootError`] — the fleet budget sum check or a boot invariant.
    pub(crate) fn boot(desc: &'static SeatRoleDesc, boot: FleetBoot) -> Result<Self, BootError> {
        let host = FleetHost::boot(boot)?;
        let seats = (desc.seats)(host.budget());
        let (tx, rx) = mpsc::channel::<HostMsg>();
        // Pooled seats contend on ONE shared work queue: a grant lands a
        // unit there, any idle seat takes it, and the completion reports
        // the GRANTED slot id so the host applies T5 to the right slot.
        // Grants never exceed the role's slot cap, which never exceeds the
        // seat count, so every granted unit is picked up without delay.
        let work = Arc::new(WorkQueue::new());
        for seat in 0..seats {
            let done = tx.clone();
            let work = Arc::clone(&work);
            let spawned = std::thread::Builder::new()
                .name(desc.role.thread_name().replace("{n}", &seat.to_string()))
                .spawn(move || seat_loop(desc, &work, &done));
            if let Err(err) = spawned {
                // A missing seat strands the receipts of every unit that
                // would have run on it — loud (§10).
                abort_executor(
                    desc,
                    &format!("{} seat spawn", desc.noun),
                    &format!("{err:?}"),
                );
            }
        }
        let spawned = std::thread::Builder::new()
            .name(desc.host_thread.to_string())
            .spawn(move || {
                host_loop(rx, host, desc, Arc::clone(&work));
                // Process teardown: the submission channel closed. Retire
                // the seats so no worker parks forever on an empty queue.
                work.close();
            });
        if let Err(err) = spawned {
            abort_executor(
                desc,
                &format!("fleet {} host thread spawn", desc.noun),
                &format!("{err:?}"),
            );
        }
        Ok(Self {
            desc,
            tx,
            unit_seq: AtomicU64::new(0),
            #[cfg(test)]
            seats,
        })
    }

    /// Test-facing seat count (the role's budget slot cap).
    #[cfg(test)]
    pub(crate) fn seat_count(&self) -> usize {
        self.seats
    }

    /// The port's unit body (folded from the two executors' pre-existing
    /// submit bodies): wraps into `Unit::new(.., Box::new(move |_ctx|
    /// work()))` and enqueues over `tx.send(HostMsg::Enqueue(unit))`, typed
    /// to the port's `Result<(), ()>` close vocabulary — the send VALUE
    /// carries the close arm; the abort lives in [`intake_spawn`] (same
    /// process-exit semantics, one owner of the abort).
    pub(crate) fn try_send(&self, work: InnerWork) -> Result<(), ()> {
        let unit = Unit::new(
            self.unit_seq.fetch_add(1, Ordering::Relaxed),
            self.desc.role,
            None,
            // The unit's receipt feeds the awaiting caller's join — a
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

/// The [`crate::arb_engine::fleet_intake::FleetIntake`] port's close arm,
/// shared by both executors' trait impls: a closed host channel (the host
/// thread died) is a LOUD abort — a lost unit strands its receipt pipe
/// (§10). One owner of the per-role close strings; the impls keep the
/// original `if self.try_send(work).is_err()` shape.
pub(crate) fn intake_close_abort(host: &SeatHost) -> ! {
    abort_executor(
        host.desc,
        &format!("{} submission", host.desc.noun),
        &format!("fleet {} host channel closed", host.desc.noun),
    );
}

/// One pooled seat: take granted units from the shared work queue, run
/// them one at a time, and report the granted slot's completion so the
/// host applies T5 (run → idle).
fn seat_loop(desc: &'static SeatRoleDesc, work: &WorkQueue, done: &mpsc::Sender<HostMsg>) {
    while let Some(job) = work.take() {
        // A panicking unit closure must not kill the seat (its pool would
        // strand receipts): keep the seat alive, log loudly, report done.
        let ctx = LaneCtx::detached();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| (job.work)(&ctx)));
        if outcome.is_err() {
            tracing::error!(
                target: "degenbot::fleet",
                seat = job.slot,
                "{} {} unit panicked — the seat survives, the failure is loud",
                desc.abort_tag,
                desc.noun
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
fn host_loop(
    rx: mpsc::Receiver<HostMsg>,
    mut host: FleetHost,
    desc: &'static SeatRoleDesc,
    queue: Arc<WorkQueue>,
) {
    let mut backlog: VecDeque<Unit> = VecDeque::new();
    while let Ok(msg) = rx.recv() {
        apply_host_msg(desc, &mut host, &mut backlog, msg);
        pump(desc, &mut host, &mut backlog, &queue);
    }
}

/// Apply one submission or completion (both arrive on the single host
/// channel — completions can never starve behind a blocking recv).
fn apply_host_msg(
    desc: &'static SeatRoleDesc,
    host: &mut FleetHost,
    backlog: &mut VecDeque<Unit>,
    msg: HostMsg,
) {
    match msg {
        HostMsg::Enqueue(unit) => {
            // Pre-check capacity INSTEAD of failing enqueue: the host
            // thread owns every queue mutation, so the check is exact.
            // Units that do not fit spill to the backlog (unbounded, like
            // the legacy pipelines) and drain FIRST on the next pump —
            // never dropped (§10 ledger).
            if host.queue_len(desc.role) >= host.queue_cap(desc.role) {
                backlog.push_back(unit);
            } else if desc.cordon == CordonAdmission::Hold
                && host.posture() == FleetPosture::Cordoned
            {
                // The `Hold` admission arm (Deferrable class —
                // registration): a cordon HOLDS intake — the unit waits in
                // the backlog and re-queues when the fleet exits the
                // cordon (the legacy crawl threads were never cancelled
                // either; this is the deferrable stance, §6). The
                // `Admit` arm (SimPool class — sim) skips this check: a
                // cordon still ADMITS its leases (floored, not held). The
                // posture machine is host-owned (single host thread), so
                // the check cannot race a cordon onset.
                backlog.push_back(unit);
            } else if let Err(err) = host.enqueue(unit) {
                // v1-active, non-merge pooled units cannot hit
                // RoleNotActive / MergeNeverQueued; with the `Admit` arm
                // PostureHeld cannot fire either. Any such error is a
                // broken invariant, not a drop.
                abort_executor(desc, &format!("{} enqueue", desc.noun), &err.to_string());
            }
        }
        HostMsg::SeatDone { seat } => {
            if let Err(err) = host.complete(seat) {
                abort_executor(desc, "seat completion (T5)", &err.to_string());
            }
        }
    }
}

/// The one precedence grant loop pass (design doc §4): backlog first, then
/// dispatch grants onto the pooled seats. Grants apply T2 (start) at grant
/// time — the work-queue push IS the claim — and completion arrives via
/// [`HostMsg::SeatDone`] (T5).
fn pump(
    desc: &'static SeatRoleDesc,
    host: &mut FleetHost,
    backlog: &mut VecDeque<Unit>,
    queue: &Arc<WorkQueue>,
) {
    // Backlog drains FIRST (FIFO across the loud-overflow seam). A backed
    // backlog that cannot enqueue (the `Hold` arm's cordon hold, or a
    // full per-role queue) parks here until the next host message — the
    // awaiting callers already submitted, and retrying on every wake
    // matches the legacy worker semantics (work waits, never drops).
    while backlog.front().is_some() {
        // Every unit in this host's backlog carries the descriptor's role
        // by construction (try_send stamps it), so the role check is the
        // descriptor's role.
        if host.queue_len(desc.role) >= host.queue_cap(desc.role) {
            break;
        }
        // The `Hold` arm: still cordoned — the backlog head stays;
        // retrying on the next host message (submissions keep arriving;
        // no busy-spin — pump only runs on a message). The posture
        // machine is host-owned (single host thread), so the check cannot
        // race a cordon onset.
        if desc.cordon == CordonAdmission::Hold && host.posture() == FleetPosture::Cordoned {
            break;
        }
        let Some(unit) = backlog.pop_front() else {
            break;
        };
        if let Err(err) = host.enqueue(unit) {
            abort_executor(desc, "backlog drain", &err.to_string());
        }
    }
    loop {
        let grants = host.dispatch();
        if grants.is_empty() {
            break;
        }
        for (grant, unit) in grants {
            // Invariant: this executor only enqueues its descriptor's
            // role's units, so every grant is that role's pooled grant.
            // Anything else is a broken host contract, not a drop.
            if grant.kind != desc.grant {
                abort_executor(
                    desc,
                    "dispatch grant",
                    &format!("non-{} grant in the {} executor", desc.noun, desc.noun),
                );
            }
            if let Err(err) = host.start(grant.slot, &unit) {
                abort_executor(desc, "grant start (T2)", &err.to_string());
            }
            queue.push(SeatJob {
                slot: grant.slot,
                work: unit.work,
            });
        }
    }
}

/// Loud, unrecoverable executor failure (mirror of the fleet solve
/// executor's abort discipline): a dead host would strand in-flight
/// receipts — an awaiting caller parks forever (stranded pipe, design doc
/// §10) — so swallowing the error is never an option. The per-role tag and
/// stranded-pipe noun come from the descriptor, byte-identical to the
/// pre-fold messages.
#[expect(
    clippy::print_stderr,
    reason = "the abort path must stay legible with no tracing subscriber installed (test harnesses drop the tracing event); stderr is the process's last message"
)]
fn abort_executor(desc: &SeatRoleDesc, context: &str, err: &str) -> ! {
    tracing::error!(
        context = %context,
        error = %err,
        "{} unrecoverable — aborting (stranded {} receipt pipe)",
        desc.abort_tag,
        desc.noun
    );
    eprintln!(
        "{} UNRECOVERABLE, aborting (stranded {} receipt pipe): {context}: {err}",
        desc.abort_tag, desc.noun
    );
    std::process::abort();
}

/// Install the CONSTRUCTION-STAMPED boot (YI5NGB): the engine's own typed
/// boot descriptor (fleet quota + overrides + posture) parsed at ITS
/// construction from the CALLER cfg, stamped with the engine id + a
/// deterministic cfg hash. Never overrides an installed value (first
/// engine wins, like the other stance statics) — every construction after
/// the first RIDES, and the ride is ledgered (a divergent-cfg rider is
/// counted + warned in prod, ILLEGAL in tests) on the descriptor's
/// [`BootRole`] row.
pub(crate) fn install_boot(courier: &OnceLock<BootStamp>, desc: &SeatRoleDesc, stamp: BootStamp) {
    crate::arb_engine::boot_stamp::record_ride(desc.boot_role, &stamp);
    let _ = courier.set(stamp);
}

/// The process-wide materializer (the global_* boilerplate, folded from
/// the two executors): lazily boot the executor from the
/// construction-stamped boot and persist it for the process lifetime.
/// YI5NGB: the absence window is CLOSED BY CONSTRUCTION — every dispatch
/// path builds on a constructed engine, and construction (`with_core_cfg`)
/// installs the stamp BEFORE any dispatch can exist. A missing stamp means
/// a caller skipped the construction contract: LOUD abort (never a silent
/// fallback boot of a boot nobody chose).
pub(crate) fn global_executor<T>(
    desc: &'static SeatRoleDesc,
    courier: &'static OnceLock<BootStamp>,
    slot: &'static OnceLock<T>,
    boot: fn(FleetBoot) -> Result<T, BootError>,
) -> &'static T {
    slot.get_or_init(|| {
        #[expect(
            clippy::expect_used,
            reason = "the loud construction-contract abort IS the YI5NGB design: a stamp-less materialization must abort, never fall back silently"
        )]
        let stamp = courier.get().expect(desc.stamp_missing);
        match boot(stamp.boot()) {
            Ok(executor) => executor,
            Err(err) => abort_executor(
                desc,
                &format!("fleet {} budget boot", desc.noun),
                &err.to_string(),
            ),
        }
    })
}
