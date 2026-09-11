//! `seat_host` — the ONE host machinery for the fleet hosts: the
//! byte-identical `WorkQueue` pair, the `seat_loop` pair, and — since
//! 6HE6RF — the ONE [`HostPump`] host-message triple (`apply_host_msg` +
//! the backlog-draining grant pump + the recv loop) that ALL THREE fleet
//! hosts run (the pooled pair AND the solve host), plus the
//! construction-stamped boot install/global boilerplate.
//! `fleet_sim_executor`, `fleet_registration_executor`, and
//! `fleet_solve_executor` parameterize the triple; the seat models stay
//! per host kind (P-RZEWTX): the pooled `WorkQueue` here, the solve
//! host's per-seat keyed mailboxes there.
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
//!   `fleet_intake_facade_preserves_the_never_drop_flood`, and JCI2FW
//!   Part A's reached-Cordoned test
//!   `a_cordoned_posture_holds_registration_intake_until_the_cordon_
//!   lifts`).
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
//! DECISION (RZEWTX, narrowed by 6HE6RF): the host serves the pairwise-
//! compatible `WorkQueue` pair (sim + registration); the solve executor's
//! SEAT MODEL (per-seat mpsc mailboxes keyed by the Solver pin — T3/T6
//! warm arenas, `seat_loop(seat, rx, done)`) and its typed submit seam
//! (`Result<SubmitReceipt, SubmitError>`) stay in
//! `fleet_solve_executor.rs` — folding the seat models themselves is the
//! documented misfit. 6HE6RF executes the spike's settled WAITING answer
//! on top: the host-MESSAGE triple (`apply_host_msg` + `pump`, backlog
//! drain included) folds ONCE here as [`HostPump`], and the solve host
//! JOINS it — the cap/consult/hold rules now apply ONCE per host instead
//! of twice per executor. The posture consult is unconditional in the
//! unified shape and behavior-EXACT for solve (spike-proven; the proof
//! restated on [`HostPump`]): `admits_lease` blocks ONLY the
//! `(Cordoned, Deferrable)` pair and `Solver` is `CordonClass::Never`.
//! JCI2FW Part A dissolved the TWO-arm `CordonAdmission` descriptor
//! field entirely: admission consults the ONE shared posture owner
//! directly and derives the policy from the role's own cordon class —
//! `FleetHost::posture_admits_role` is the SAME `admits_lease`
//! predicate the dispatcher's gates consult, so Deferrable (registration)
//! intake holds while cordoned and `SimPool` (sim) leases are
//! floored-never-held, with no per-role enum arm and no hand mirror to
//! drift. (Solve's retired `HostMsg::Throttle` arm went with it — the
//! process posture owner is fed by the block pump; every host consults
//! the same owner.) Solve follows the same never-drop PRINCIPLE (unbounded
//! backlog, §10): its queue-len mirror + typed submit receipt stay at its
//! submit seam — the mirror stamps ride the unified triple through
//! [`HostPump`]'s `mirror` field.

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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, OnceLock};

use crate::arb_engine::boot_stamp::{BootRole, BootStamp};
use degenbot_workers::budget::FleetBudget;
use degenbot_workers::dispatcher::{
    BootError, EnqueueError, FleetBoot, FleetHost, Grant, GrantKind, Unit,
};
use degenbot_workers::lane::LaneCtx;
use degenbot_workers::role::WorkerRole;

use crate::arb_engine::fleet_intake::InnerWork;

/// The role descriptor: everything that differs between the two pooled
/// `WorkQueue` executors, and nothing else. The executors are THIN over
/// this — the machinery (queue, seat loop, host loop, admission, boot
/// boilerplate) lives once, here. (JCI2FW Part A: the old `cordon`
/// descriptor arm — `CordonAdmission::Admit`/`Hold` — is dissolved; the
/// role's own cordon class + the ONE shared posture owner ARE the
/// admission policy, read through `FleetHost::posture_admits_role`.)
pub(crate) struct SeatRoleDesc {
    /// The pooled [`WorkerRole`] — drives the seat thread names
    /// (`thread_name()`), the per-role queue len/cap keys, and the
    /// [`Unit`] role stamp.
    pub role: WorkerRole,
    /// The one dispatch [`GrantKind`] this executor produces — the
    /// grant-shape invariant check in [`pump`] (anything else is a broken
    /// host contract, not a drop).
    pub grant: GrantKind,
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

/// Host-bound message — ONE shape for all three fleet hosts (6HE6RF): a
/// submitted unit, or a seat reporting its unit done. Completion applies
/// the slot's own T-row ([`FleetHost::complete`] keys off the slot FSM
/// state: T5 pooled → idle, T3 Solver → warm re-pin), so the message
/// needs no per-role arm.
pub(crate) enum HostMsg {
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

/// Which dispatch grant kinds a host's [`HostPump`] is contracted to serve
/// (the row-#6 contract check, loud on ALL THREE hosts since 6HE6RF): a
/// grant outside this set is a broken host contract, never a seat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GrantContract {
    /// A pooled host serves exactly ONE grant kind — the descriptor's
    /// (`GrantKind::Sim` / `GrantKind::PoolStateUpdate`).
    Single(GrantKind),
    /// The solve host serves the Solver pair: a new pin claim (T1 keyed
    /// lease) or the SAME pin's continuation (T6) — both route to the
    /// grant slot's keyed seat.
    SolverPins,
}

impl GrantContract {
    /// The pure contract predicate (the test-declared tripwire's seam —
    /// the abort itself is a process abort, untestable in-process).
    #[must_use]
    pub(crate) fn admits(self, kind: GrantKind) -> bool {
        match self {
            Self::Single(expected) => kind == expected,
            Self::SolverPins => {
                matches!(kind, GrantKind::NewPinClaim | GrantKind::PinContinuation)
            }
        }
    }
}

/// P-RZEWTX: the seat-model split of the unified host-message triple. The
/// triple (admission, backlog drain, grant loop) is ONE shape; routing a
/// GRANTED unit to its seat stays per host kind — the pooled `WorkQueue`
/// condvar pair vs the solve host's per-seat keyed mailboxes (warm arenas,
/// typed `LaneCtx`). Folding the seat models themselves is the RZEWTX
/// misfit; this trait is the seam that keeps them apart.
pub(crate) trait SeatSink {
    /// Route one T2-started grant to its seat. `host` is handed back
    /// because the solve sink mints the grant slot's warm arena
    /// (`ensure_arena`) at the same seam the pre-fold loop did.
    fn deliver(&self, host: &mut FleetHost, grant: Grant, unit: Unit);
}

/// The pooled seat model (sim + registration, RZEWTX byte-identical): the
/// granted unit joins the shared `WorkQueue` — any idle pooled seat takes
/// it (seats contend, no pin affinity); completion carries the granted
/// slot back for T5.
pub(crate) struct PooledSink {
    queue: Arc<WorkQueue>,
}

impl SeatSink for PooledSink {
    fn deliver(&self, _host: &mut FleetHost, grant: Grant, unit: Unit) {
        self.queue.push(SeatJob {
            slot: grant.slot,
            work: unit.work,
        });
    }
}

/// The per-host loud-abort + wording seam of the unified triple: every
/// context/message string stays owned by its host module so the pre-fold
/// abort wording stays byte-identical (the pooled pair's strings ride the
/// descriptor; solve's are its own). The unified code calls the SEMANTIC
/// operations only.
pub(crate) trait HostDiscipline {
    /// Unrecoverable host failure at a SHARED context ("backlog drain",
    /// "grant start (T2)") — never returns.
    fn fail(&self, context: &str, err: &str) -> !;
    /// The apply-time enqueue refusal (pooled: `"{noun} enqueue"`;
    /// solve: `"solver enqueue"`).
    fn enqueue_refused(&self, err: &str) -> !;
    /// The `SeatDone` completion refusal (pooled: `"seat completion (T5)"`;
    /// solve: `"seat completion (T3)"`).
    fn completion_refused(&self, err: &str) -> !;
    /// The row-#6 grant-kind contract denial (context `"dispatch grant"`).
    fn foreign_grant(&self, kind: GrantKind) -> !;
}

/// The pooled hosts' discipline: the descriptor owns the tag + noun, so
/// every abort string is the pre-fold byte-identical one.
pub(crate) struct PooledDiscipline {
    desc: &'static SeatRoleDesc,
}

impl HostDiscipline for PooledDiscipline {
    fn fail(&self, context: &str, err: &str) -> ! {
        abort_executor(self.desc, context, err)
    }

    fn enqueue_refused(&self, err: &str) -> ! {
        abort_executor(self.desc, &format!("{} enqueue", self.desc.noun), err)
    }

    fn completion_refused(&self, err: &str) -> ! {
        abort_executor(self.desc, "seat completion (T5)", err)
    }

    fn foreign_grant(&self, _kind: GrantKind) -> ! {
        abort_executor(
            self.desc,
            "dispatch grant",
            &format!(
                "non-{} grant in the {} executor",
                self.desc.noun, self.desc.noun
            ),
        )
    }
}

/// THE one host-message/waiting shape (6HE6RF): `apply_host_msg` + the
/// backlog-draining grant pump + the recv loop, folded ONCE behind all
/// three fleet hosts. The pooled pair (sim + registration) and the solve
/// host parameterize it — everything the JCI2FW-diverged pair actually
/// differed on is a field:
///
/// - [`SeatSink`] — the seat model split (P-RZEWTX): the pooled
///   `WorkQueue` vs the solve host's per-seat keyed mailboxes.
/// - `mirror: Option<&AtomicUsize>` — the typed-submit receipt's
///   advisory stamp (solve only; `None` on the fire-and-forget pooled
///   port). It stores the BOUNDED role-queue length at the last stamp
///   (spill or `SeatDone`) — NOT the backlog depth: the receipt bit means
///   "the role queue was >= cap at the last stamp", an advisory lagging
///   flag exactly as `SubmitReceipt`'s doc says. (6HE6RF fixed the
///   overstated "backlog mirror" COMMENT; the mechanism is kept.)
/// - [`GrantContract`] — the row-#6 grant-kind contract, now explicit on
///   all three hosts (solve GAINED it: pre-fold a foreign grant reached
///   the solve pump only to die by the `seats.get(slot)` indexing
///   accident — or, under Nominal, to be silently SEATED on a Solver
///   seat; the pooled side aborted loudly).
/// - [`HostDiscipline`] — the per-host abort wording (byte-pinned).
///
/// # Posture consult (behavior-EXACT for solve — spike-proven)
///
/// The consult runs UNCONDITIONALLY in the apply pre-check and the drain
/// guard. For the solve host this is provably a no-op:
/// `FleetHost::posture_admits_role` is `posture.admits_lease(role.
/// cordon_class())`, and `admits_lease` blocks ONLY the
/// `(FleetPosture::Cordoned, CordonClass::Deferrable)` pair
/// (`degenbot_workers::posture`), while `WorkerRole::Solver.cordon_class()`
/// is `CordonClass::Never` (`degenbot_workers::role`) — so the consult is
/// constant-true for Solver and the guard reduces to the cap-only check
/// the solve loop had pre-fold. The same proof covers the `try_enqueue`
/// hand-back arm: `EnqueueError::PostureHeld` fires only for Deferrable
/// units, never a Solver unit. Do NOT re-add a Solver cordon gate by hand
/// — the consult's unconditional presence here IS the point (one shape,
/// one consult, class-derived).
///
/// # Wake discipline (the preserved contract — catalog row #9)
///
/// Both pre-fold loops were message-driven ONLY ("no busy-spin — pump
/// only runs on a message"), and a posture transition does NOT wake a
/// parked backlog: the posture owner is fed by the block-pump thread and
/// intentionally never signals the host channels. A lifted cordon with an
/// idle seat pool and zero in-flight messages leaves held units parked
/// until the next message — never dropped (§10); the registration flood
/// keeps submitting, so a held backlog self-wakes. This shape preserves
/// that verbatim (a posture-to-host wake channel is an explicit non-goal:
/// a new mechanism with no never-drop gain).
pub(crate) struct HostPump<'a> {
    /// The FSM — exclusively owned by this host thread.
    pub(crate) host: &'a mut FleetHost,
    /// The unbounded §10 backlog (FIFO; drains FIRST on every pass).
    pub(crate) backlog: &'a mut VecDeque<Unit>,
    /// The hosted role — the descriptor's pooled role or the Solver pin
    /// role; drives the queue len/cap keys and the posture consult.
    pub(crate) role: WorkerRole,
    /// The row-#6 grant-kind contract (loud on all three hosts).
    pub(crate) grants: GrantContract,
    /// The seat model (P-RZEWTX split).
    pub(crate) sink: &'a dyn SeatSink,
    /// The typed-receipt advisory mirror (solve only; `None` = pooled).
    pub(crate) mirror: Option<&'a AtomicUsize>,
    /// The per-host abort wording owner.
    pub(crate) discipline: &'a dyn HostDiscipline,
}

impl HostPump<'_> {
    /// The ONE dispatch loop (design doc §4): recv → apply → pump. Exits
    /// when the submission channel closes (all executor handles dropped —
    /// process teardown).
    ///
    /// # Lint note
    /// `rx` is taken by value under an explicit lint expectation: the
    /// Receiver's ownership moves into the spawned host thread — a borrow
    /// cannot cross the thread boundary.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the host Receiver's ownership moves into the spawned host thread — a borrow cannot cross the thread boundary"
    )]
    pub(crate) fn run(mut self, rx: mpsc::Receiver<HostMsg>) {
        while let Ok(msg) = rx.recv() {
            self.apply_host_msg(msg);
            self.pump();
        }
    }

    /// Apply one submission or completion (both arrive on the single host
    /// channel — completions can never starve behind a blocking recv).
    pub(crate) fn apply_host_msg(&mut self, msg: HostMsg) {
        match msg {
            HostMsg::Enqueue(unit) => {
                // Pre-check capacity INSTEAD of failing enqueue: the host
                // thread owns every queue mutation, so the check is exact.
                // Units that do not fit spill to the backlog (unbounded,
                // like the legacy pipelines) and drain FIRST on the next
                // pump — never dropped (§10 ledger).
                //
                // The pre-check ALSO consults the ONE shared posture owner
                // (JCI2FW Part A), unconditionally. For Solver this is
                // provably a no-op (the struct doc's proof:
                // `admits_lease` blocks only `(Cordoned, Deferrable)`;
                // `Solver` is `CordonClass::Never`) — the consult's
                // presence is the unified shape, not a new gate.
                if self.host.queue_len(self.role) >= self.host.queue_cap(self.role)
                    || !self.host.posture_admits_role(self.role)
                {
                    self.backlog.push_back(unit);
                    if let Some(mirror) = self.mirror {
                        // The spill stamp: the receipt's advisory reads the
                        // BOUNDED role-queue length at the stamp — not the
                        // backlog depth (the honest mirror note, 6HE6RF).
                        mirror.store(self.host.queue_len(self.role), Ordering::Relaxed);
                    }
                } else if let Err((err, unit)) = self.host.try_enqueue(unit) {
                    if matches!(err, EnqueueError::PostureHeld(_)) {
                        // The shared owner is fed from the throttle-poller
                        // thread (the block pump), NOT this host thread — a
                        // cordon can onset between the check and the
                        // enqueue. Hold, never drop, never abort (§10): the
                        // gate handed the unit BACK (`try_enqueue`), so it
                        // waits in the backlog and re-queues on the next
                        // pump. (For Solver this arm is dead — the proof
                        // above; its presence is the unified hand-back
                        // seam, which the pre-fold solve loop LACKED: the
                        // same input aborted the process.)
                        self.backlog.push_back(unit);
                    } else {
                        // v1-active, non-merge units of this host's role
                        // cannot hit RoleNotActive / MergeNeverQueued, and
                        // QueueFull cannot fire behind the exact same-thread
                        // capacity check. Any such error is a broken
                        // invariant, not a drop.
                        self.discipline.enqueue_refused(&err.to_string());
                    }
                }
            }
            HostMsg::SeatDone { seat } => {
                if let Err(err) = self.host.complete(seat) {
                    // Completion applies the slot's own T-row (T5 pooled →
                    // idle / T3 Solver → warm re-pin) — keyed off the slot
                    // FSM state, so ONE shape serves all three hosts.
                    self.discipline.completion_refused(&err.to_string());
                }
                if let Some(mirror) = self.mirror {
                    mirror.store(self.host.queue_len(self.role), Ordering::Relaxed);
                }
            }
        }
    }

    /// The one precedence grant loop pass (design doc §4): backlog first,
    /// then dispatch grants onto the seats. Grants apply T2 (start) at
    /// grant time — the seat-model delivery IS the claim — and completion
    /// arrives via [`HostMsg::SeatDone`].
    pub(crate) fn pump(&mut self) {
        // Backlog drains FIRST (FIFO across the loud-overflow seam). A
        // backed backlog that cannot enqueue (the cordon hold of Deferrable
        // intake, or a full per-role queue) parks here until the next host
        // message — the awaiting callers already submitted, and retrying on
        // every wake matches the legacy worker semantics (work waits, never
        // drops). Every unit in this host's backlog carries the host's role
        // by construction (the submit seams stamp it), so the role check is
        // the host's own role.
        while self.backlog.front().is_some() {
            // The per-pop guard re-reads BOTH terms live (no mirrors): the
            // cap, and the posture consult (Solver: provably constant-true
            // — the struct doc's proof; the term exists so nobody re-adds
            // a Solver cordon gate by hand).
            if self.host.queue_len(self.role) >= self.host.queue_cap(self.role)
                || !self.host.posture_admits_role(self.role)
            {
                break;
            }
            let Some(unit) = self.backlog.pop_front() else {
                break;
            };
            if let Err((err, unit)) = self.host.try_enqueue(unit) {
                if matches!(err, EnqueueError::PostureHeld(_)) {
                    // The shared owner is fed from the throttle-poller
                    // thread — a cordon can onset between the check and the
                    // enqueue. The gate handed the unit BACK
                    // (`try_enqueue`): the head goes back (FIFO order
                    // preserved) and the drain parks until the next host
                    // message — hold, never drop, never abort (§10).
                    self.backlog.push_front(unit);
                    break;
                }
                self.discipline.fail("backlog drain", &err.to_string());
            }
        }
        loop {
            let grants = self.host.dispatch();
            if grants.is_empty() {
                break;
            }
            for (grant, unit) in grants {
                // Row #6, loud on ALL THREE hosts since 6HE6RF: this host
                // only enqueues its own role's units, so every grant must
                // be one of its contracted kinds. Anything else is a broken
                // host contract, not a drop. (The solve host GAINED this
                // check — pre-fold a foreign grant reached it only to die
                // by the `seats.get` indexing accident, or — under
                // Nominal — to be silently SEATED on a Solver seat.)
                if !self.grants.admits(grant.kind) {
                    self.discipline.foreign_grant(grant.kind);
                }
                if let Err(err) = self.host.start(grant.slot, &unit) {
                    self.discipline.fail("grant start (T2)", &err.to_string());
                }
                self.sink.deliver(self.host, grant, unit);
            }
        }
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

/// The pooled hosts' dispatch loop: build the unified [`HostPump`] for the
/// descriptor's role (the FOLD MAP — everything per-host is a field) and
/// run the ONE recv → apply → pump loop (6HE6RF).
/// (`rx` moves into [`HostPump::run`] — the thread-boundary move the
/// pre-fold loop needed a lint expectation for is now `run`'s.)
fn host_loop(
    rx: mpsc::Receiver<HostMsg>,
    mut host: FleetHost,
    desc: &'static SeatRoleDesc,
    queue: Arc<WorkQueue>,
) {
    let sink = PooledSink { queue };
    let discipline = PooledDiscipline { desc };
    let mut backlog = VecDeque::new();
    HostPump {
        host: &mut host,
        backlog: &mut backlog,
        role: desc.role,
        grants: GrantContract::Single(desc.grant),
        sink: &sink,
        // The pooled port is fire-and-forget (`Result<(), ()>`): there is
        // no typed receipt to inform, so no mirror.
        mirror: None,
        discipline: &discipline,
    }
    .run(rx);
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

// ---------------------------------------------------------------------------
// 6HE6RF: the cross-host property suite. The unified HostPump is driven
// DIRECTLY in both parameterizations (the dispatcher::harness precedent —
// the core is exercised by a deterministic script, not by production
// callers), with the seat-model artifacts (seat counts, keyed-pin chain,
// mailbox routing, arena ids) normalized away by a recording sink.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use degenbot_workers::budget::BudgetOverrides;
    use degenbot_workers::dispatcher::{FleetBoot, FleetHost, Grant, GrantKind, Unit};
    use degenbot_workers::posture::{FleetPosture, PostureOwner, PosturePolicy, ThrottleSample};
    use degenbot_workers::role::WorkerRole;

    use super::{GrantContract, HostDiscipline, HostMsg, HostPump, SeatSink};
    use crate::arb_engine::fleet_solve_executor::SOLVE_BIN_KEY_BASE;

    /// A FRESH hermetic posture owner (leaked to 'static): every test boot
    /// gets its own owner, never the process global (7KAPBB isolation).
    /// Per-file hermeticity stays per-file (the 6HE6RF KILL list): this
    /// suite owns its helper and does not couple the other test binaries.
    fn hermetic_owner() -> &'static PostureOwner {
        std::boxed::Box::leak(std::boxed::Box::new(PostureOwner::new(
            PosturePolicy::doc_defaults(),
        )))
    }

    /// Force the shared owner Cordoned (the event-burst trigger the
    /// dispatcher fixtures use).
    fn force_cordoned(owner: &'static PostureOwner) {
        owner.observe_throttle(
            0,
            ThrottleSample {
                events: 3,
                throttled_usec: 0,
                elapsed_usec: 100_000,
            },
        );
        assert_eq!(owner.current(), FleetPosture::Cordoned);
    }

    /// Feed the full clean hysteresis (10 s of virtual clean ticks since
    /// the dirty sample at now = 0) — the JCI2FW lift.
    fn lift_cordon(owner: &'static PostureOwner) {
        let mut now = 1_000;
        loop {
            owner.observe_throttle(
                now,
                ThrottleSample {
                    events: 0,
                    throttled_usec: 0,
                    elapsed_usec: 1_000,
                },
            );
            if owner.current() == FleetPosture::Nominal {
                break;
            }
            now += 1_000;
            assert!(now <= 60_000, "the cordon never lifted");
        }
    }

    /// The recorded outcome of a driven pass — seat-model artifacts
    /// (`SeatJob` shapes, mailbox routing, arena ids, seat-thread identity)
    /// normalized away to what the cross-host property asserts: WHICH unit
    /// got seated WHERE, and which contract denial fired.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Outcome {
        /// A granted unit reached a seat (unit id, granted slot).
        Seated { unit: u64, slot: u64 },
        /// The row-#6 grant-kind denial fired (recorded, not aborted).
        ForeignGrant(GrantKind),
    }

    /// One shared recorder behind the sink + the discipline.
    struct Recorder {
        outcomes: parking_lot::Mutex<Vec<Outcome>>,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                outcomes: parking_lot::Mutex::new(Vec::new()),
            })
        }

        fn record(&self, outcome: Outcome) {
            self.outcomes.lock().push(outcome);
        }

        fn snapshot(&self) -> Vec<Outcome> {
            self.outcomes.lock().clone()
        }
    }

    /// Recording sink: grants land here instead of real seats (the seat
    /// models themselves are pinned by the executors' own suites — the
    /// property normalizes them away).
    struct RecordingSink {
        recorder: Arc<Recorder>,
    }

    impl SeatSink for RecordingSink {
        fn deliver(&self, _host: &mut FleetHost, grant: Grant, _unit: Unit) {
            self.recorder.record(Outcome::Seated {
                unit: grant.unit,
                slot: grant.slot,
            });
        }
    }

    /// Recording discipline: the row-#6 denial becomes DATA (the
    /// `VerdictRecorder` idiom — never a real process abort under test);
    /// any other failure is an unexpected shape and panics loudly.
    struct RecordingDiscipline {
        recorder: Arc<Recorder>,
    }

    impl HostDiscipline for RecordingDiscipline {
        fn fail(&self, context: &str, err: &str) -> ! {
            panic!("unexpected host failure at {context}: {err}")
        }

        fn enqueue_refused(&self, err: &str) -> ! {
            panic!("unexpected enqueue refusal: {err}")
        }

        fn completion_refused(&self, err: &str) -> ! {
            panic!("unexpected completion refusal: {err}")
        }

        fn foreign_grant(&self, kind: GrantKind) -> ! {
            self.recorder.record(Outcome::ForeignGrant(kind));
            panic!("6HE6RF row-#6 denial recorded: {kind:?}");
        }
    }

    /// The two host kinds the property drives (the parameterization IS the
    /// fold map).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum HostKind {
        /// The pooled pair's registration arm: `PoolStateUpdater` (Deferrable
        /// — the JCI2FW cordon hold is reachable), one grant kind, no
        /// receipt mirror (the fire-and-forget port).
        Pooled,
        /// The solve host: `Solver` (`CordonClass::Never`), the pin-pair
        /// grant contract, the receipt mirror present.
        Solve,
    }

    /// One driven host: a REAL hermetic `FleetHost` behind the unified
    /// `HostPump`, driven by direct `HostMsg` sequences — no seat threads,
    /// fully deterministic; the sink records the grants.
    struct DrivenHost {
        kind: HostKind,
        host: FleetHost,
        backlog: VecDeque<Unit>,
        sink: RecordingSink,
        discipline: RecordingDiscipline,
        recorder: Arc<Recorder>,
        mirror: Option<Arc<AtomicUsize>>,
        cursor: usize,
        in_flight: VecDeque<u64>,
        unit_seq: u64,
    }

    impl DrivenHost {
        fn boot(kind: HostKind) -> Self {
            Self::boot_with(kind, hermetic_owner())
        }

        fn boot_with(kind: HostKind, owner: &'static PostureOwner) -> Self {
            let boot = FleetBoot {
                quota_cpus: 8.0,
                overrides: BudgetOverrides::default(),
                posture: PosturePolicy::doc_defaults(),
                owner: Some(owner),
            };
            let host = FleetHost::boot(boot).expect("hermetic fleet boot");
            let recorder = Recorder::new();
            let mirror = match kind {
                HostKind::Pooled => None,
                HostKind::Solve => Some(Arc::new(AtomicUsize::new(0))),
            };
            Self {
                kind,
                host,
                backlog: VecDeque::new(),
                sink: RecordingSink {
                    recorder: Arc::clone(&recorder),
                },
                discipline: RecordingDiscipline {
                    recorder: Arc::clone(&recorder),
                },
                recorder,
                mirror,
                cursor: 0,
                in_flight: VecDeque::new(),
                unit_seq: 0,
            }
        }

        fn role(&self) -> WorkerRole {
            match self.kind {
                HostKind::Pooled => WorkerRole::PoolStateUpdater,
                HostKind::Solve => WorkerRole::Solver,
            }
        }

        /// The role's next unit (pooled: no key; solve: the ONE bin key —
        /// the single-key chain keeps the drive deterministic: a hot key
        /// waits for its own seat's T6 continuation, so grants are
        /// strictly ordered).
        fn next_unit(&mut self) -> Unit {
            self.next_unit_keyed(Some(SOLVE_BIN_KEY_BASE))
        }

        /// The role's next unit with an EXPLICIT solve pin key (the Q3.4
        /// drive pins TWO seats so the role queue dips below cap while the
        /// backlog is still non-empty — the honest-mirror window).
        fn next_unit_keyed(&mut self, solve_key: Option<u64>) -> Unit {
            self.unit_seq += 1;
            let key = match self.kind {
                HostKind::Pooled => None,
                HostKind::Solve => solve_key,
            };
            Unit::new(self.unit_seq, self.role(), key, false, Box::new(|_ctx| {}))
        }

        fn pump_handle(&mut self) -> HostPump<'_> {
            // Read the copy fields BEFORE the mutable borrows (disjoint
            // field borrows read fine, but the role() method call would
            // re-borrow all of self).
            let role = self.role();
            let grants = match self.kind {
                HostKind::Pooled => GrantContract::Single(GrantKind::PoolStateUpdate),
                HostKind::Solve => GrantContract::SolverPins,
            };
            HostPump {
                host: &mut self.host,
                backlog: &mut self.backlog,
                role,
                grants,
                sink: &self.sink,
                mirror: self.mirror.as_deref(),
                discipline: &self.discipline,
            }
        }

        /// Apply one host message + run one grant pass (the `run()` loop's
        /// per-message shape, driven directly).
        fn apply_and_pump(&mut self, msg: HostMsg) {
            let mut pump = self.pump_handle();
            pump.apply_host_msg(msg);
            pump.pump();
        }

        /// Like `Self::apply_and_pump`, but the row-#6 denial (a recorded,
        /// deliberate panic under the recording discipline) is caught —
        /// the discipline makes the loud abort DATA for the property;
        /// production uses the real process abort.
        fn apply_and_pump_catching_denial(&mut self, msg: HostMsg) {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.apply_and_pump(msg);
            }));
            let _ = result;
        }

        /// Absorb newly recorded grants into the in-flight queue (the
        /// slots awaiting their `SeatDone` message).
        fn absorb_new_grants(&mut self) {
            let snap = self.recorder.snapshot();
            while self.cursor < snap.len() {
                if let Outcome::Seated { slot, .. } = snap[self.cursor] {
                    self.in_flight.push_back(slot);
                }
                self.cursor += 1;
            }
        }

        /// Complete the oldest in-flight grant (one `SeatDone` message +
        /// pass — what a real seat thread does).
        fn complete_oldest_in_flight(&mut self) {
            self.absorb_new_grants();
            let slot = self
                .in_flight
                .pop_front()
                .expect("an in-flight grant to complete");
            self.apply_and_pump(HostMsg::SeatDone { seat: slot });
        }

        /// `SeatDone` choreography: complete outstanding grants, one message
        /// + pass at a time, until want units are seated.
        fn drain_until_seated(&mut self, want: usize) {
            loop {
                self.absorb_new_grants();
                if self.seated_units().len() >= want {
                    break;
                }
                let slot = self
                    .in_flight
                    .pop_front()
                    .expect("an in-flight grant to complete — the drive must have granted first");
                self.apply_and_pump(HostMsg::SeatDone { seat: slot });
            }
            self.absorb_new_grants();
        }

        /// Complete EVERY outstanding grant (the drive runs the host to
        /// quiescence: no unit left running, the final stamp reads the
        /// empty role queue).
        fn run_to_quiescence(&mut self) {
            loop {
                self.absorb_new_grants();
                if self.in_flight.is_empty() {
                    break;
                }
                self.complete_oldest_in_flight();
            }
            self.absorb_new_grants();
        }

        fn seated_units(&self) -> Vec<u64> {
            self.recorder
                .snapshot()
                .iter()
                .filter_map(|outcome| match outcome {
                    Outcome::Seated { unit, .. } => Some(*unit),
                    Outcome::ForeignGrant(_) => None,
                })
                .collect()
        }
    }

    /// THE cross-host property (6HE6RF): drive IDENTICAL unit sequences
    /// into both host kinds and assert IDENTICAL backlog ORDER outcomes —
    /// every unit seated exactly once, in SUBMISSION order (the §10
    /// spill-to-backlog + drain-first FIFO), with the backlog empty at the
    /// end. Seat-model artifacts (when the spill begins, seat counts,
    /// keyed-pin chaining) are normalized away; the queue-cap geometry is
    /// the one legitimate host-kind difference the catalog preserves.
    #[test]
    fn cross_host_identical_unit_sequences_yield_identical_backlog_order() {
        for kind in [HostKind::Pooled, HostKind::Solve] {
            let mut driven = DrivenHost::boot(kind);
            // cap + 2 with seats left busy: the queue fills to the role
            // cap, the last units spill to the unbounded backlog, and the
            // drain-first pump must reorder NOTHING (the same N = cap + 7
            // units into both kinds — the SAME sequence shape, both spill).
            let n = driven.host.queue_cap(driven.role()) + 7;
            for _ in 0..n {
                let unit = driven.next_unit();
                driven.apply_and_pump(HostMsg::Enqueue(unit));
            }
            driven.drain_until_seated(n);
            driven.run_to_quiescence();
            let want: Vec<u64> = (1..=n as u64).collect();
            assert_eq!(
                driven.seated_units(),
                want,
                "{kind:?}: the backlog ORDER outcome must be the submission order                  (the spill never reorders, the drain-first pump is FIFO)"
            );
            assert!(
                driven.backlog.is_empty(),
                "{kind:?}: the backlog must drain empty (never dropped, §10)"
            );
            if let Some(mirror) = &driven.mirror {
                assert_eq!(
                    mirror.load(Ordering::Relaxed),
                    0,
                    "{kind:?}: at rest the role queue is empty and the stamp says so"
                );
            }
        }
    }

    /// Q3.3 (the `push_front` head-preservation pin): hold three pooled
    /// units under a forced cordon (forced BEFORE any unit is in flight —
    /// a cordon onset SHEDS running Deferrable units to Draining (T7),
    /// whose completion rides T8, not the T5 `SeatDone` arm), lift, and
    /// wake the host with ONE message — the held three drain in
    /// SUBMISSION order (the backlog head order survived the hold; the
    /// drain re-queues FIFO).
    #[test]
    fn held_backlog_requeues_fifo_after_the_cordon_lifts() {
        let owner = hermetic_owner();
        let mut driven = DrivenHost::boot_with(HostKind::Pooled, owner);
        // Force the shared owner Cordoned first — Deferrable intake HOLDS.
        force_cordoned(owner);
        for _ in 0..3 {
            let unit = driven.next_unit();
            driven.apply_and_pump(HostMsg::Enqueue(unit));
        }
        assert!(
            driven.seated_units().is_empty(),
            "the cordon HOLDS Deferrable intake — nothing granted"
        );
        assert_eq!(
            driven.backlog.len(),
            3,
            "the held units wait in the unbounded backlog, never dropped"
        );
        // Lift, then wake the parked backlog with ONE submission message.
        lift_cordon(owner);
        let unit = driven.next_unit();
        driven.apply_and_pump(HostMsg::Enqueue(unit));
        driven.run_to_quiescence();
        let seated = driven.seated_units();
        assert_eq!(seated.len(), 4, "all four units seated — nothing dropped");
        let held: Vec<u64> = seated.iter().filter(|u| **u <= 3).copied().collect();
        assert_eq!(
            held,
            vec![1, 2, 3],
            "the held units drain in submission order after the lift              (FIFO head order preserved: 1 before 2 before 3)"
        );
        assert!(
            driven.backlog.is_empty(),
            "the backlog drained empty (never dropped, §10)"
        );
    }

    /// THE posture-consult unification, behavior-pinned (catalog row #1
    /// dissolved): the SAME input — one unit submitted under a FORCED
    /// CORDON — into both host kinds. The unified shape consults posture
    /// unconditionally; the outcomes differ ONLY by the role's own cordon
    /// class, never by host-kind code: the Deferrable registration host
    /// HOLDS intake in the backlog (JCI2FW), while the solve host ADMITS
    /// and grants under the cordon (7OGY5V — the consult is provably
    /// constant-true for `CordonClass::Never`, so the unconditional consult
    /// is behavior-EXACT for solve).
    #[test]
    fn cross_host_cordon_holds_the_deferrable_host_and_never_the_solver_host() {
        // Pooled (registration): held.
        let owner = hermetic_owner();
        let mut pooled = DrivenHost::boot_with(HostKind::Pooled, owner);
        force_cordoned(owner);
        let unit = pooled.next_unit();
        pooled.apply_and_pump(HostMsg::Enqueue(unit));
        assert!(
            pooled.seated_units().is_empty(),
            "Deferrable intake is HELD under cordon"
        );
        assert_eq!(
            pooled.backlog.len(),
            1,
            "held = the unbounded backlog, never a drop"
        );

        // Solve: admitted + granted under the SAME cordon.
        let owner = hermetic_owner();
        let mut solve = DrivenHost::boot_with(HostKind::Solve, owner);
        force_cordoned(owner);
        let unit = solve.next_unit();
        solve.apply_and_pump(HostMsg::Enqueue(unit));
        assert_eq!(
            solve.seated_units(),
            vec![1],
            "Solver intake is posture-INVARIANT (CordonClass::Never): the              unified consult admits and grants under the same cordon"
        );
        assert!(
            solve.backlog.is_empty(),
            "the consult never holds a Solver unit"
        );
    }

    /// THE rows-#2 + #6 dissolution, made observable cross-host: a
    /// foreign-role unit reaching each host's pump under a forced cordon.
    /// Pre-fold, the solve host ABORTED THE PROCESS on this exact input
    /// (the 6HE6RF red: no `try_enqueue` hand-back arm — the pooled host
    /// held the identical input) and, under Nominal, silently SEATED a
    /// foreign grant via the seats.get indexing accident. The unified
    /// shape: BOTH hosts hold the unit in the backlog during the cordon
    /// (never dropped, §10) and BOTH deny the foreign grant at dispatch
    /// (recorded here, aborted in prod) — the unit is NEVER seated.
    #[test]
    fn cross_host_foreign_unit_is_held_never_dropped_and_denied_at_grant_on_both_hosts() {
        // Solve host + a Deferrable registration unit, cordoned.
        let owner = hermetic_owner();
        let mut solve = DrivenHost::boot_with(HostKind::Solve, owner);
        force_cordoned(owner);
        let foreign = Unit::new(
            9_001,
            WorkerRole::PoolStateUpdater,
            None,
            false,
            Box::new(|_ctx| {}),
        );
        solve.apply_and_pump(HostMsg::Enqueue(foreign));
        assert!(
            !solve.backlog.is_empty(),
            "solve: the foreign Deferrable unit is HELD by the unified              try_enqueue hand-back (pre-fold: process abort — the row-#2 red)"
        );
        assert!(
            solve.recorder.snapshot().is_empty(),
            "nothing seated while held"
        );
        // Lift, then wake the drain with a legal Solver unit: the backlog
        // head (the foreign unit) re-queues into the PoolStateUpdater
        // queue, dispatch grants it — and the unified SolverPins contract
        // DENIES it loudly (recorded), before it could reach any seat.
        lift_cordon(owner);
        let unit = solve.next_unit();
        solve.apply_and_pump_catching_denial(HostMsg::Enqueue(unit));
        let outcomes = solve.recorder.snapshot();
        assert!(
            matches!(
                outcomes.last(),
                Some(Outcome::ForeignGrant(GrantKind::PoolStateUpdate))
            ),
            "solve: the foreign grant is denied loudly at dispatch (row #6): {outcomes:?}"
        );
        assert!(
            !outcomes
                .iter()
                .any(|o| matches!(o, Outcome::Seated { unit: 9_001, .. })),
            "the foreign unit is NEVER seated — no silent misroute, no seat-map accident"
        );

        // The pooled mirror: a foreign SOLVER unit, cordoned — held by the
        // host-role consult, denied at grant by the Single(PoolStateUpdate)
        // contract. IDENTICAL visible outcome: held, never dropped, never
        // seated.
        let owner = hermetic_owner();
        let mut pooled = DrivenHost::boot_with(HostKind::Pooled, owner);
        force_cordoned(owner);
        let foreign = Unit::new(
            9_002,
            WorkerRole::Solver,
            // A bin key: Solver leases are PINNED-key (T1's pinnable-key
            // constraints) — a keyless Solver unit could not be granted,
            // and the drive must reach the grant to pin the row-#6 denial.
            Some(SOLVE_BIN_KEY_BASE),
            false,
            Box::new(|_ctx| {}),
        );
        pooled.apply_and_pump(HostMsg::Enqueue(foreign));
        assert!(
            !pooled.backlog.is_empty(),
            "pooled: the foreign unit is HELD under the cordon (the host-role consult)"
        );
        lift_cordon(owner);
        let unit = pooled.next_unit();
        pooled.apply_and_pump_catching_denial(HostMsg::Enqueue(unit));
        let outcomes = pooled.recorder.snapshot();
        assert!(
            matches!(
                outcomes.last(),
                Some(Outcome::ForeignGrant(GrantKind::NewPinClaim))
            ),
            "pooled: the foreign grant is denied loudly at dispatch (row #6): {outcomes:?}"
        );
        assert!(
            !outcomes
                .iter()
                .any(|o| matches!(o, Outcome::Seated { unit: 9_002, .. })),
            "the foreign unit is NEVER seated on the pooled host either"
        );
    }

    /// Q3.4 (the honest mirror, 6HE6RF): the receipt bit tracks the
    /// BOUNDED role-queue stamp — NOT the backlog depth. After the spill
    /// the mirror equals the cap (bit TRUE: the queue WAS at cap); after
    /// the next `SeatDone` stamp the mirror reads the DRAINED queue (bit
    /// FALSE) while the backlog is still NON-EMPTY — the advisory-lag
    /// semantics `SubmitReceipt` documents, pinned so the corrected comment
    /// cannot rot back into "the backlog mirror".
    #[test]
    fn receipt_bit_tracks_the_role_queue_stamp_not_backlog_depth() {
        let mut solve = DrivenHost::boot(HostKind::Solve);
        let cap = solve.host.queue_cap(WorkerRole::Solver);
        let key_a = Some(SOLVE_BIN_KEY_BASE);
        let key_b = Some(SOLVE_BIN_KEY_BASE + 1);
        // Two bin keys: the first two units pin two seats, so the role
        // queue DIPS BELOW cap while the backlog still holds units — the
        // honest-mirror window (a single-key chain would keep refilling
        // the queue to cap on every drain pass).
        let unit = solve.next_unit_keyed(key_a);
        solve.apply_and_pump(HostMsg::Enqueue(unit));
        let unit = solve.next_unit_keyed(key_b);
        solve.apply_and_pump(HostMsg::Enqueue(unit));
        // Fill the queue to the cap, then spill two.
        let mut flip = false;
        let n = cap + 4;
        for _ in 2..n {
            let unit = solve.next_unit_keyed(if flip { key_b } else { key_a });
            flip = !flip;
            solve.apply_and_pump(HostMsg::Enqueue(unit));
        }
        let mirror = Arc::clone(
            solve
                .mirror
                .as_ref()
                .expect("the solve host carries the receipt mirror"),
        );
        let spilled = mirror.load(Ordering::Relaxed);
        assert_eq!(
            spilled, cap,
            "the spill stamps the BOUNDED role-queue length at the stamp (= cap),              not the backlog size"
        );
        // Complete the two in-flight pins: the first SeatDone stamps the
        // FULL queue; the drain-first pump then grants from the queue
        // faster than the 2-deep backlog can refill it — the SECOND
        // SeatDone stamps a SUB-CAP queue while the backlog still holds
        // units: the bit goes FALSE with a non-empty backlog — it does
        // NOT track backlog depth.
        solve.complete_oldest_in_flight();
        solve.complete_oldest_in_flight();
        let after = mirror.load(Ordering::Relaxed);
        assert!(
            after < cap,
            "the stamp follows the role queue ({after} < cap), not the backlog"
        );
        assert!(
            !solve.backlog.is_empty(),
            "the backlog still holds units while the bit reads FALSE — the stamp              is the role-queue length, an advisory lagging flag"
        );
        // Full drain to quiescence: the final SeatDone stamp reads the
        // empty queue.
        solve.drain_until_seated(n);
        solve.run_to_quiescence();
        assert_eq!(
            mirror.load(Ordering::Relaxed),
            0,
            "at rest the role queue is empty and the stamp says so"
        );
    }
}
