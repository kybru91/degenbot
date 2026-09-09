//! The dispatcher: per-role bounded queues, one precedence grant loop, and
//! the slot host that grants leases ONLY along the T-table (design doc §3–§4).
//!
//! Precedence at lease time (design doc §4, reconciled):
//!
//! 1. pinned continuations (T6) — cycle-critical; the pin IS the key;
//! 2. sim-before-solve — queued `SimDriver` units drain before ANY new
//!    `Solver` queue intake when both contend for free slots (a queued sim
//!    preempts queue position, never a running walk);
//! 3. `Solver` queue intake (walk admission capped by the Solver share);
//! 4. `Resolve` chunks fill the remaining pooled capacity.
//!
//! `Merge` is pinned at boot and NEVER queued. Queues are bounded and
//! overflow LOUDLY (ADR-021 posture: classify, stop loudly, never silently
//! drop). The deadlock ledger carries over (§10): a unit whose results feed
//! a pipe is never abandoned silently — abandoning one mid-flight trips the
//! loud-abort tripwire (log at error + `std::process::abort`), mirroring the
//! executor discipline.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::budget::{BudgetError, BudgetOverrides, FleetBudget};
use crate::gauges::{self as gauges_mod, RoleGaugeSample};
use crate::posture::{
    FleetPosture, PostureChange, PosturePolicy, PostureStateMachine, ThrottleSample,
};
use crate::role::{CordonClass, WorkerRole, V1_ACTIVE_ROLES};
use crate::slot::{
    transition, PinKey, RejectedTransition, RejectionReason, SlotState, Transition,
    TransitionContext, UnitId, MERGE_PIN_KEY,
};

/// Worker-slot id within this host.
pub type SlotId = u64;

/// Warm allocator/L1/L2 arena token. Minted when a slot FIRST pins; reused
/// (warm identity) across every cycle while that pin lives; released only at
/// T9 — an arena is never live across a role switch (design doc §3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaToken(u64);

/// A unit of role work. The payload is a `'static + Send` RUST closure — the
/// fleet crate has no pyo3 in it and simulation never round-trips Python
/// (design doc §8), so no worker ever holds a GIL across role work by
/// construction.
pub struct Unit {
    /// Unit id (dispatch bookkeeping).
    pub id: UnitId,
    /// The role this unit executes under.
    pub role: WorkerRole,
    /// The pin key for a Solver bin unit.
    pub key: Option<PinKey>,
    /// Whether this unit's results feed a result pipe (merge/sidecar
    /// discipline: abandoning it mid-flight is a STRANDED PIPE).
    pub result_pipe: bool,
    /// The work payload.
    pub work: Box<dyn FnOnce() + Send>,
}

impl std::fmt::Debug for Unit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unit")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("key", &self.key)
            .field("result_pipe", &self.result_pipe)
            .finish_non_exhaustive()
    }
}

impl Unit {
    /// Build a unit from a Rust closure.
    #[must_use]
    pub fn new(
        id: UnitId,
        role: WorkerRole,
        key: Option<PinKey>,
        result_pipe: bool,
        work: Box<dyn FnOnce() + Send>,
    ) -> Self {
        Self {
            id,
            role,
            key,
            result_pipe,
            work,
        }
    }

    /// An inert `work = || {}` unit for pool/dispatch tests.
    #[must_use]
    pub fn noop(id: UnitId, role: WorkerRole, key: Option<PinKey>) -> Self {
        Self::new(id, role, key, false, Box::new(|| {}))
    }
}

/// Why the fleet refused to boot.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// Budget sum check failed (fail-loud over-subscription).
    #[error("fleet budget refused: {0}")]
    Budget(#[from] BudgetError),
    /// A boot invariant (slot layout / the merge pin) did not hold.
    #[error("fleet boot invariant violated: {0}")]
    Invariant(&'static str),
}

/// Why a queue enqueue was refused (all loud: ADR-021 classify-and-stop).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnqueueError {
    /// Declared role without v1 hosting (Known/planned gating in dispatch).
    #[error(
        "role {0:?} is declared but not v1-active; hosting is a later migration step (ADR-042 Q2)"
    )]
    RoleNotActive(WorkerRole),
    /// Merge is pinned at boot and never queued.
    #[error("merge is pinned at boot and never queued (design doc §4)")]
    MergeNeverQueued,
    /// Bounded queue at capacity.
    #[error("queue full: role {role:?} holds {len}/{cap} — loud overflow, never silent drop")]
    QueueFull {
        /// The overflowing role queue.
        role: WorkerRole,
        /// Current length.
        len: usize,
        /// The bound.
        cap: usize,
    },
    /// The posture holds this role's intake (cordon × deferrable).
    #[error("cordon holds intake for cordon-deferrable role {0:?}")]
    PostureHeld(WorkerRole),
}

/// Why a host state operation was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    /// The T-table rejected the move.
    #[error("illegal transition: {0}")]
    Transition(#[from] RejectedTransition),
    /// Unknown slot id.
    #[error("unknown slot id {0}")]
    UnknownSlot(SlotId),
    /// A unit with in-flight result sends was abandoned mid-flight — the
    /// stranded-pipe tripwire fired (loud abort discipline).
    #[error("stranded result pipe on slot {0}: loud-abort tripwire fired")]
    StrandedPipe(SlotId),
}

/// What kind of dispatch grant this was (harness + telemetry surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantKind {
    /// A pinned slot's next-cycle unit (T6, same key).
    PinContinuation,
    /// A new Solver pin claim from the queue (T1 keyed lease).
    NewPinClaim,
    /// A pooled sim (T1; cordon floors intake).
    Sim,
    /// A pooled resolve chunk (T1).
    Resolve,
}

/// One dispatch grant (slot + unit id; the granted [`Unit`] travels
/// alongside in `dispatch`'s return value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant {
    /// The slot the unit was granted to.
    pub slot: SlotId,
    /// The granted unit id.
    pub unit: UnitId,
    /// The precedence lane that produced this grant.
    pub kind: GrantKind,
}

/// What [`FleetHost::complete`] produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// The unit completed into a steady pin (T3/T4) on `key`.
    Pinned {
        /// The pin key.
        key: PinKey,
    },
    /// The unit completed back to the pooled set (T5).
    BackToIdle,
}

struct SlotCell {
    /// The idle pool this slot belongs to (dashboards render idle fleet
    /// slots per role even when no lease is active).
    home: WorkerRole,
    state: SlotState,
    arena: Option<ArenaToken>,
}

/// The fleet host: slots, queues, posture, budget, telemetry — harness-
/// driven core with no production callers yet (design doc §11; hosting of
/// the real engines is F3–F5).
pub struct FleetHost {
    budget: FleetBudget,
    posture: PostureStateMachine,
    slots: Vec<SlotCell>,
    queues: [VecDeque<Unit>; 8],
    merge_pin: Option<SlotId>,
    /// Pinned (key, slot) pairs (the pin IS the key: continuations grant
    /// only to the slot pinned for that key).
    pins: Vec<(PinKey, SlotId)>,
    epoch_boundary: bool,
    next_arena: u64,
    overflow_count: u64,
    tripwire: Arc<dyn Fn(&str) + Send + Sync>,
}

impl std::fmt::Debug for FleetHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetHost")
            .field("budget", &self.budget)
            .field("posture", &self.posture.state())
            .field("slots", &self.slots.len())
            .finish_non_exhaustive()
    }
}

/// Boot description for [`FleetHost::boot`].
#[derive(Debug, Clone, Copy)]
pub struct FleetBoot {
    /// Fractional cgroup quota (cores), from
    /// [`crate::quota::fractional_cpu_budget`].
    pub quota_cpus: f64,
    /// Terminal typed overrides.
    pub overrides: BudgetOverrides,
    /// Posture thresholds (typed config).
    pub posture: PosturePolicy,
}

impl FleetBoot {
    /// Defaults for tests/hermetic runs: plain fractional quota detection,
    /// no overrides, config-default thresholds.
    #[must_use]
    pub fn from_config(cfg: &degenbot_config::BotConfig) -> Self {
        Self {
            quota_cpus: crate::budget::detected_quota_cpus(&cfg.fleet),
            overrides: BudgetOverrides::from_config(cfg),
            posture: PosturePolicy::from_config(&cfg.fleet),
        }
    }
}

impl FleetHost {
    /// Boot the fleet host: derive the budget (fail-fast), size the slot
    /// table, pin the merge sidecar (T1→T2→T4, exactly one), and
    /// self-register every hosted role into the worker census (§7).
    ///
    /// # Errors
    /// [`BudgetError`] fail-fasts on over-subscription / pinned-role floor.
    pub fn boot(boot: FleetBoot) -> Result<Self, BootError> {
        let budget = FleetBudget::derive(boot.quota_cpus, &boot.overrides)?;
        let posture = PostureStateMachine::new(boot.posture);

        // Slot layout: solver pins, sim slots, resolve, then the merge
        // sidecar (its dedicated slot, pinned below before anything else
        // can claim it).
        let mut slots = Vec::new();
        for _ in 0..budget.solver_pin_count {
            slots.push(SlotCell {
                home: WorkerRole::Solver,
                state: SlotState::Idle,
                arena: None,
            });
        }
        for _ in 0..budget.sim_slot_cap {
            slots.push(SlotCell {
                home: WorkerRole::SimDriver,
                state: SlotState::Idle,
                arena: None,
            });
        }
        for _ in 0..usize::try_from(budget.resolve_cpus).unwrap_or(1) {
            slots.push(SlotCell {
                home: WorkerRole::Resolve,
                state: SlotState::Idle,
                arena: None,
            });
        }
        slots.push(SlotCell {
            home: WorkerRole::Merge,
            state: SlotState::Idle,
            arena: None,
        });

        let mut host = Self {
            budget,
            posture,
            slots,
            queues: Default::default(),
            merge_pin: None,
            pins: Vec::new(),
            epoch_boundary: false,
            next_arena: 1,
            overflow_count: 0,
            tripwire: Arc::new(loud_abort),
        };

        host.register_census();

        // The merge pin: exactly one, pinned at boot (T4) — per-path sends
        // land in a pipe somebody drinks from.
        let Some(merge_slot) = host.merge_slot_id() else {
            return Err(BootError::Invariant("boot did not size a merge slot"));
        };
        let merge_claim = || Unit::noop(0, WorkerRole::Merge, Some(MERGE_PIN_KEY));
        host.lease_claim(merge_slot, WorkerRole::Merge, Some(MERGE_PIN_KEY))
            .map_err(|_| BootError::Invariant("the fresh merge slot rejected its claim (T1)"))?;
        host.start(merge_slot, &merge_claim())
            .map_err(|_| BootError::Invariant("the merge claim start (T2) failed"))?;
        host.complete(merge_slot)
            .map_err(|_| BootError::Invariant("the merge pin conversion (T4) failed"))?;
        Ok(host)
    }

    fn merge_slot_id(&self) -> Option<SlotId> {
        let last = self.slots.len().checked_sub(1)?;
        u64::try_from(last).ok()
    }

    fn register_census(&self) {
        use degenbot_core::worker_census::{register, WorkerCensusEntry};
        for role in V1_ACTIVE_ROLES {
            register(WorkerCensusEntry {
                resource: role.census_resource(),
                kind: role.census_kind(),
                count: self.role_slot_budget(role),
                thread_name: role.thread_name(),
                sizing: role.census_sizing(),
            });
        }
    }

    fn role_slot_budget(&self, role: WorkerRole) -> usize {
        match role {
            WorkerRole::Solver => self.budget.solver_pin_count,
            WorkerRole::SimDriver => self.budget.sim_slot_cap,
            WorkerRole::Resolve => usize::try_from(self.budget.resolve_cpus).unwrap_or(1),
            WorkerRole::Merge => usize::try_from(self.budget.merge_cpus).unwrap_or(1),
            _ => 0,
        }
    }

    // ---- observation surface --------------------------------------------------

    /// The derived budget table (re-derived only via `resize_quota`).
    #[must_use]
    pub const fn budget(&self) -> &FleetBudget {
        &self.budget
    }

    /// Current posture.
    #[must_use]
    pub const fn posture(&self) -> FleetPosture {
        self.posture.state()
    }

    /// The posture machine (thresholds + counters for the tuning loop).
    #[must_use]
    pub const fn posture_machine(&self) -> &PostureStateMachine {
        &self.posture
    }

    /// The full slot state map.
    #[must_use]
    pub fn slot_states(&self) -> Vec<(SlotId, SlotState)> {
        self.slots
            .iter()
            .enumerate()
            .map(|(i, cell)| (u64::try_from(i).unwrap_or(SlotId::MAX), cell.state))
            .collect()
    }

    /// One slot's state (`None` = unknown slot id).
    #[must_use]
    pub fn slot_state(&self, slot: SlotId) -> Option<SlotState> {
        let idx = usize::try_from(slot).ok()?;
        self.slots.get(idx).map(|c| c.state)
    }

    /// The slot's warm arena token: identity is STABLE across cycles while
    /// the pin lives; `None` once released via T9 (§3.4).
    #[must_use]
    pub fn arena(&self, slot: SlotId) -> Option<ArenaToken> {
        let idx = usize::try_from(slot).ok()?;
        self.slots.get(idx).and_then(|c| c.arena)
    }

    /// Slot id of the (unique) merge pin.
    #[must_use]
    pub const fn merge_slot(&self) -> Option<SlotId> {
        self.merge_pin
    }

    /// The pinned (key, slot) table.
    #[must_use]
    pub fn pins(&self) -> &[(PinKey, SlotId)] {
        &self.pins
    }

    /// Slot pinned for `key`, if any.
    #[must_use]
    pub fn pin_slot(&self, key: PinKey) -> Option<SlotId> {
        self.pins.iter().find(|(k, _)| *k == key).map(|(_, s)| *s)
    }

    /// Queue length for a role (declared roles hold nothing).
    #[must_use]
    pub fn queue_len(&self, role: WorkerRole) -> usize {
        role.index_in_all_roles()
            .map(usize::from)
            .and_then(|i| self.queues.get(i))
            .map_or(0, VecDeque::len)
    }

    /// Loud-overflow counter (metric export surface for the ADR-021 posture).
    #[must_use]
    pub const fn overflow_count(&self) -> u64 {
        self.overflow_count
    }

    /// The per-role gauge table (busy/idle per role).
    #[must_use]
    pub fn role_gauges(&self) -> Vec<RoleGaugeSample> {
        self.gauge_rows()
    }

    // ---- posture feed -----------------------------------------------------------

    /// Feed a throttle delta to the posture. Cordon onset immediately sheds
    /// cordon-deferrable in-flight units to Draining (T7) — they always
    /// complete (T8); pinned walks and the merge pin are never shed.
    pub fn observe_throttle(&mut self, now_ms: u64, sample: ThrottleSample) -> PostureChange {
        let change = self.posture.observe(now_ms, sample);
        if matches!(change, PostureChange::Entered(_)) {
            for slot in 0..self.slots.len() {
                let slot = u64::try_from(slot).unwrap_or(SlotId::MAX);
                let Some(state) = self.slot_state(slot) else {
                    continue;
                };
                let Some(role) = state.role() else { continue };
                let sheddable =
                    matches!(state, SlotState::Running { .. } | SlotState::Leased { .. })
                        && role.cordon_class() == CordonClass::Deferrable;
                if sheddable {
                    let _ = self.apply_transition(slot, Transition::BeginDraining);
                }
            }
        }
        change
    }

    // ---- epoch / quota lifecycle -----------------------------------------------

    /// Begin an epoch boundary: pin release (T9) is ONLY legal between this
    /// and [`FleetHost::end_epoch`] — quota re-detection and config
    /// overrides drive it, never a mid-cycle event (design doc §3.3 T9).
    pub fn begin_epoch(&mut self) {
        self.epoch_boundary = true;
    }

    /// End the epoch boundary.
    pub fn end_epoch(&mut self) {
        self.epoch_boundary = false;
    }

    /// Release the pin on `key` (T9, epoch boundary only) and drop its warm
    /// arena — an arena is never live across a role switch (§3.4).
    ///
    /// # Errors
    /// [`HostError::Transition`] (the FSM's `MidCyclePin`) off-boundary.
    pub fn release_pin(&mut self, key: PinKey) -> Result<SlotId, HostError> {
        let slot = self
            .pins
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, s)| *s)
            .ok_or(HostError::UnknownSlot(SlotId::MAX))?;
        self.apply_transition(slot, Transition::ReleasePin)?;
        self.pins.retain(|(k, _)| *k != key);
        if let Some(cell) = usize::try_from(slot)
            .ok()
            .and_then(|i| self.slots.get_mut(i))
        {
            cell.arena = None;
        }
        if self.merge_pin == Some(slot) {
            self.merge_pin = None;
        }
        Ok(slot)
    }

    /// Quota resize: re-derive the budget under the new quota (fail-loud).
    /// Sum is re-checked by construction; pin re-keying happens at an epoch
    /// boundary (T9) that the CALLER opens — the host never re-keys a pin
    /// mid-cycle.
    ///
    /// # Errors
    /// [`BudgetError`] if the new quota cannot host the fleet.
    pub fn resize_quota(
        &mut self,
        new_quota_cpus: f64,
        new_overrides: &BudgetOverrides,
    ) -> Result<(), BudgetError> {
        let next = self.budget.resize(new_quota_cpus, new_overrides)?;
        self.budget = next;
        tracing::info!(
            target: "degenbot::fleet",
            quota = new_quota_cpus,
            solver_pins = self.budget.solver_pin_count,
            sim_slots = self.budget.sim_slot_cap,
            "[fleet-budget] quota re-detected — shares re-declared, sum re-checked"
        );
        Ok(())
    }

    // ---- the dispatch core -------------------------------------------------------

    /// Enqueue a unit into its role's bounded queue. Loud rejections:
    /// declared-not-active roles, merge-never-queued, bounded-queue
    /// overflow (counted + `error!`, ADR-021), posture-held deferrable
    /// intake.
    ///
    /// # Errors
    /// [`EnqueueError`] — every variant is a loud refusal, never a drop.
    pub fn enqueue(&mut self, unit: Unit) -> Result<(), EnqueueError> {
        if !unit.role.v1_active() {
            return Err(EnqueueError::RoleNotActive(unit.role));
        }
        if unit.role == WorkerRole::Merge {
            return Err(EnqueueError::MergeNeverQueued);
        }
        if unit.role.cordon_class() == CordonClass::Deferrable
            && !self.posture.admits_lease(unit.role.cordon_class())
        {
            self.posture.note_intake_suppressed();
            return Err(EnqueueError::PostureHeld(unit.role));
        }
        let cap = self.queue_cap(unit.role);
        let len = self
            .role_queue_mut(unit.role)
            .as_deref()
            .map_or(0, VecDeque::len);
        if len >= cap {
            self.overflow_count += 1;
            tracing::error!(
                target: "degenbot::fleet",
                role = unit.role.label(),
                len,
                cap,
                overflows = self.overflow_count,
                "[fleet-dispatch] queue FULL — loud overflow (ADR-021: classify, stop, never silently drop)"
            );
            return Err(EnqueueError::QueueFull {
                role: unit.role,
                len,
                cap,
            });
        }
        if let Some(queue) = self.role_queue_mut(unit.role) {
            queue.push_back(unit);
        }
        Ok(())
    }

    fn role_queue_mut(&mut self, role: WorkerRole) -> Option<&mut VecDeque<Unit>> {
        let idx = usize::from(role.index_in_all_roles()?);
        self.queues.get_mut(idx)
    }

    fn role_queue(&self, role: WorkerRole) -> Option<&VecDeque<Unit>> {
        let idx = usize::from(role.index_in_all_roles()?);
        self.queues.get(idx)
    }

    fn take_from_role(&mut self, role: WorkerRole) -> Option<Unit> {
        self.role_queue_mut(role)?.pop_front()
    }

    fn return_unit(&mut self, unit: Unit) {
        if let Some(queue) = self.role_queue_mut(unit.role) {
            queue.push_front(unit);
        }
    }

    /// The per-role queue bound: 2× the role's slot budget (pipelining
    /// depth); Merge is unbounded-by-type because it is never queued.
    #[must_use]
    pub fn queue_cap(&self, role: WorkerRole) -> usize {
        match role {
            WorkerRole::Solver => self.budget.solver_pin_count * 2,
            WorkerRole::SimDriver => self.budget.sim_slot_cap * 2,
            WorkerRole::Resolve => usize::try_from(self.budget.resolve_cpus).unwrap_or(1) * 4,
            _ => 0,
        }
    }

    /// The one precedence grant loop (design doc §4). Returns granted
    /// (grant, unit) pairs; the caller executes the unit's Rust closure on
    /// its own worker, then reports via [`FleetHost::start`] / [`FleetHost::complete`]
    /// / [`FleetHost::shed`]. A pinned continuation's `start` applies T6.
    #[must_use]
    pub fn dispatch(&mut self) -> Vec<(Grant, Unit)> {
        let mut grants = Vec::new();

        // 1. Pinned continuations (T6): cycle-critical, keyed to their pin.
        let pin_snapshot: Vec<(PinKey, SlotId)> = self.pins.clone();
        for (key, slot) in pin_snapshot {
            let is_solver_pin = matches!(
                self.slot_state(slot),
                Some(SlotState::Pinned {
                    role: WorkerRole::Solver,
                    ..
                })
            );
            if !is_solver_pin || self.pin_queue_len(key) == 0 {
                continue;
            }
            let Some(unit) = self.take_solver_unit_for(key) else {
                continue;
            };
            grants.push((
                Grant {
                    slot,
                    unit: unit.id,
                    kind: GrantKind::PinContinuation,
                },
                unit,
            ));
        }

        // 2. sim-before-solve: queued sims drain before ANY new Solver
        //    queue intake, pooled slots permitting, cordon intake-cap aware.
        let sim_intake_cap = self.posture.sim_intake_cap(self.budget.sim_slot_cap);
        let mut sim_busy = self.count_leased_or_running(WorkerRole::SimDriver);
        while sim_busy < sim_intake_cap {
            let Some(idle) = self.first_idle_slot() else {
                break;
            };
            let Some(unit) = self.take_from_role(WorkerRole::SimDriver) else {
                break;
            };
            if self.lease(idle, WorkerRole::SimDriver, None).is_err() {
                self.return_unit(unit);
                break;
            }
            grants.push((
                Grant {
                    slot: idle,
                    unit: unit.id,
                    kind: GrantKind::Sim,
                },
                unit,
            ));
            sim_busy += 1;
        }

        // 3. Solver queue intake: new pin claims, admission-capped by the
        //    Solver CPU share (a gated bin parks — §5 note). Only reached
        //    after the sim queue drained: sim-before-solve at lease time.
        //    A unit whose key is HOT (pinned or in flight on its seat) is
        //    skipped here — it is granted only via T6 onto its OWN seat by
        //    the continuation lane; granting a hot key cold would seat one
        //    bin on two workers (the pin IS the key, §3.4).
        let admission_cap = usize::try_from(self.budget.solver_cpus).unwrap_or(1);
        while self.count_leased_or_running(WorkerRole::Solver) < admission_cap {
            let Some(idle) = self.first_idle_slot() else {
                break;
            };
            let Some(pos) = self.first_cold_solver_pos() else {
                break;
            };
            let Some(unit) = self
                .role_queue_mut(WorkerRole::Solver)
                .and_then(|q| q.remove(pos))
            else {
                break;
            };
            let key = unit.key;
            if self.lease(idle, WorkerRole::Solver, key).is_err() {
                self.return_unit(unit);
                break;
            }
            grants.push((
                Grant {
                    slot: idle,
                    unit: unit.id,
                    kind: GrantKind::NewPinClaim,
                },
                unit,
            ));
        }

        // 4. Resolve chunks fill the remaining pooled capacity.
        while let Some(idle) = self.first_idle_slot() {
            let Some(unit) = self.take_from_role(WorkerRole::Resolve) else {
                break;
            };
            if self.lease(idle, WorkerRole::Resolve, None).is_err() {
                self.return_unit(unit);
                break;
            }
            grants.push((
                Grant {
                    slot: idle,
                    unit: unit.id,
                    kind: GrantKind::Resolve,
                },
                unit,
            ));
        }

        self.export_gauges();
        grants
    }

    /// Position of the FIRST queued Solver unit whose key is COLD — not
    /// pinned and not in flight on its seat. Hot-keyed units wait for their
    /// own seat's T6 continuation (a hot key granted cold would seat one
    /// bin on two workers, breaking the one-seat-per-bin contract).
    fn first_cold_solver_pos(&self) -> Option<usize> {
        self.role_queue(WorkerRole::Solver)?
            .iter()
            .position(|u| !self.solver_key_is_hot(u.key))
    }

    /// Whether a keyed Solver unit currently has a claimed seat — a live
    /// pin ([`FleetHost::pin_slot`]) or an in-flight Leased/Running unit
    /// carrying the same key.
    fn solver_key_is_hot(&self, key: Option<PinKey>) -> bool {
        let Some(key) = key else {
            return false;
        };
        if self.pin_slot(key).is_some() {
            return true;
        }
        self.slots.iter().any(|c| {
            matches!(
                c.state,
                SlotState::Leased { role: WorkerRole::Solver, key: Some(k) }
                | SlotState::Running { role: WorkerRole::Solver, key: Some(k) }
                    if k == key
            )
        })
    }

    fn pin_queue_len(&self, key: PinKey) -> usize {
        self.role_queue(WorkerRole::Solver)
            .map_or(0, |q| q.iter().filter(|u| u.key == Some(key)).count())
    }

    fn take_solver_unit_for(&mut self, key: PinKey) -> Option<Unit> {
        let queue = self.role_queue_mut(WorkerRole::Solver)?;
        let pos = queue.iter().position(|u| u.key == Some(key))?;
        queue.remove(pos)
    }
    // (index conversions: usize::from(u8) is infallible)

    fn first_idle_slot(&self) -> Option<SlotId> {
        let pos = self.slots.iter().position(|c| c.state == SlotState::Idle)?;
        u64::try_from(pos).ok()
    }

    fn count_leased_or_running(&self, role: WorkerRole) -> usize {
        self.slots
            .iter()
            .filter(|c| {
                matches!(
                    c.state,
                    SlotState::Leased { .. } | SlotState::Running { .. }
                ) && c.state.role() == Some(role)
            })
            .count()
    }

    // ---- T-table application --------------------------------------------------------

    fn apply_transition(&mut self, slot: SlotId, t: Transition) -> Result<SlotState, HostError> {
        let idx = usize::try_from(slot).map_err(|_| HostError::UnknownSlot(slot))?;
        let from = self
            .slots
            .get(idx)
            .ok_or(HostError::UnknownSlot(slot))?
            .state;
        let ctx = TransitionContext {
            at_epoch_boundary: self.epoch_boundary,
            posture_admits_role: from
                .role()
                .is_none_or(|r| self.posture.admits_lease(r.cordon_class())),
        };
        match transition(from, t, ctx) {
            Ok(to) => {
                self.slots[idx].state = to;
                Ok(to)
            }
            Err(rejected) => {
                tracing::error!(
                    target: "degenbot::fleet",
                    slot,
                    from = ?rejected.from,
                    transition = ?rejected.transition,
                    reason = %rejected.reason,
                    "[fleet-fsm] transition REJECTED — off the T-table"
                );
                Err(HostError::Transition(rejected))
            }
        }
    }

    fn lease(
        &mut self,
        slot: SlotId,
        role: WorkerRole,
        key: Option<PinKey>,
    ) -> Result<(), HostError> {
        self.apply_transition(slot, Transition::Lease { role, key })
            .map(|_| ())
    }

    /// Commit a unit onto a slot: T2 (Leased → Running) or T6 (Pinned →
    /// Running, key-matched at the host — the pin IS the key, dispatched
    /// only to its own slot).
    ///
    /// # Errors
    /// [`HostError::Transition`] off the table.
    pub fn start(&mut self, slot: SlotId, unit: &Unit) -> Result<(), HostError> {
        let state = self.slot_state(slot).ok_or(HostError::UnknownSlot(slot))?;
        if let SlotState::Pinned { key, .. } = state {
            if unit.key != Some(key) {
                return Err(HostError::Transition(RejectedTransition {
                    from: state,
                    transition: Transition::Start { unit: unit.id },
                    reason: RejectionReason::NoLegalRow,
                }));
            }
        }
        self.apply_transition(slot, Transition::Start { unit: unit.id })
            .map(|_| ())
    }

    /// Complete the in-flight unit: T3/T4 (pinnable roles convert to a warm
    /// pin, minting/reusing its arena) or T5 (pooled roles return to idle).
    ///
    /// # Errors
    /// [`HostError::Transition`] off the table.
    pub fn complete(&mut self, slot: SlotId) -> Result<Completion, HostError> {
        let state = self.slot_state(slot).ok_or(HostError::UnknownSlot(slot))?;
        let to = match state {
            SlotState::Running {
                role: WorkerRole::SimDriver | WorkerRole::Resolve,
                ..
            } => self.apply_transition(slot, Transition::CompleteToIdle)?,
            SlotState::Running {
                role: WorkerRole::Solver | WorkerRole::Merge,
                ..
            } => self.apply_transition(slot, Transition::CompleteToPinned)?,
            _ => {
                return Err(HostError::Transition(RejectedTransition {
                    from: state,
                    transition: Transition::CompleteToIdle,
                    reason: RejectionReason::NoLegalRow,
                }));
            }
        };
        match to {
            SlotState::Idle => {
                self.export_gauges();
                Ok(Completion::BackToIdle)
            }
            SlotState::Pinned { role, key } => {
                // Mint the warm arena on first pin; reuse across cycles.
                if let Some(cell) = usize::try_from(slot)
                    .ok()
                    .and_then(|i| self.slots.get_mut(i))
                {
                    if cell.arena.is_none() {
                        cell.arena = Some(ArenaToken(self.next_arena));
                        self.next_arena += 1;
                    }
                }
                self.pins.retain(|(k, _)| *k != key);
                self.pins.push((key, slot));
                if role == WorkerRole::Merge {
                    self.merge_pin = Some(slot);
                }
                self.export_gauges();
                Ok(Completion::Pinned { key })
            }
            _ => Err(HostError::Transition(RejectedTransition {
                from: state,
                transition: Transition::CompleteToIdle,
                reason: RejectionReason::NoLegalRow,
            })),
        }
    }

    /// Shed a running/leased unit: T7 (cordon onset / resize — the in-flight
    /// unit ALWAYS completes), then [`FleetHost::drain_done`] for T8.
    ///
    /// # Errors
    /// [`HostError::Transition`] off the table.
    pub fn shed(&mut self, slot: SlotId) -> Result<(), HostError> {
        self.apply_transition(slot, Transition::BeginDraining)
            .map(|_| ())
    }

    /// Draining done: T8 back to idle.
    ///
    /// # Errors
    /// [`HostError::Transition`] off the table.
    pub fn drain_done(&mut self, slot: SlotId) -> Result<(), HostError> {
        self.apply_transition(slot, Transition::DrainComplete)
            .map(|_| ())
    }

    /// Lease a pin claim onto a specific idle slot (Solver bins / the merge
    /// sidecar): T1 with the pinnable-key constraints.
    ///
    /// # Errors
    /// [`HostError::Transition`] off the table.
    pub fn lease_claim(
        &mut self,
        slot: SlotId,
        role: WorkerRole,
        key: Option<PinKey>,
    ) -> Result<(), HostError> {
        self.lease(slot, role, key)
    }

    /// The stranded-pipe tripwire: abandoning a unit whose results feed a
    /// pipe WITHOUT draining it first hits the loud-abort path (log at
    /// error + `std::process::abort`) — the executor discipline carried
    /// over verbatim (design doc §10.3). The harness injects an observing
    /// tripwire via [`FleetHost::with_tripwire_observer`].
    ///
    /// # Errors
    /// Always: [`HostError::StrandedPipe`] (the default handler aborts
    /// before returning).
    pub fn strand_unit(&mut self, slot: SlotId) -> Result<(), HostError> {
        tracing::error!(
            target: "degenbot::fleet",
            slot,
            "[fleet-deadlock] stranded result pipe: a dead host abandons a unit with in-flight result sends — loud abort (design doc §10.3)"
        );
        (self.tripwire)("stranded result pipe: unit abandoned mid-drain");
        Err(HostError::StrandedPipe(slot))
    }

    /// Install a tripwire observer (the default is the loud abort; test-
    /// declared only, mirroring the conformance-stub pattern).
    #[must_use]
    pub fn with_tripwire_observer(mut self, tripwire: Arc<dyn Fn(&str) + Send + Sync>) -> Self {
        self.tripwire = tripwire;
        self
    }

    // ---- gauge export -------------------------------------------------------------

    fn gauge_rows(&self) -> Vec<RoleGaugeSample> {
        let (mut idle, mut leased, mut running, mut pinned, mut draining) =
            ([0_u64; 8], [0_u64; 8], [0_u64; 8], [0_u64; 8], [0_u64; 8]);
        for cell in &self.slots {
            let idx = cell.home.index_in_all_roles().map_or(0, usize::from);
            match cell.state {
                SlotState::Idle => idle[idx] += 1,
                SlotState::Leased { .. } => leased[idx] += 1,
                SlotState::Running { .. } => running[idx] += 1,
                SlotState::Pinned { .. } => pinned[idx] += 1,
                SlotState::Draining { .. } => draining[idx] += 1,
            }
        }
        gauges_mod::sample_table(&idle, &leased, &running, &pinned, &draining)
    }

    fn export_gauges(&self) {
        gauges_mod::export_dashboard(&self.gauge_rows());
    }
}

/// The default loud-abort tripwire (executor discipline, §10.3): the error
/// is logged by [`FleetHost::strand_unit`], then the process aborts —
/// swallowing the error is never an option.
fn loud_abort(_reason: &str) {
    std::process::abort();
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod harness;
