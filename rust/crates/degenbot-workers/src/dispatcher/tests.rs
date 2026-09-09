//! Dispatcher/host unit tests: bounded queues, precedence, loud overflow,
//! cordon intake, census + gauge integration.
#![expect(clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::posture::PosturePolicy;

fn boot() -> FleetBoot {
    FleetBoot {
        quota_cpus: 8.0,
        overrides: BudgetOverrides::default(),
        posture: PosturePolicy {
            enter_events: 2,
            enter_window_ms: 1_000,
            duty_percent: 2.0,
            duty_window_ms: 5_000,
            exit_clean_ms: 10_000,
            sim_intake_floor_override: None,
        },
    }
}

fn host() -> FleetHost {
    FleetHost::boot(boot()).expect("8-core boot")
}

fn policy() -> PosturePolicy {
    PosturePolicy {
        enter_events: 2,
        enter_window_ms: 1_000,
        duty_percent: 2.0,
        duty_window_ms: 5_000,
        exit_clean_ms: 10_000,
        sim_intake_floor_override: None,
    }
}

#[test]
fn boot_fails_loudly_on_an_unhostable_quota() {
    let err = FleetHost::boot(FleetBoot {
        quota_cpus: 4.5,
        overrides: BudgetOverrides::default(),
        posture: policy(),
    })
    .expect_err("H+A+R+M+2 > 4");
    assert!(matches!(err, BootError::Budget(_)));
}

#[test]
fn boot_pins_exactly_one_merge_and_registers_the_census() {
    let host = host();
    assert_eq!(host.budget().declared_sum(), host.budget().quota_floor);
    let merge = host.merge_slot().expect("merge pinned at boot");
    assert_eq!(
        host.slot_state(merge),
        Some(SlotState::Pinned {
            role: WorkerRole::Merge,
            key: MERGE_PIN_KEY
        })
    );
    // Exactly one merge pin exists.
    assert_eq!(
        host.slot_states()
            .iter()
            .filter(|(_, s)| matches!(
                s,
                SlotState::Pinned {
                    role: WorkerRole::Merge,
                    ..
                }
            ))
            .count(),
        1
    );
    // Census self-registration: the four v1-active roles are visible with
    // their budget-derived slot counts.
    let snap = degenbot_core::worker_census::snapshot();
    let budget = host.budget();
    let expected = |role: WorkerRole| match role {
        WorkerRole::Solver => budget.solver_pin_count,
        WorkerRole::SimDriver => budget.sim_slot_cap,
        WorkerRole::Resolve => usize::try_from(budget.resolve_cpus).unwrap_or(1),
        WorkerRole::Merge => usize::try_from(budget.merge_cpus).unwrap_or(1),
        WorkerRole::PoolStateUpdater => budget.pool_state_updater_slots,
        _ => 0,
    };
    for role in V1_ACTIVE_ROLES {
        let entry = snap.iter().find(|e| e.resource == role.census_resource());
        assert!(entry.is_some(), "census row missing for {role:?}");
        let entry = entry.unwrap_or(&degenbot_core::worker_census::WorkerCensusEntry {
            resource: "",
            kind: "",
            count: 0,
            thread_name: "",
            sizing: "",
        });
        assert_eq!(entry.count, expected(role));
        assert_eq!(entry.thread_name, role.thread_name());
    }
}

#[test]
fn merge_is_never_queued_and_declared_roles_are_gated() {
    let mut host = host();
    assert_eq!(
        host.enqueue(Unit::noop(1, WorkerRole::Merge, None)),
        Err(EnqueueError::MergeNeverQueued)
    );
    for role in [WorkerRole::Registrar, WorkerRole::Submitter] {
        assert_eq!(
            host.enqueue(Unit::noop(2, role, None)),
            Err(EnqueueError::RoleNotActive(role))
        );
    }
}

/// RED→GREEN (BCA77G port): a keyed Solver unit whose pin is claimed
/// (in-flight or running on its pinned seat) must WAIT for its own seat —
/// the dispatch lanes must never grant a hot key as a new pin claim onto a
/// second idle seat (the pin IS the key; a double grant breaks the
/// one-seat-per-bin RAYPAR T3 contract).
#[test]
fn a_busy_pinned_key_never_grants_a_second_seat() {
    let mut host = host();
    // Claim the key-1 pin and leave it RUNNING (no T3 completion yet).
    let solver_slot = first_idle_of(&host, WorkerRole::Solver);
    host.lease_claim(solver_slot, WorkerRole::Solver, Some(1))
        .expect("T1 claim");
    host.start(solver_slot, &Unit::noop(1, WorkerRole::Solver, Some(1)))
        .expect("T2");
    // Queue: a continuation for the BUSY key 1, plus a new claim on a
    // COLD key 2, plus another continuation for key 1 behind it.
    host.enqueue(Unit::noop(10, WorkerRole::Solver, Some(1)))
        .expect("continuation (busy key)");
    host.enqueue(Unit::noop(11, WorkerRole::Solver, Some(2)))
        .expect("cold-key claim");
    host.enqueue(Unit::noop(12, WorkerRole::Solver, Some(1)))
        .expect("second continuation (busy key)");
    let grants = host.dispatch();
    let solver_grants: Vec<_> = grants
        .iter()
        .filter(|(g, _)| g.kind != GrantKind::Sim)
        .collect();
    assert_eq!(
        solver_grants.len(),
        1,
        "only the COLD key 2 may claim a seat while key 1 is hot: {solver_grants:?}"
    );
    assert_eq!(solver_grants[0].0.kind, GrantKind::NewPinClaim);
    assert_ne!(
        solver_grants[0].0.slot, solver_slot,
        "the hot key's seat must not be touched"
    );
    // The busy key's units stay queued for their pin continuation (T6
    // after T3), not dropped and not re-seated.
    assert_eq!(host.queue_len(WorkerRole::Solver), 2);
}

#[test]
fn queue_overflow_is_loud_and_counted_never_silent() {
    let mut host = host();
    let cap = host.queue_cap(WorkerRole::SimDriver);
    assert!(cap > 0);
    for i in 0..cap {
        host.enqueue(Unit::noop(
            u64::try_from(i).unwrap_or(u64::MAX) + 10,
            WorkerRole::SimDriver,
            None,
        ))
        .expect("within the bound");
    }
    let err = host
        .enqueue(Unit::noop(9999, WorkerRole::SimDriver, None))
        .expect_err("one past the bound overflows loudly");
    assert!(matches!(err, EnqueueError::QueueFull { .. }));
    assert_eq!(host.overflow_count(), 1);
}

#[test]
fn sim_before_solve_at_lease_time_and_solver_pins_first_via_continuations() {
    let mut host = host();
    // Claim one solver pin (cycle-critical): T1→T2→T3 by hand.
    let solver_slot = first_idle_of(&host, WorkerRole::Solver);
    host.lease_claim(solver_slot, WorkerRole::Solver, Some(1))
        .expect("T1 claim");
    host.start(solver_slot, &Unit::noop(1, WorkerRole::Solver, Some(1)))
        .expect("T2");
    host.complete(solver_slot).expect("T3");

    // Enqueue BOTH a sim and a walk with pooled slots scarce: the sim
    // queue drains first for new intake.
    host.enqueue(Unit::noop(10, WorkerRole::SimDriver, None))
        .expect("sim");
    host.enqueue(Unit::noop(11, WorkerRole::Solver, Some(2)))
        .expect("walk claim");
    let grants = host.dispatch();
    let kinds: Vec<GrantKind> = grants.iter().map(|(g, _)| g.kind).collect();
    // The queued walk on the PINNED key 1 continues first (T6).
    // (Key 2's claim lands only after sims: enqueue a continuation now.)
    host.enqueue(Unit::noop(12, WorkerRole::Solver, Some(1)))
        .expect("continuation");
    let grants2 = host.dispatch();
    if let Some(sim_pos) = kinds.iter().position(|k| *k == GrantKind::Sim) {
        assert!(
            !kinds[..sim_pos].contains(&GrantKind::NewPinClaim),
            "new Solver intake must not precede queued sims: {kinds:?}"
        );
    }
    let continuation = grants2
        .iter()
        .find(|(g, _)| g.kind == GrantKind::PinContinuation)
        .expect("pinned key continuations grant first");
    assert_eq!(continuation.0.slot, solver_slot, "the pin IS the key");
}

fn first_idle_of(host: &FleetHost, role: WorkerRole) -> crate::dispatcher::SlotId {
    // Boot layout: [solver pins][sim slots][resolve][merge]. Find the first
    // idle slot of the requested home by probing an idle state.
    let slot_total = u64::try_from(host.slot_states().len()).unwrap_or(0);
    for slot in 0..slot_total {
        if host.slot_state(slot) == Some(SlotState::Idle) {
            // Home is encoded by position: derive from budget layout.
            let pins = host.budget().solver_pin_count;
            let sims = host.budget().sim_slot_cap;
            let idx = usize::try_from(slot).unwrap_or(usize::MAX);
            let expected = if idx < pins {
                WorkerRole::Solver
            } else if idx < pins + sims {
                WorkerRole::SimDriver
            } else if idx < pins + sims + 1 {
                WorkerRole::Resolve
            } else {
                WorkerRole::Merge
            };
            if expected == role {
                return slot;
            }
        }
    }
    // A scripted role without an idle home slot is a boot-layout bug: the
    // harness fails loudly without a process-level panic (lspec)
    let idle = host
        .slot_states()
        .iter()
        .find(|(_, s)| *s == SlotState::Idle)
        .map(|(s, _)| *s);
    assert!(
        idle.is_some(),
        "no idle {role:?} slot in the boot layout (no idle slot at all)"
    );
    idle.unwrap_or(0)
}

#[test]
fn cordon_floors_sim_intake_but_never_cancels_in_flight() {
    let mut host = host();
    // Fill all sim slots with running units BEFORE cordon.
    let cap = host.budget().sim_slot_cap;
    for i in 0..cap {
        host.enqueue(Unit::noop(
            100 + u64::try_from(i).unwrap_or(0),
            WorkerRole::SimDriver,
            None,
        ))
        .expect("enqueue");
    }
    let grants = host.dispatch();
    assert_eq!(
        grants
            .iter()
            .filter(|(g, _)| g.kind == GrantKind::Sim)
            .count(),
        cap
    );
    for (g, unit) in &grants {
        host.start(g.slot, unit).expect("T2");
    }

    // Cordon onset.
    let change = host.observe_throttle(
        0,
        crate::posture::ThrottleSample {
            events: 3,
            throttled_usec: 0,
            elapsed_usec: 100_000,
        },
    );
    assert!(matches!(change, crate::posture::PostureChange::Entered(_)));
    assert_eq!(host.posture(), FleetPosture::Cordoned);
    // In-flight sims were never cancelled.
    assert_eq!(
        host.slot_states()
            .iter()
            .filter(|(_, s)| matches!(
                s,
                SlotState::Running {
                    role: WorkerRole::SimDriver,
                    ..
                }
            ))
            .count(),
        cap
    );
    // Complete them; intake beyond the floor is refused by the grant loop.
    for (g, _) in &grants {
        host.complete(g.slot).expect("T5");
    }
    // The floor is half the cap: queue two more sims; only the floor grants.
    let floor = host.posture_machine().sim_intake_cap(cap);
    for i in 0..cap {
        host.enqueue(Unit::noop(
            200 + u64::try_from(i).unwrap_or(0),
            WorkerRole::SimDriver,
            None,
        ))
        .expect("enqueue");
    }
    let granted_now = host.dispatch();
    assert_eq!(
        granted_now
            .iter()
            .filter(|(g, _)| g.kind == GrantKind::Sim)
            .count(),
        floor,
        "cordon floors new sim intake at half the cap"
    );
}

#[test]
fn solver_admission_is_gated_by_the_cpu_share() {
    let mut host = host();
    let share = usize::try_from(host.budget().solver_cpus).unwrap_or(1);
    for (offset, key) in (1_u64..=(2 * u64::try_from(share).unwrap_or(0))).enumerate() {
        host.enqueue(Unit::noop(
            300 + u64::try_from(offset).unwrap_or(0),
            WorkerRole::Solver,
            Some(key),
        ))
        .expect("enqueue");
    }
    let grants = host.dispatch();
    assert_eq!(
        grants
            .iter()
            .filter(|(g, _)| g.kind == GrantKind::NewPinClaim)
            .count(),
        share,
        "at most S walks runnable concurrently — a gated bin parks"
    );
}

#[test]
fn gauge_rows_and_the_dashboard_hook_see_the_fleet() {
    static ROWS: AtomicUsize = AtomicUsize::new(0);
    fn hook(samples: &[RoleGaugeSample]) {
        ROWS.store(samples.len(), Ordering::SeqCst);
    }
    // Process-global first-wins: the funnel assertion holds only when THIS
    // test installed the hook (parallel siblings may have won it).
    let installed = crate::gauges::set_dashboard_hook(hook);
    let host = host();
    let rows = host.role_gauges();
    assert_eq!(rows.len(), 8);
    // Busy + idle == total per role; merge is pinned at boot -> busy 1.
    for row in &rows {
        assert_eq!(row.busy() + row.idle, row.total());
    }
    let merge_row = rows
        .iter()
        .find(|r| r.role == WorkerRole::Merge)
        .expect("merge row");
    assert_eq!(merge_row.pinned, 1);
    assert_eq!(merge_row.busy(), 1);
    if installed {
        assert_eq!(ROWS.load(Ordering::SeqCst), 8);
    }
}

#[test]
fn the_stranded_pipe_trips_the_loud_abort_path() {
    let tripped = std::sync::Arc::new(AtomicUsize::new(0));
    let observer = std::sync::Arc::clone(&tripped);
    let host = FleetHost::boot(boot())
        .expect("boot")
        .with_tripwire_observer(Arc::new(move |_reason| {
            observer.fetch_add(1, Ordering::SeqCst);
        }));
    let mut host = host;
    let sim_slot = first_idle_of(&host, WorkerRole::SimDriver);
    host.lease_claim(sim_slot, WorkerRole::SimDriver, None)
        .expect("T1");
    let unit = Unit::new(1, WorkerRole::SimDriver, None, true, Box::new(|| {}));
    host.start(sim_slot, &unit).expect("T2");
    // Abandoning a result-pipe unit mid-flight trips the loud abort path...
    assert!(host.strand_unit(sim_slot).is_err());
    assert_eq!(
        tripped.load(Ordering::SeqCst),
        1,
        "tripwire fired once, loudly"
    );
    // ...while the legal shed path (T7/T8) never trips it.
    host.shed(sim_slot).expect("T7");
    host.drain_done(sim_slot).expect("T8");
    assert_eq!(tripped.load(Ordering::SeqCst), 1);
}

#[test]
fn the_intake_station_is_booted_and_census_registered() {
    let host = host();
    let slots = host.budget().pool_state_updater_slots;
    assert!(slots >= 1, "the station hosts at least one slot by default");
    // The merge pin is still structurally the LAST slot.
    let last = host.slot_states().last().expect("slots").1;
    assert!(
        matches!(
            last,
            SlotState::Pinned {
                role: WorkerRole::Merge,
                ..
            }
        ),
        "merge pin is the last slot"
    );
    // Census row registered with the duty-counted budget.
    let snap = degenbot_core::worker_census::snapshot();
    let entry = snap
        .iter()
        .find(|e| e.resource == WorkerRole::PoolStateUpdater.census_resource())
        .expect("census row for the intake station");
    assert_eq!(entry.count, slots);
    assert_eq!(
        entry.thread_name,
        WorkerRole::PoolStateUpdater.thread_name()
    );
}

#[test]
fn intake_units_admit_nominal_and_held_while_cordoned() {
    let mut host = host();
    host.enqueue(Unit::noop(1, WorkerRole::PoolStateUpdater, None))
        .expect("nominal intake admits");

    // Cordon onset (same trigger the sim fixture uses).
    let change = host.observe_throttle(
        0,
        crate::posture::ThrottleSample {
            events: 3,
            throttled_usec: 0,
            elapsed_usec: 100_000,
        },
    );
    assert!(matches!(change, crate::posture::PostureChange::Entered(_)));
    host.enqueue(Unit::noop(2, WorkerRole::PoolStateUpdater, None))
        .expect_err("Deferrable intake is held while cordoned");
}

#[test]
fn intake_grants_run_behind_solve_sim_and_resolve_precedence() {
    let mut host = host();
    // Queue one unit for EVERY grant-step role plus the station.
    host.enqueue(Unit::noop(1, WorkerRole::SimDriver, None))
        .expect("sim");
    host.enqueue(Unit::noop(2, WorkerRole::Solver, Some(0x10)))
        .expect("solver");
    host.enqueue(Unit::noop(3, WorkerRole::Resolve, None))
        .expect("resolve");
    host.enqueue(Unit::noop(4, WorkerRole::PoolStateUpdater, None))
        .expect("intake");
    let grants = host.dispatch();
    let kinds: Vec<GrantKind> = grants.iter().map(|(g, _)| g.kind).collect();
    let poolupd_pos = kinds
        .iter()
        .position(|k| *k == GrantKind::PoolStateUpdate)
        .expect("the intake unit was granted");
    assert_eq!(
        grants
            .iter()
            .filter(|(g, _)| g.kind == GrantKind::PoolStateUpdate)
            .count(),
        1,
        "one intake grant for one queued unit"
    );
    // Every higher-precedence grant precedes the intake grant.
    assert!(
        kinds[..poolupd_pos]
            .iter()
            .all(|k| *k != GrantKind::PoolStateUpdate),
        "intake grant is last"
    );
}

#[test]
fn the_intake_queue_is_bounded_per_role() {
    let host = host();
    let slots = host.budget().pool_state_updater_slots;
    assert_eq!(
        host.queue_cap(WorkerRole::PoolStateUpdater),
        slots * 2,
        "the per-role bound is 2x the slot cap (same rule as sim)"
    );
}
