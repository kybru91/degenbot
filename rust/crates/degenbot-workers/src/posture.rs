//! The `Nominal ⇄ Cordoned` posture FSM (design doc §6) — the first
//! consumer of `degenbot.cgroup.throttled` (`cpu_budget::cgroup_throttle_delta`).
//!
//! Thresholds are typed, runtime-tunable config keys (the sign-off
//! amendment 2026-09-09: enter triggers, exit window, and cordon effects
//! calibrated from soak data via the operator channel — never share
//! arithmetic, which is [`crate::budget`]'s authority).
//!
//! Entering/exiting is LOUD: a transition fires a structured log line plus
//! the posture counters (never a silent degrade). Time is passed in as
//! monotonic milliseconds so the FSM is deterministic under test; callers
//! feed `Instant::now()` deltas from their throttle poller.

use std::collections::VecDeque;

use degenbot_config::FleetConfig;

use crate::role::CordonClass;

/// Process-level fleet posture. NOT a slot state (design doc §3.2): it
/// gates lease transitions, it never sheds a running unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FleetPosture {
    /// Normal operation: every dispatchable role leases freely.
    Nominal,
    /// Under cgroup throttling: no new leases for cordon-deferrable roles,
    /// sim intake floored; in-flight units COMPLETE (T7/T8), pins are never
    /// shed, the merge pin and ambient I/O are never cordoned.
    Cordoned,
}

/// Why the posture entered cordon (exported as the transition's cause).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EnterReason {
    /// ≥ `enter_events` throttle events within the rolling enter window.
    EventBurst {
        /// The events observed in the window.
        events: u64,
    },
    /// Throttled-time duty exceeded `duty_percent` over the duty window.
    DutySpike {
        /// Measured duty percent.
        duty_percent: f64,
    },
}

/// The posture change a [`PostureStateMachine::observe`] tick produced.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PostureChange {
    /// No transition (the FSM does not re-enter while already cordoned).
    Held,
    /// Nominal → Cordoned (loud: log at warn + counter).
    Entered(EnterReason),
    /// Cordoned → Nominal after the clean-window hysteresis (log at info).
    Exited,
}

/// Typed thresholds (Q5 amendment) — [`PosturePolicy::from_config`] is the
/// boot source; the operator channel re-derives from the same schema keys.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PosturePolicy {
    /// Enter trigger (a): >= this many throttle events in the enter window.
    pub enter_events: usize,
    /// Enter window (a): the rolling burst window.
    pub enter_window_ms: u64,
    /// Enter trigger (b): throttled-time duty percent over the duty window
    /// (`2.0` = >2%).
    pub duty_percent: f64,
    /// Enter trigger (b) window.
    pub duty_window_ms: u64,
    /// Exit: this much continuous clean time required after the last dirty
    /// sample (hysteresis prevents flapping; §6: 10 s).
    pub exit_clean_ms: u64,
    /// Cordon effect (b): sim intake cap while cordoned; `None` = half the
    /// slot cap (in-flight sims are never cancelled).
    pub sim_intake_floor_override: Option<usize>,
}

impl PosturePolicy {
    /// The typed-config projection — every threshold is a schema key.
    #[must_use]
    pub fn from_config(cfg: &FleetConfig) -> Self {
        Self {
            enter_events: cfg.cordon_enter_events,
            enter_window_ms: cfg.cordon_enter_window_ms,
            duty_percent: cfg.cordon_duty_percent,
            duty_window_ms: cfg.cordon_duty_window_ms,
            exit_clean_ms: cfg.cordon_exit_clean_ms,
            sim_intake_floor_override: cfg.cordon_sim_intake_floor,
        }
    }

    /// The cordon sim-intake cap: override or half the slot cap, floored
    /// at 1, never above the cap (§6 effect (b)).
    #[must_use]
    pub fn sim_intake_cap(&self, slot_cap: usize) -> usize {
        self.sim_intake_floor_override
            .unwrap_or(slot_cap / 2)
            .min(slot_cap)
            .max(1)
    }
}

/// One throttle-poll delta: `cgroup_throttle_delta()`'s counters plus the
/// elapsed wall time of the poll interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThrottleSample {
    /// `nr_throttled` delta since the last sample.
    pub events: u64,
    /// `throttled_usec` delta since the last sample.
    pub throttled_usec: u64,
    /// Elapsed wall time (µs) of the poll interval backing the deltas.
    pub elapsed_usec: u64,
}

impl ThrottleSample {
    /// A clean sample: no throttle events, no throttled time.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.events == 0 && self.throttled_usec == 0
    }
}

/// Loud-transition counters (exported with the posture metrics so
/// thresholds are tuned against measurements, §6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PostureCounters {
    /// Cordons entered.
    pub entered: u64,
    /// Cordons exited.
    pub exited: u64,
    /// Lease grants denied while cordoned (deferrable intake held + sim
    /// intake suppression above the floor).
    pub intake_suppressed: u64,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    now_ms: u64,
    sample: ThrottleSample,
}

/// The posture state machine. Deterministic: time is monotonic ms fed by
/// the caller.
#[derive(Debug)]
pub struct PostureStateMachine {
    state: FleetPosture,
    policy: PosturePolicy,
    samples: VecDeque<Sample>,
    last_unclean_ms: Option<u64>,
    counters: PostureCounters,
}

impl PostureStateMachine {
    /// A fresh machine in [`FleetPosture::Nominal`].
    #[must_use]
    pub fn new(policy: PosturePolicy) -> Self {
        Self {
            state: FleetPosture::Nominal,
            policy,
            samples: VecDeque::new(),
            last_unclean_ms: None,
            counters: PostureCounters::default(),
        }
    }

    /// Current posture.
    #[must_use]
    pub const fn state(&self) -> FleetPosture {
        self.state
    }

    /// Loud-transition counters (metrics export surface).
    #[must_use]
    pub const fn counters(&self) -> &PostureCounters {
        &self.counters
    }

    /// The active policy (operator-channel re-tune reads it through here).
    #[must_use]
    pub const fn policy(&self) -> &PosturePolicy {
        &self.policy
    }

    /// Feed one throttle-poll delta at `now_ms`. Returns whether the
    /// posture transitioned (and why) — the caller surfaces that loudly.
    pub fn observe(&mut self, now_ms: u64, sample: ThrottleSample) -> PostureChange {
        self.samples.push_back(Sample { now_ms, sample });
        self.prune(now_ms);
        if !sample.is_clean() {
            self.last_unclean_ms = Some(now_ms);
        }

        match self.state {
            FleetPosture::Nominal => self.maybe_enter(now_ms),
            FleetPosture::Cordoned => self.maybe_exit(now_ms),
        }
    }

    /// A lease grant was denied because of the posture (counter only — the
    /// dispatcher supplies the typed error).
    pub const fn note_intake_suppressed(&mut self) {
        self.counters.intake_suppressed += 1;
    }

    /// Whether the posture admits new lease intake for `class` right now
    /// (§6: `Never` and `SimPool` lease freely — sim is only intake-FLOORED;
    /// `Deferrable` is held while cordoned).
    #[must_use]
    pub const fn admits_lease(&self, class: CordonClass) -> bool {
        !matches!(
            (self.state, class),
            (FleetPosture::Cordoned, CordonClass::Deferrable)
        )
    }

    /// The sim intake cap in the current posture (§6 effect (b)).
    #[must_use]
    pub fn sim_intake_cap(&self, slot_cap: usize) -> usize {
        match self.state {
            FleetPosture::Nominal => slot_cap,
            FleetPosture::Cordoned => self.policy.sim_intake_cap(slot_cap),
        }
    }

    fn prune(&mut self, now_ms: u64) {
        let window = self.policy.duty_window_ms.max(self.policy.enter_window_ms);
        while let Some(front) = self.samples.front() {
            if now_ms.saturating_sub(front.now_ms) > window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn maybe_enter(&mut self, now_ms: u64) -> PostureChange {
        // Trigger (a): event burst inside the enter window.
        let burst: u64 = self
            .samples
            .iter()
            .filter(|s| now_ms.saturating_sub(s.now_ms) <= self.policy.enter_window_ms)
            .map(|s| s.sample.events)
            .sum();
        if burst >= u64::try_from(self.policy.enter_events.max(1)).unwrap_or(1) {
            return self.enter(EnterReason::EventBurst { events: burst });
        }
        // Trigger (b): duty percent over the trailing duty window.
        let (events, throttled_usec, elapsed_usec) = self.duty_window_totals();
        let _ = events;
        if elapsed_usec > 0 {
            #[expect(
                clippy::cast_precision_loss,
                reason = "duty percent is an f64 metric by definition (µs/µs ratio)"
            )]
            let duty_percent = throttled_usec as f64 / elapsed_usec as f64 * 100.0;
            if duty_percent > self.policy.duty_percent {
                return self.enter(EnterReason::DutySpike { duty_percent });
            }
        }
        PostureChange::Held
    }

    fn duty_window_totals(&self) -> (u64, u64, u64) {
        self.samples
            .iter()
            .fold((0, 0, 0), |(ev, th, el), Sample { sample, .. }| {
                (
                    ev + sample.events,
                    th + sample.throttled_usec,
                    el + sample.elapsed_usec,
                )
            })
    }

    fn enter(&mut self, reason: EnterReason) -> PostureChange {
        self.state = FleetPosture::Cordoned;
        self.counters.entered += 1;
        tracing::warn!(
            target: "degenbot::fleet",
            reason = ?reason,
            entered = self.counters.entered,
            "[fleet-posture] cordon ENTER — deferrable intake held, sim intake floored; in-flight units complete"
        );
        PostureChange::Entered(reason)
    }

    fn maybe_exit(&mut self, now_ms: u64) -> PostureChange {
        // Exit: `exit_clean_ms` of clean time since the last dirty sample
        // (hysteresis; §6: 10 s of clean windows).
        let dirty_recently = self
            .last_unclean_ms
            .is_some_and(|last| now_ms.saturating_sub(last) < self.policy.exit_clean_ms);
        if dirty_recently {
            return PostureChange::Held;
        }
        self.state = FleetPosture::Nominal;
        self.counters.exited += 1;
        tracing::info!(
            target: "degenbot::fleet",
            exited = self.counters.exited,
            clean_ms = self.policy.exit_clean_ms,
            "[fleet-posture] cordon EXIT after clean-window hysteresis"
        );
        PostureChange::Exited
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn sm() -> PostureStateMachine {
        PostureStateMachine::new(policy())
    }

    fn sample(events: u64, throttled_usec: u64, elapsed_usec: u64) -> ThrottleSample {
        ThrottleSample {
            events,
            throttled_usec,
            elapsed_usec,
        }
    }

    #[test]
    fn nominal_is_the_boot_state() {
        assert_eq!(sm().state(), FleetPosture::Nominal);
    }

    #[test]
    fn enters_on_event_burst_within_the_window() {
        let mut m = sm();
        // One lone event inside the window: below the threshold.
        assert_eq!(m.observe(100, sample(1, 0, 1_000_000)), PostureChange::Held);
        // The second event inside the same trailing 1 s window -> enter,
        // loudly typed.
        let change = m.observe(600, sample(1, 0, 500_000));
        assert_eq!(
            change,
            PostureChange::Entered(EnterReason::EventBurst { events: 2 })
        );
        assert_eq!(m.state(), FleetPosture::Cordoned);
        assert_eq!(m.counters().entered, 1);
        // Already cordoned: further dirty samples do not re-enter.
        assert!(matches!(
            m.observe(700, sample(1, 0, 100_000)),
            PostureChange::Held
        ));
        assert_eq!(m.counters().entered, 1);
    }

    #[test]
    fn burst_outside_the_enter_window_does_not_cordon() {
        let mut m = sm();
        m.observe(0, sample(1, 0, 1_000_000));
        // The 1 s window expired; two lone events > 1 s apart never burst.
        m.observe(3_000, sample(1, 0, 2_000_000));
        m.observe(3_100, sample(0, 0, 100_000));
        assert_eq!(m.state(), FleetPosture::Nominal);
    }

    #[test]
    fn enters_on_duty_spike_over_the_duty_window() {
        let mut m = sm();
        // A 1 s poll interval whose throttled time was 50% — measured over
        // the trailing 5 s window that is 50% duty, far above the 2%
        // threshold: the duty trigger fires on the first observation.
        let change = m.observe(1_000, sample(0, 500_000, 1_000_000));
        assert!(matches!(
            change,
            PostureChange::Entered(EnterReason::DutySpike { .. })
        ));
        assert_eq!(m.state(), FleetPosture::Cordoned);
        assert_eq!(m.counters().entered, 1);
    }

    #[test]
    fn a_rising_duty_only_crosses_once_the_trailing_total_exceeds_the_threshold() {
        let mut m = sm();
        // Sub-threshold ticks (0.05% each) hold the posture...
        for t in 1..5_u64 {
            assert_eq!(
                m.observe(t * 1_000, sample(0, 500, 1_000_000)),
                PostureChange::Held
            );
        }
        // ...until one more dirty tick crosses the window total above 2%.
        let change = m.observe(5_000, sample(0, 5_000_000, 1_000_000));
        assert!(matches!(
            change,
            PostureChange::Entered(EnterReason::DutySpike { .. })
        ));
    }

    #[test]
    fn sub_threshold_duty_never_cordons() {
        let mut m = sm();
        // 1% duty (design doc: steady state 0.18%) for ten windows.
        for t in 0..10_u64 {
            m.observe((t + 1) * 1_000, sample(0, 10_000, 1_000_000));
        }
        assert_eq!(m.state(), FleetPosture::Nominal);
    }

    #[test]
    fn exits_only_after_the_full_clean_hysteresis() {
        let mut m = sm();
        m.observe(100, sample(2, 0, 100_000)); // burst enter
        assert_eq!(m.state(), FleetPosture::Cordoned);
        // Clean time below the hysteresis keeps the cordon.
        let mut t = 200;
        while t < 10_000 {
            assert_eq!(m.observe(t, sample(0, 0, 1_000_000)), PostureChange::Held);
            t += 1_000;
        }
        assert_eq!(m.state(), FleetPosture::Cordoned);
        // Past 10 s clean: exit.
        let change = m.observe(10_200, sample(0, 0, 100_000));
        assert_eq!(change, PostureChange::Exited);
        assert_eq!(m.state(), FleetPosture::Nominal);
        assert_eq!(m.counters().exited, 1);
    }

    #[test]
    fn a_dirty_window_restarts_the_clean_clock() {
        let mut m = sm();
        m.observe(0, sample(2, 0, 100_000));
        let mut t = 1_000;
        while t < 9_000 {
            m.observe(t, sample(0, 0, 1_000_000));
            t += 1_000;
        }
        // A lone dirty sample resets the clean-window clock.
        m.observe(9_000, sample(1, 0, 100_000));
        assert_eq!(m.state(), FleetPosture::Cordoned);
        let mut t = 10_000;
        while t < 18_000 {
            m.observe(t, sample(0, 0, 1_000_000));
            t += 1_000;
        }
        assert_eq!(m.state(), FleetPosture::Cordoned, "only 9 s clean");
        m.observe(19_100, sample(0, 0, 100_000));
        assert_eq!(
            m.state(),
            FleetPosture::Nominal,
            "10 s clean since the reset"
        );
    }

    #[test]
    fn cordon_effects_match_the_sign_off_table() {
        let mut m = sm();
        // Nominal: full sim cap; deferrable admitted.
        assert_eq!(m.sim_intake_cap(4), 4);
        assert!(m.admits_lease(CordonClass::Never));
        assert!(m.admits_lease(CordonClass::SimPool));
        assert!(m.admits_lease(CordonClass::Deferrable));

        m.observe(0, sample(2, 0, 100_000));
        assert_eq!(m.state(), FleetPosture::Cordoned);
        // Cordon: deferrable held, sim floored at half the cap, Never free.
        assert!(!m.admits_lease(CordonClass::Deferrable));
        assert!(m.admits_lease(CordonClass::SimPool));
        assert!(m.admits_lease(CordonClass::Never));
        assert_eq!(m.sim_intake_cap(4), 2, "floor = half the slot cap");
        assert_eq!(m.sim_intake_cap(1), 1, "floored at one");
    }

    #[test]
    fn override_intake_floor_wins_and_caps_at_the_slot_cap() {
        let p = PosturePolicy {
            sim_intake_floor_override: Some(7),
            ..policy()
        };
        assert_eq!(p.sim_intake_cap(4), 4, "nothing above the slot cap");
        let p = PosturePolicy {
            sim_intake_floor_override: Some(0),
            ..policy()
        };
        assert_eq!(p.sim_intake_cap(4), 1, "floored at one");
    }

    #[test]
    fn suppressed_intake_is_counted_for_the_tuning_loop() {
        let mut m = sm();
        m.note_intake_suppressed();
        m.note_intake_suppressed();
        assert_eq!(m.counters().intake_suppressed, 2);
    }

    #[test]
    fn typed_config_projects_all_the_q5_amendment_thresholds() {
        let cfg = degenbot_config::BotConfig::default();
        let p = PosturePolicy::from_config(&cfg.fleet);
        // Defaults mirror design doc §6: 2 events / 1 s, >2% / 5 s, 10 s.
        assert_eq!(p.enter_events, 2);
        assert_eq!(p.enter_window_ms, 1_000);
        assert!((p.duty_percent - 2.0).abs() < 1e-9, "duty default is 2%");
        assert_eq!(p.duty_window_ms, 5_000);
        assert_eq!(p.exit_clean_ms, 10_000);
        assert_eq!(p.sim_intake_floor_override, None);
    }
}
