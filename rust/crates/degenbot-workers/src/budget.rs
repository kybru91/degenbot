//! `FleetBudget` — the ONE budget authority bounding the SUM (design doc
//! §5; ADR-042 §3).
//!
//! Every consumer declares a peak-CPU share and a thread/slot count; the
//! fleet refuses to boot when the declared shares exceed the quota —
//! oversubscription is a configuration bug surfaced at boot, never a runtime
//! throttle storm. Fractional-quota policy (reviewed in the ADR): detection
//! keeps `degenbot_core::cpu_budget`'s ceil for worker-existence sizing,
//! while allocation arithmetic FLOORS — integer shares sum against
//! `floor(Q)` and the fractional remainder is spendable only by I/O-dominant
//! consumers (`SimDriver` slots, ambient I/O), whose measured duty is
//! partial-core by construction. `Q` comes from [`crate::quota`].
//!
//! # Discrepancy note (recorded, not redesigned)
//!
//! The design doc's worked 8-core table lists ambient `A = 2` while the
//! rule column reads `max(1, floor((Q−H)/4))`, which evaluates to 1 at
//! `Q = 8, H = 1` (leaving `S = 4`). This implementation follows the RULE
//! literally; a deployed operator recovers the table's `A = 2, S = 3` split
//! with `DEGENBOT_IO_WORKERS=2` (the terminal override both the table and
//! this code honor). The SUM invariant holds under either assignment.
//!
//! # Pin count = the LPT bin count (P6YXA6 sizing reconciliation)
//!
//! Solver pins are STRUCTURAL, not a share multiple: one seat per LPT bin,
//! the bin count following the same policy as
//! `degenbot_core::cpu_budget`'s solve bins — `floor(Q)` minus
//! [`degenbot_core::cpu_budget::DEFAULT_SOLVE_HEADROOM`]. At the deployed
//! Q = 8 that is 6 pins, exactly the bin count every dispatch arm binds at
//! (the ad-hoc `shares x 2` pin derivation — 8 seats at Q = 8 against
//! 6 bins — retires with the hard cutover). Walk ADMISSION stays the
//! share `S`: a gated bin parks, per design doc §5.

use degenbot_config::FleetConfig;

/// Fixed reserve share `H` (Python bridge, pump, `OTel`, async GC): the fleet
/// must never starve I/O.
pub const DEFAULT_RESERVE_CPUS: u64 = 1;
/// Minimum Solver share the fleet will host.
pub const MIN_SOLVER_CPUS: u64 = 2;
/// Today's `SimSlots` cap, preserved as the `SimDriver` slot cap (design doc §5).
pub const DEFAULT_SIM_SLOT_CAP: usize = 4;
/// The registration intake station's `PoolStateUpdater` slot cap (PRG-3,
/// ADR-042 F2: the registration crawl's pool-build consumers hosted as
/// keyed deferrable units). I/O-dominant by construction (RPC-bound
/// builds), so the billing follows the `SimDriver` model exactly.
pub const DEFAULT_POOL_STATE_UPDATER_SLOTS: usize = 4;

/// Terminal, typed overrides (config/env) consumed by [`FleetBudget::derive`]
/// — a configured value wins, is logged, and participates in the same sum
/// check (design doc §5: "overrides are terminal").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetOverrides {
    /// `fleet.reserve_cpus` — reserve share `H`.
    pub reserve_cpus: Option<u64>,
    /// `runtime.io_workers` — ambient I/O runtime `A`.
    pub ambient_io_workers: Option<u64>,
    /// `fleet.solver_cpus` — Solver share `S`.
    pub solver_cpus: Option<u64>,
    /// `fleet.sim_slot_cap` — `SimDriver` slot count.
    pub sim_slot_cap: Option<usize>,
    /// `fleet.pool_state_updater_slots` — `PoolStateUpdater` slot count.
    pub pool_state_updater_slots: Option<usize>,
}

impl BudgetOverrides {
    /// The typed-config projection (env vars land in the same fields via
    /// the loader's env layer).
    #[must_use]
    pub fn from_config(cfg: &degenbot_config::BotConfig) -> Self {
        Self {
            reserve_cpus: cfg.fleet.reserve_cpus.and_then(|v| u64::try_from(v).ok()),
            ambient_io_workers: cfg.runtime.io_workers.and_then(|v| u64::try_from(v).ok()),
            solver_cpus: cfg.fleet.solver_cpus.and_then(|v| u64::try_from(v).ok()),
            sim_slot_cap: cfg.fleet.sim_slot_cap,
            pool_state_updater_slots: cfg.fleet.pool_state_updater_slots,
        }
    }
}

/// Why a budget was refused.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum BudgetError {
    /// Declared peak shares exceed the quota floor.
    #[error(
        "fleet budget oversubscribed: declared peak shares {declared} cores exceed \
         floor(quota {quota}) = {floor} — oversubscription is a configuration bug, failed at boot"
    )]
    Oversubscribed {
        /// The fractional quota (cores).
        quota: f64,
        /// `floor(quota)` the integer shares must sum to.
        floor: u64,
        /// The declared sum that exceeded it.
        declared: u64,
    },
    /// Fractional quota below the pinned-role floor (`H+A+R+M+2`).
    #[error(
        "fractional quota {quota:.2} below the pinned-role floor of {required} cores \
         (reserve H + ambient A + resolve R + merge M + the 2-core Solver minimum); \
         the fleet cannot host the pinned latency roles there"
    )]
    QuotaTooSmallForPinnedRoles {
        /// The fractional quota (cores).
        quota: f64,
        /// The minimum usable quota.
        required: u64,
    },
    /// Solver share derives below the minimum.
    #[error("fleet Solver share derives to {solver} < the {min}-core minimum")]
    TooFewSolverCpus {
        /// The derived/declared Solver share.
        solver: u64,
        /// [`MIN_SOLVER_CPUS`].
        min: u64,
    },
}

/// One consumer row of the boot allocation table (design doc §5) — declared
/// `(peak_cpus, thread_count)` per the ADR's sum-bounding contract.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConsumerShare {
    /// Consumer name for the boot log.
    pub consumer: &'static str,
    /// Declared peak CPU share (cores).
    pub peak_cpus: u64,
    /// Declared thread/slot count (may exceed the share for I/O-dominant
    /// roles; the CPU share is what the authority bounds).
    pub thread_count: usize,
    /// Sizing rule in words (mirrors the worker-census `sizing` text).
    pub sizing: &'static str,
}

/// The derived fleet allocation — every consumer's declared share, the sum
/// check, and the fractional remainder policy (design doc §5).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FleetBudget {
    /// The fractional quota `Q` this budget was derived from (cores).
    pub quota_cpus: f64,
    /// `floor(Q)` — integer shares sum against this.
    pub quota_floor: u64,
    /// Reserve `H` (Python bridge, pump, `OTel`, async GC). Fixed default 1.
    pub reserve_cpus: u64,
    /// Ambient I/O runtime `A` = `max(1, floor((Q−H)/4))`, terminal override
    /// `DEGENBOT_IO_WORKERS`.
    pub ambient_cpus: u64,
    /// Resolve `R` — fixed v1 (1 core, 12.4 ms/cycle measured).
    pub resolve_cpus: u64,
    /// Merge `M` — exactly one sidecar.
    pub merge_cpus: u64,
    /// Solver pins `S` = `floor(Q) − H − A − R − M` (or the terminal
    /// override); `>= 2` or the boot fails.
    pub solver_cpus: u64,
    /// Solver pin seats: STRUCTURAL — one per LPT bin
    /// (`floor(Q)` − `cpu_budget::DEFAULT_SOLVE_HEADROOM`; concurrent walk
    /// admission stays the share `S`).
    pub solver_pin_count: usize,
    /// `SimDriver` slots (duty-counted, spendable from the fractional
    /// remainder only), capped at today's `SimSlots` cap by default.
    pub sim_slot_cap: usize,
    /// `PoolStateUpdater` slots (duty-counted, spendable from the
    /// fractional remainder only) — the registration intake station's
    /// bounded per-role unit pool. Behind Solver precedence at dispatch.
    pub pool_state_updater_slots: usize,
    /// The fractional remainder `Q − Σ(shares)` — spendable ONLY by
    /// I/O-dominant consumers, enforced by construction: it is never
    /// included in the integer sum check.
    pub fractional_remainder: f64,
}

impl FleetBudget {
    /// Derive the table for `quota_cpus` under the terminal overrides.
    /// Fails loudly (the typed [`BudgetError`]s) on over-subscription or a
    /// quota too small for the pinned latency roles.
    ///
    /// # Errors
    /// Any of the [`BudgetError`] fail-fast conditions.
    pub fn derive(quota_cpus: f64, overrides: &BudgetOverrides) -> Result<Self, BudgetError> {
        derive_table(quota_cpus, overrides)
    }

    /// H + A + R + M + S — the declared sum the authority bounds.
    #[must_use]
    pub const fn declared_sum(&self) -> u64 {
        self.reserve_cpus
            + self.ambient_cpus
            + self.resolve_cpus
            + self.merge_cpus
            + self.solver_cpus
    }

    /// The boot allocation table (log/boot-dump consumer order).
    #[must_use]
    pub fn allocation_table(&self) -> Vec<ConsumerShare> {
        vec![
            ConsumerShare {
                consumer: "reserve",
                peak_cpus: self.reserve_cpus,
                thread_count: 0,
                sizing: "fixed; must never starve I/O",
            },
            ConsumerShare {
                consumer: "ambient_io",
                peak_cpus: self.ambient_cpus,
                thread_count: usize::try_from(self.ambient_cpus).unwrap_or(1),
                sizing: "max(1, floor((Q-H)/4)); DEGENBOT_IO_WORKERS override is terminal",
            },
            ConsumerShare {
                consumer: "resolve",
                peak_cpus: self.resolve_cpus,
                thread_count: usize::try_from(self.resolve_cpus).unwrap_or(1),
                sizing: "fixed v1 (12.4 ms/cycle measured)",
            },
            ConsumerShare {
                consumer: "merge",
                peak_cpus: self.merge_cpus,
                thread_count: usize::try_from(self.merge_cpus).unwrap_or(1),
                sizing: "exactly one sidecar",
            },
            ConsumerShare {
                consumer: "solver",
                peak_cpus: self.solver_cpus,
                thread_count: self.solver_pin_count,
                sizing: "Q - H - A - R - M (>= 2 or fail-fast); pins = one per LPT bin (floor(Q) - solve headroom)",
            },
        ]
    }

    /// Re-declare the shares under a changed quota (posture/logged event).
    /// Pure re-derivation; the HOST decides which pins change (T9) and never
    /// re-keys mid-cycle.
    ///
    /// # Errors
    /// Any of the [`BudgetError`] fail-fast conditions under the new quota.
    pub fn resize(
        &self,
        new_quota_cpus: f64,
        new_overrides: &BudgetOverrides,
    ) -> Result<Self, BudgetError> {
        Self::derive(new_quota_cpus, new_overrides)
    }

    /// Whether moving from this budget to `next` requires pin re-keying
    /// (a pin-count change) — the epoch-boundary rebalance trigger (T9).
    #[must_use]
    pub const fn pins_require_rekey(&self, next: &FleetBudget) -> bool {
        self.solver_pin_count != next.solver_pin_count
    }
}

/// Fractional quota detection seam: the typed `fleet.quota_cpus` override
/// wins (terminal, per the §5 override rule); unset detects the real cgroup
/// via [`crate::quota::fractional_cpu_budget`]. Tests inject values
/// directly into [`FleetBudget::derive`].
#[must_use]
pub fn detected_quota_cpus(cfg: &FleetConfig) -> f64 {
    cfg.quota_cpus
        .filter(|q| *q >= 1.0)
        .unwrap_or_else(crate::quota::fractional_cpu_budget)
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "quota floors are small positive values (core counts)"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "core counts are exact in f64 at any realistic quota"
)]
#[expect(
    clippy::cast_sign_loss,
    reason = "the floor is clamped to >= 1.0 before the cast"
)]
fn derive_table(quota_cpus: f64, overrides: &BudgetOverrides) -> Result<FleetBudget, BudgetError> {
    let quota_floor = quota_cpus.max(1.0).floor() as u64;

    // H — fixed default 1, terminal override.
    let reserve_cpus = overrides.reserve_cpus.unwrap_or(DEFAULT_RESERVE_CPUS);

    // A — the leftover-share rule, terminal DEGENBOT_IO_WORKERS override.
    let ambient_cpus = overrides
        .ambient_io_workers
        .unwrap_or_else(|| ((quota_floor.saturating_sub(reserve_cpus)) / 4).max(1));

    // R, M — fixed v1 consumers.
    let (resolve_cpus, merge_cpus) = (1, 1);
    let base = reserve_cpus + ambient_cpus + resolve_cpus + merge_cpus;

    if base + MIN_SOLVER_CPUS > quota_floor {
        return Err(BudgetError::QuotaTooSmallForPinnedRoles {
            quota: quota_cpus,
            required: base + MIN_SOLVER_CPUS,
        });
    }

    // S — the leftover of the floor after the fixed consumers (or the
    // terminal override, checked against the same sum).
    let solver_cpus = overrides.solver_cpus.unwrap_or(quota_floor - base);

    if base + solver_cpus > quota_floor {
        return Err(BudgetError::Oversubscribed {
            quota: quota_cpus,
            floor: quota_floor,
            declared: base + solver_cpus,
        });
    }
    if solver_cpus < MIN_SOLVER_CPUS {
        return Err(BudgetError::TooFewSolverCpus {
            solver: solver_cpus,
            min: MIN_SOLVER_CPUS,
        });
    }

    let sim_slot_cap = overrides.sim_slot_cap.unwrap_or(DEFAULT_SIM_SLOT_CAP);
    let pool_state_updater_slots = overrides
        .pool_state_updater_slots
        .unwrap_or(DEFAULT_POOL_STATE_UPDATER_SLOTS);
    let fractional_remainder = quota_cpus - (base + solver_cpus) as f64;

    Ok(FleetBudget {
        quota_cpus,
        quota_floor,
        reserve_cpus,
        ambient_cpus,
        resolve_cpus,
        merge_cpus,
        solver_cpus,
        // Pin seats are STRUCTURAL (P6YXA6 reconciliation): one per LPT
        // bin, the bin count following cpu_budget's solve-bin policy
        // (floor(Q) minus the solve headroom, floored at 1). Walk admission
        // stays the share S — a gated bin parks (design doc §5).
        solver_pin_count: usize::try_from(quota_floor)
            .unwrap_or(usize::MAX)
            .saturating_sub(degenbot_core::cpu_budget::DEFAULT_SOLVE_HEADROOM)
            .max(1),
        sim_slot_cap,
        pool_state_updater_slots,
        fractional_remainder,
    })
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;

    fn overrides() -> BudgetOverrides {
        BudgetOverrides::default()
    }

    #[test]
    fn the_8_core_quota_table_sums_exactly_against_the_floor() {
        let b = FleetBudget::derive(8.0, &overrides()).expect("8-core quota hostable");
        assert_eq!(b.quota_floor, 8);
        // SUM invariant: H + A + R + M + S = Q (design doc §5).
        assert_eq!(b.declared_sum(), b.quota_floor);
        assert_eq!(b.reserve_cpus, 1);
        assert_eq!(b.resolve_cpus, 1);
        assert_eq!(b.merge_cpus, 1);
        // The rule column: A = max(1, floor((Q-H)/4)) = 1, leaving S = 4.
        // (The doc TABLE's A=2/S=3 split is reachable via the terminal
        // DEGENBOT_IO_WORKERS override — see the module discrepancy note.)
        assert_eq!(b.ambient_cpus, 1);
        assert_eq!(b.solver_cpus, 4);
        // Pins are STRUCTURAL: one seat per LPT bin = floor(Q) - the solve
        // headroom (the same policy cpu_budget uses for the bin count) —
        // not the 2:1 parked-wait over-subscription (P6YXA6 sizing note).
        assert_eq!(b.solver_pin_count, 6);
    }

    #[test]
    fn the_worked_8_core_table_split_is_reachable_via_the_terminal_override() {
        // DEGENBOT_IO_WORKERS=2 (the current deployment) reproduces the
        // doc §5 worked table exactly: A=2, S=3, sum 8.
        let b = FleetBudget::derive(
            8.0,
            &BudgetOverrides {
                ambient_io_workers: Some(2),
                ..overrides()
            },
        )
        .expect("hostable");
        assert_eq!(b.ambient_cpus, 2);
        assert_eq!(b.solver_cpus, 3);
        assert_eq!(b.solver_pin_count, 6);
        assert_eq!(b.declared_sum(), 8);
    }

    #[test]
    fn a_quota_below_the_pinned_role_floor_fails_fast() {
        // 4.5-core quota: floor is 4 but H+A+R+M+2 = 6 > 4 — the two pinned
        // latency roles cannot be hosted there (the integer sum floors; the
        // fractional remainder is never part of the sum check).
        let err = FleetBudget::derive(4.5, &overrides()).expect_err("too small");
        assert!(matches!(
            err,
            BudgetError::QuotaTooSmallForPinnedRoles { .. }
        ));
    }

    #[test]
    fn fractional_quota_banks_the_remainder_outside_the_integer_sum() {
        // 6.5-core quota: floor 6, base (H1+A1+R1+M1) = 4, S = 2; the 0.5
        // remainder banks outside the sum check (I/O-dominant spend only).
        let b = FleetBudget::derive(6.5, &overrides()).expect("hostable");
        assert_eq!(b.quota_floor, 6);
        assert_eq!(b.declared_sum(), 6);
        assert!((b.fractional_remainder - 0.5).abs() < 1e-9);
        assert_eq!(b.solver_cpus, 2);
        assert_eq!(b.solver_pin_count, 4);
    }

    #[test]
    fn oversubscription_by_overrides_fails_at_boot() {
        let err = FleetBudget::derive(
            8.0,
            &BudgetOverrides {
                ambient_io_workers: Some(2),
                solver_cpus: Some(4),
                ..overrides()
            },
        )
        .expect_err("H1+A2+R1+M1+S4 = 9 > 8");
        assert!(matches!(err, BudgetError::Oversubscribed { .. }));
    }

    #[test]
    fn a_sub_minimum_solver_share_is_refused() {
        let err = FleetBudget::derive(
            8.0,
            &BudgetOverrides {
                solver_cpus: Some(1),
                ..overrides()
            },
        )
        .expect_err("S=1 < the 2-core minimum");
        assert!(matches!(err, BudgetError::TooFewSolverCpus { .. }));
    }

    #[test]
    fn resize_redeclares_shares_and_keeps_the_sum_invariant() {
        let big = FleetBudget::derive(8.0, &overrides()).expect("8");
        let small = big.resize(6.5, &overrides()).expect("6.5 hostable");
        assert_eq!(small.declared_sum(), small.quota_floor);
        // Pin-count change flags the T9 rebalance (never a mid-cycle re-key).
        assert!(big.pins_require_rekey(&small));
        // Idempotent re-declaration under an unchanged quota.
        let same = big.resize(8.0, &overrides()).expect("8 again");
        assert!(!big.pins_require_rekey(&same));
    }

    #[test]
    fn the_registration_intake_station_is_duty_counted_like_sim() {
        // PoolStateUpdater slots default to the ADR-042 F2 station size and
        // stay OUTSIDE the declared integer sum (I/O-dominant billing —
        // exactly the SimDriver model).
        let b = FleetBudget::derive(8.0, &overrides()).expect("hostable");
        assert_eq!(b.pool_state_updater_slots, DEFAULT_POOL_STATE_UPDATER_SLOTS);
        assert_eq!(b.declared_sum(), b.quota_floor);
        // The terminal override wins (same terminal rule as the rest).
        let b2 = FleetBudget::derive(
            8.0,
            &BudgetOverrides {
                pool_state_updater_slots: Some(6),
                ..overrides()
            },
        )
        .expect("hostable");
        assert_eq!(b2.pool_state_updater_slots, 6);
        assert_eq!(b2.declared_sum(), b2.quota_floor);
    }

    #[test]
    fn the_pool_state_updater_override_projects_from_the_typed_config() {
        let mut cfg = degenbot_config::BotConfig::default();
        cfg.fleet.pool_state_updater_slots = Some(6);
        let o = BudgetOverrides::from_config(&cfg);
        assert_eq!(o.pool_state_updater_slots, Some(6));
        let b = FleetBudget::derive(8.0, &o).expect("hostable");
        assert_eq!(b.pool_state_updater_slots, 6);
    }

    #[test]
    fn allocation_table_covers_every_consumer_row() {
        let b = FleetBudget::derive(8.0, &overrides()).expect("8");
        let consumers: Vec<&str> = b
            .allocation_table()
            .into_iter()
            .map(|s| s.consumer)
            .collect();
        for want in ["reserve", "ambient_io", "resolve", "merge", "solver"] {
            assert!(consumers.contains(&want), "missing {want}");
        }
    }

    #[test]
    fn typed_config_projection_carries_the_override_fields() {
        let mut cfg = degenbot_config::BotConfig::default();
        cfg.fleet.solver_cpus = Some(3);
        cfg.runtime.io_workers = Some(2);
        cfg.fleet.sim_slot_cap = Some(6);
        let o = BudgetOverrides::from_config(&cfg);
        assert_eq!(o.solver_cpus, Some(3));
        assert_eq!(o.ambient_io_workers, Some(2));
        assert_eq!(o.sim_slot_cap, Some(6));
        let b = FleetBudget::derive(8.0, &o).expect("hostable");
        assert_eq!(b.solver_cpus, 3);
        assert_eq!(b.sim_slot_cap, 6);
    }
}
