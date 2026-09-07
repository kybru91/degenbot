//! The per-chain Rust-owned bot state + the unified Uniswap V2/V3/V4
//! arbitrage engine, combined into one crate.
//!
//! Per ADR-003, `bot_core` (the `BotState` single-owner state, decoders,
//! reorg journal, verifier, pump) and `arb_engine` (the `ArbitrageEngine`
//! path/solve/dispatch + delivery layer) are a **mutually coupled pair** —
//! ~30 cross-references each way (`BotState` needs the solver value types
//! `IntHopState`/`IntV3TickRangeSequence` from `degenbot-solvers` and the
//! decoders; the engine needs `BotState`/`V3PoolState`/`TickInfo`/
//! `PoolStateSubscriber` from `bot_core`). ADR-003 explicitly refuses to
//! extract a `LiquidityMap` generic against this sample-of-one, so the two
//! live in one crate here rather than behind an artificial shared-trait
//! seam.
//!
//! This fusion is **tracked debt** (ADR-018): the solve surface is not
//! reachable standalone (a `cargo add degenbot` consumer wanting only the
//! V2/V3/V4 solve math must take this crate + `degenbot-rpc` +
//! `degenbot-db` + `tokio` + `rayon` + `dashmap`). The extraction trigger
//! is a **second engine family** joining (e.g. an `AaveLiquidationEngine`
//! or a split `SolidlyEngine`); until then, the cross-references are the
//! cost of one engine family and one state owner co-evolving.
//!
//! # `PyO3` boundary
//!
//! The pure core (this crate's default features) has **no `pyo3` dependency**.
//! The `#[pyclass]`/`#[pyfunction]` bindings (`PyBot`, `PyLiquidityPool`,
//! `PyErc20Token`, `PyDexIdentity`, `PyArbitrageEngine`, the
//! `Verification*Error`/`*RejectedError` exception types) live in the root
//! `degenbot_rs` cdylib's `py_bot` / `py_liquidity_pool` / `py_erc20_token` /
//! `py_dex_identity` / `py_binding` modules — they need `conversion::alloy` /
//! `conversion::cache` (binding-layer concerns). They reach the pure core through
//! `degenbot_bot::{arb_engine, bot_core}`.
//!
//! # Modules
//!
//! - [`bot_core`] — `BotState`, decoders, reorg journal, liquidity verifier,
//!   block pump, log/solve/reorg coordinators, V2/V3/V4 state.
//! - [`arb_engine`] — the unified multi-DEX `ArbitrageEngine`: per-block
//!   lifecycle, path registry, solve dispatch, inline simulation, delivery.
//!
//! The stateless solver math itself (Möbius composition, V3/V4 integer
//! tick-range solving, the QuantAMM Balancer basket solver) lives in the
//! `degenbot-solvers` crate and is imported via `::degenbot_solvers` —
//! never re-exported from here.

pub mod bot_core;

/// Default-build stub for the instruments module: same call surface, always
/// `None`. Observation sites stay ungated (`if let Some(p) = pipeline()`) so
/// the hot path reads identically in both builds — the compiler drops the
/// branch, and default builds compile zero metrics code.
#[cfg(not(feature = "otel"))]
pub mod instruments {
    /// Inert twin of the real instrument set; never constructed.
    #[derive(Debug)]
    pub struct PipelineInstruments;

    impl PipelineInstruments {
        /// no-op
        pub fn observe_header_to_solved(&self, _secs: f64) {}
        /// no-op
        pub fn observe_header_to_first_log(&self, _secs: f64) {}
        /// no-op
        pub fn observe_log_burst(&self, _secs: f64) {}
        /// no-op
        pub fn observe_settle_wait(&self, _secs: f64) {}
        /// no-op
        pub fn observe_drain_queue_wait(&self, _secs: f64) {}
        /// no-op
        pub fn observe_log_decode(&self, _secs: f64) {}
        /// no-op
        pub fn observe_state_apply(&self, _secs: f64) {}
        /// no-op
        pub fn count_block(&self) {}
        /// no-op
        pub fn count_log_received(&self) {}
        /// no-op
        pub fn count_log_applied(&self) {}
        /// no-op
        pub fn count_log_apply_missed(&self) {}
        /// no-op (WAJEQP T-R1)
        pub fn count_reorg_window(&self) {}
        /// no-op (WAJEQP T-R1)
        pub fn count_reorg_unwound_pool(&self) {}
        /// no-op (WAJEQP T-R1)
        pub fn observe_reorg_depth(&self, _blocks: u64) {}
        /// no-op (WAJEQP T-R1)
        pub fn count_reorg_recovery_dropped(&self) {}
        /// no-op
        pub fn count_ws_log_seen(&self) {}
        /// no-op
        pub fn count_log_decoded(&self) {}
        /// no-op
        pub fn count_log_undecoded(&self) {}
        /// no-op
        pub fn count_solver_verify_block(&self) {}
        /// no-op
        pub fn count_backfill(&self) {}
        /// no-op
        pub fn observe_cgroup_throttled(&self, _events_delta: u64, _usecs_delta: u64) {}
        /// no-op (K4ETHF T2)
        pub fn observe_state_lock_wait(&self, _site: &str, _mode: &str, _secs: f64) {}
        /// no-op (K4ETHF T2)
        pub fn observe_state_lock_hold(&self, _site: &str, _mode: &str, _secs: f64) {}
        /// no-op (ADR-040)
        pub fn set_quarantined_pools(&self, _count: usize) {}
        /// no-op (ADR-040)
        pub fn count_quarantine_event(&self, _cause: &str, _scope: &str) {}
        /// no-op (ADR-040)
        pub fn count_sim_error_reason(&self, _reason: &str) {}
        /// no-op (FRKBGP)
        pub fn set_process_rss_bytes(&self, _bytes: u64) {}
        /// no-op
        pub fn set_drain_queue_depth(&self, _depth: u64) {}
        /// no-op
        pub fn set_state_head_lag(&self, _head_minus_clock: i64) {}
        /// no-op
        pub fn set_seconds_since_header(&self, _secs: f64) {}
        /// no-op
        pub fn set_seconds_since_apply(&self, _secs: f64) {}
        /// no-op
        pub fn observe_mutex_hold_duration(&self, _secs: f64) {}
        /// no-op
        pub fn observe_solve_duration(&self, _secs: f64) {}
        /// no-op
        pub fn observe_per_path_solve_duration(&self, _secs: f64) {}
        /// no-op
        pub fn observe_per_path_gate_duration(&self, _secs: f64) {}
        /// no-op
        pub fn count_solves_executed(&self) {}
        /// no-op
        pub fn set_registered_paths(&self, _count: u64) {}
        /// no-op
        pub fn count_candidates_found(&self, _n: u64) {}
        /// no-op
        pub fn observe_simulate_duration(&self, _secs: f64) {}
        /// no-op
        pub fn count_simulate_verdict(&self, _verdict: &str) {}
        /// no-op
        pub fn observe_dispatch_profits(&self, _gross_wei: f64, _net_wei: f64) {}
        /// no-op
        pub fn observe_dispatch_gas(&self, _gas: u64) {}
        /// no-op
        pub fn count_submit_outcome(&self, _outcome: &str) {}
        /// no-op
        pub fn observe_submit_latency(&self, _secs: f64) {}
        /// no-op
        pub fn add_profit_realized(&self, _wei: f64) {}
        /// no-op
        pub fn add_profit_missed(&self, _wei: f64) {}
        /// no-op
        pub fn count_monitor_outcome(&self, _outcome: &str) {}
        /// no-op
        pub fn count_clamp(&self) {}
        /// no-op
        pub fn count_error(&self, _kind: &'static str) {}
        /// no-op
        pub fn count_solver_state_check(&self) {}
        /// no-op
        pub fn set_detached_in_flight(&self, _count: u64) {}
        /// no-op
        pub fn count_detached_stale_dropped(&self) {}
        /// no-op
        pub fn count_detached_applied(&self) {}
    }

    /// Epic FRKBGP close-out: resident set bytes (drift-watch). Default
    /// builds return None — the otel twin reads /proc/self/statm.
    #[must_use]
    pub fn read_process_rss_bytes() -> Option<u64> {
        None
    }

    /// Always `None` — metrics are compiled out of this build.
    #[must_use]
    pub fn pipeline() -> Option<&'static PipelineInstruments> {
        None
    }
}
pub mod allocator_ctrl;
pub mod arb_engine;
pub mod failure_policy;
#[cfg(feature = "otel")]
pub mod instruments;
#[cfg(feature = "otel")]
pub mod metrics;
#[cfg(feature = "otel")]
pub mod otel;
pub mod profiling;
pub mod telemetry;

/// Configure the process-global rayon pool used by the engine's
/// `par_iter` solve fan-out ([`crate::arb_engine`]).
///
/// GOQWCL (incident 2026-08-21): the pool was previously implicit — rayon
/// spawns its global pool lazily on first `par_iter` with **unnamed**
/// threads, which masquerade as unrelated workers in thread dumps (during
/// the incident forensics they showed up as anonymous futex waiters and
/// were nearly misattributed to tokio). Naming them makes any future
/// dump immediately attributable.
///
/// Idempotent-ish: if a global pool already exists (another component or
/// test built it first), the request is ignored with a debug log — rayon's
/// global pool can only be configured once per process.
/// Two-runtime contract (ADR: the tokio CPU/I-O split): the ambient I/O
/// runtime (pump, websocket, dispatch, delivery) keeps the headroom cores
/// (`DEFAULT_SOLVE_HEADROOM`), so the CPU-side pools — this rayon global
/// pool, the solve-executor bins, and the sim drivers behind the
/// `sim_slots` cap — are all sized from `solve_worker_count()` and its
/// leftover, never from raw `available_parallelism`.
pub fn configure_rayon_solver_pool() {
    let workers = crate::bot_core::cpu_budget::solve_worker_count();
    let result = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("degenbot-solve-{i}"))
        .build_global();
    if let Err(err) = result {
        tracing::debug!(
            %err,
            "[rayon] global pool already configured - keeping existing pool"
        );
    }
}
