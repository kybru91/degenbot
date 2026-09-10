//! Path resolution, solver dispatch, and rebuild logic.

use alloy::primitives::{I256, U256};
use std::sync::PoisonError;

use ::degenbot_pools::v3_state::{v3_simulate_swap, V3PoolState};
use ::degenbot_pools::v4_state::v4_simulate_swap;

use super::{ArbitrageEngine, BlockMetadata, HashMap, HashSet};

// There is deliberately NO solve-time "staleness" pre-gate here (ergo YXHHKR,
// resolved QNFYR5). The former TQ43TU `hop_is_too_stale` gate deferred a whole
// path on any co-hop whose price-clock `update_block` trailed > 10 blocks — but
// `update_block` is a last-activity clock, so a pool that swapped once and then
// went quiet (state byte-identical to on-chain) was falsely deferred: QNFYR5's
// instrumented live run showed 3,550 such defers (V2/V3/V4, gap 11-16, 0 genuine)
// with a healthy engine solve→sim path underneath. A static age check cannot
// distinguish "quiet but current" from "genuinely moved but only moderately
// behind" (AV42C7 — the zero-tolerance retread was already REVERTED for the same
// over-deferral). The accurate discriminator requires a fresh on-chain read, which
// the ADR-021 publish-edge verifier used to perform at
// publish (per-hop anchor diff + process abort). Task 2UVG3E (epic MROOY7)
// retired that in-process chain-vs-solver-state gate — the stage-separated
// data plane makes its desync class unrepresentable — and keeps ONLY the
// upstream RPC-disagreement verification (CompletenessDecision::Verify →
// assert_ws_block_complete) at the Published edge. No age heuristic replaced
// it: solve-on-quiet is correct by construction under the stage-separated
// data plane; stale results are dropped by the Q1a merge gate, never applied.

use crate::arb_engine::fleet_solve_executor::{
    run_solve_lane, LaneOutcome, SolveLane, SOLVE_BIN_KEY_BASE,
};
use crate::arb_engine::inline_sim::{PendingSim, SimPoll, SimulatedPathResult};
use crate::bot_core::resolve::resolve_hops;
use crate::bot_core::BotState;
use ::degenbot_solvers::mixed::{
    HopType, MixedPath, MixedPoolRef, ResolvedHop, ResolvedMixedPath, SolvePathResult,
};
use degenbot_workers::dispatcher::SeatSurvivesPolicy;

/// How many slowest-path entries the solve-cycle completion event names
/// (D63GSE intra-solve visibility).
const SLOWEST_PATHS_K: usize = 5;

/// Q3 dense one-shot alert flag — the CONSUMER side of the moved alert: the
/// walk reports `WalkStats::max_dense_words`; this logs once per process.
static WALK_DENSE_ALERTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// One solved path's arm tuple: `(path_id, result, worker clamp twins, the
/// worker-resolved inline-sim payload)`. The solve arms hand these to the
/// merge (`merge_one_result`); `None` payload = stance off / hook silence.
pub(crate) type SolveArmOutcome = (u64, SolvePathResult, u64, Option<SimulatedPathResult>);

// ---------------------------------------------------------------------------
// RAYPAR T3: LPT-pre-balanced scoped-thread partition
// ---------------------------------------------------------------------------

/// The ONE solve-bin sizing seam (P6YXA6): fleet-hosted cycles bin at the
/// fleet's structural Solver seat count (pins == bins, so every bin owns a
/// warm keyed seat); every other arm bins at the machine-derived solve
/// worker count. One funnel so the arms can never re-derive the count and
/// drift apart (a hermetic fleet under machine-derived bins aborts at the
/// T2 grant — the host-only `just test-rust` failure).
pub(crate) fn solve_bin_count(fleet_hosted: bool) -> usize {
    if fleet_hosted {
        crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor().bin_count()
    } else {
        degenbot_core::cpu_budget::solve_worker_count()
    }
}

#[expect(clippy::doc_markdown)]
/// RAYPAR T3: LPT (longest-processing-time) bin-packing. Sorts items by
/// descending cost and greedily assigns each to the least-loaded bin. Returns
/// indices into the original items slice, one Vec per bin.
///
/// The RAYPAR lab (docs/rayon-parallelism-lab.md) showed rayon work-stealing
/// par_iter achieves only 4.91/8 efficiency on the heavy-CL capture corpus
/// because the workload has extreme cost skew (top 8 of 80 paths = 60% of CPU).
/// LPT pre-balances so no thread gets stuck with an unsplittable giant while
/// others idle — achieving 7.80/8 (35% wall reduction). Same solver, same
/// threads, same memory bandwidth.
pub(crate) fn lpt_partition(
    n_items: usize,
    n_bins: usize,
    cost: impl Fn(usize) -> usize,
) -> Vec<Vec<usize>> {
    if n_bins == 0 {
        return Vec::new();
    }
    if n_items == 0 {
        return vec![Vec::new(); n_bins];
    }
    let mut idx: Vec<usize> = (0..n_items).collect();
    idx.sort_unstable_by_key(|&i| std::cmp::Reverse(cost(i)));
    let mut loads = vec![0usize; n_bins];
    let mut bins: Vec<Vec<usize>> = vec![Vec::new(); n_bins];
    for i in idx {
        let mi = loads
            .iter()
            .enumerate()
            .min_by_key(|&(_, l)| l)
            .map_or(0, |(i, _)| i);
        bins[mi].push(i);
        loads[mi] += cost(i);
    }
    bins
}

/// The RUNTIME lane-capability decision (LW-T7, Seam F): the cycle either
/// runs at full structural width or takes the NAMED narrower fallback
/// (reth `state_root_task_timeout => sequential` lesson: the fallback is a
/// NAMED-AND-LOGGED decision, never a silent narrower bin mid-drain) — the
/// runtime twin of LW-T4's boot-time capacity floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CordonFallbackDecision {
    /// Structural seats cover the intended bins.
    FullCapacity,
    /// The hosting capability dropped (T9 resize under cordon): the cycle
    /// runs `running` bins (< `intended`) this block — logged at INFO.
    Narrower {
        /// The bins the workload intended.
        intended: usize,
        /// The bins the current capability hosts.
        running: usize,
    },
}

/// One cycle's planned bin fan-out (LW-T7, Seam F).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeatPlan {
    /// The bin count the cycle fans out over.
    pub bins: usize,
    /// The typed fallback decision this plan took.
    pub decision: CordonFallbackDecision,
}

/// Plan this cycle's bin fan-out: a capability narrower than the intended
/// fan-out takes the NAMED narrower fallback (typed + logged at INFO).
#[must_use]
pub(crate) fn plan_bins(intended_bins: usize, structural_seats: usize) -> SeatPlan {
    if structural_seats < intended_bins {
        tracing::info!(
            target: "degenbot::solver",
            intended = intended_bins,
            running = structural_seats,
            "[seat-plan] capability drop under cordon — running the NAMED \
             narrower fallback (LW-T7; the serial arm remains a downstream decision)"
        );
        SeatPlan {
            bins: structural_seats,
            decision: CordonFallbackDecision::Narrower {
                intended: intended_bins,
                running: structural_seats,
            },
        }
    } else {
        SeatPlan {
            bins: intended_bins,
            decision: CordonFallbackDecision::FullCapacity,
        }
    }
}

#[expect(clippy::doc_markdown)]
/// Resolve-time cost proxy for LPT binning: the total number of word-boundary
/// prices across all CL hops. Correlates with walk combinatorics without
/// requiring a solve, so it is available at to_solve collection time.
pub(crate) fn path_cost_proxy(resolved: &ResolvedMixedPath) -> usize {
    resolved
        .hops
        .iter()
        .filter_map(|h| h.as_int_sequence())
        .flat_map(|seq| seq.ranges.iter())
        .map(|r| r.word_boundary_prices.len())
        .sum()
}

/// LPT cost used at binning: max(structural word-boundary proxy, previous
/// block's measured walk sims + measured gate µs). The measured counts
/// predict the current block's combinatorics better for stable pool shapes;
/// the proxy floors it for freshly dirty pools. (loop-12 BY7BLS KUKHMX;
/// loop-18 adds the gate-µs term — gate-heavy paths carry sims≈0 and were
/// bin-packed cheap while dominating wall time.) The sims and gate terms add
/// (same µs-scale: a walk sim ≈0.7-0.8µs, so `sims` ≈ walk µs).
fn sims_aware_cost(proxy: usize, last_sims: Option<u64>, last_gate_us: Option<u64>) -> usize {
    let measured = match last_sims {
        Some(v) => usize::try_from(v).unwrap_or(usize::MAX),
        None => 0,
    };
    let measured_gate = match last_gate_us {
        Some(v) => usize::try_from(v).unwrap_or(usize::MAX),
        None => 0,
    };
    proxy.max(measured.saturating_add(measured_gate))
}

/// 7LV6VN T2: chunked parallel resolve of the affected paths (sharded hop
/// cache preserves cross-path hit reuse). Default ON; set
/// `DEGENBOT_SOLVE_RESOLVE_PAR=0` for the serial A/B fallback.
const RESOLVE_CHUNK: usize = 256;
const RESOLVE_PAR_MIN: usize = 512;

struct ResolveChunkOut {
    resolved: Vec<(u64, std::sync::Arc<ResolvedMixedPath>)>,
    status: Vec<(u64, Vec<crate::bot_core::resolve::HopDeficit>)>,
    snapshots: Vec<(u64, Vec<u64>)>,
    same_state: u64,
    projections: u64,
    invalid_reasons: HashMap<String, u64>,
    deferred: Vec<u64>,
}

fn resolve_parallel_enabled() -> bool {
    RESOLVE_PAR_STANCE.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) static RESOLVE_PAR_STANCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Pre-solve profitability floor for the profit-envelope gate (SU7MAE).
/// Precedence: `DEGENBOT_MIN_PROFIT_WEI` (decimal wei) > default 0. Default 0
/// skips only paths whose rigorous upper bound proves zero-or-negative profit.
/// The full fee-aware derivation (`gas × base_fee_next + priority_fee`, the
/// same shape as degenbot-execution's assess rule) replaces this once live
/// numbers justify it — the solver API needs no change for that.
/// (T4: parsed once from env at engine construction — see the runtime
/// stance installer; the fn reads the static, never the environment.)
fn min_profit_floor() -> U256 {
    MIN_PROFIT_FLOOR_WEI.get().copied().unwrap_or(U256::ZERO)
}

/// PE4FPM: first-spawn latch for the `arb_sim_workers` census row — keeps
/// the per-sim hot path off the census mutex (the registration itself is
/// upsert-idempotent, so a lost race only rewrites the same row).
static ARB_SIM_CENSUS_SEEDED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

static MIN_PROFIT_FLOOR_WEI: std::sync::OnceLock<U256> = std::sync::OnceLock::new();

/// T3 (epic BXUSGL): `DEGENBOT_STREAMING_DELIVERY` — emit each clamp-passed
/// above-threshold result as an immediate single-entry `ResultBatch` during the
/// solve drain instead of waiting for the pump debounce. Parsed ONCE at
/// engine construction ([`install_engine_env_stances`]); engines copy the
/// parsed static into their construction field.
///
/// **Default flipped ON by epic SRQEK5 T3 (SF3QLP):** with detached cycles the
/// streaming mode is the intended shipped behaviour — each clamp-passed result
/// arrives at Python the moment its own solve completes (per-path
/// micro-batches composed with the end-of-cycle debounce sweep, per the
/// V6TOMQ coarse proof: 1360 single-candidate batches / 0 errors / 10-min
/// mainnet). `DEGENBOT_STREAMING_DELIVERY=0` opts out to the debounce sweep
/// (A/B); any other value (or unset) streams.
pub(crate) static STREAMING_DELIVERY_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// ADR-042 Q6 migration stance: `fleet.stance=fleet` routes the solve fan-
/// out (detached AND in-cycle arms) through the degenbot-workers fleet —
/// the fleet becomes the sole executor of solve bins. Construction reads
/// the CALLER's OWN cfg (`fleet_stance_enabled`, J4HN66) — never a
/// process-wide install-window static: a parallel construction could flip
/// such a static between the engine's install and read (TOCTOU).
///
/// `fleet.stance` → hosting decision (ADR-042 Q6: `legacy` keeps the
/// per-era mechanisms; `fleet` hosts Solver (and the Merge sidecar role)
/// on the role-switching fleet). Typed enum, so both stances are explicit.
#[must_use]
pub(crate) fn fleet_stance_enabled(cfg: &::degenbot_config::BotConfig) -> bool {
    matches!(cfg.fleet.stance, ::degenbot_config::FleetStance::Fleet)
}

/// `DEGENBOT_DETACHED_SOLVES` — the detached solve cycle (enqueue-and-return
/// with sidecar merge). Default ON since task 2UVG3E (epic MROOY7, stage-table
/// seam #4): the DRIVEN solve path takes NO engine-level Mutex — the
/// stage-surface (`EngineStages`) solve hold collapses to enqueue end (µs) and each result merges on
/// the sidecar under its own short per-item acquisition (the Q1a stale
/// policy makes that safe). Opt OUT with `DEGENBOT_DETACHED_SOLVES=0` (the
/// in-cycle arm reappears, engine Mutex held through the fan-out). The
/// in-flight cap remains the safety valve when the merge sidecar lags
/// (construction-time stance: every engine packs the caller's OWN cfg
/// value at construction — never a process-wide static, J4HN66).
///
/// `DEGENBOT_DETACHED_SOLVES` parse (2UVG3E default flip): unset/empty/1/
/// unknown values all route DETACHED (the shipped posture — the solve path
/// takes no engine-level Mutex); only an explicit 0/false/off opts back into
/// the in-cycle arm (engine Mutex held through the fan-out).
#[cfg(test)]
mod streaming_stance_tests {
    /// KAHU5W (presence-gated bools resolved): `pump.streaming_delivery` is
    /// now a plain schema bool; the env-parse policy matrix above is obsolete
    /// (the loader owns the words). The static default stays streaming.
    #[test]
    fn streaming_delivery_static_default_is_streaming() {
        assert!(super::STREAMING_DELIVERY_ENABLED.load(std::sync::atomic::Ordering::Relaxed,));
    }

    // 2UVG3E detached-solve default flip: the words now live in the
    // degenbot-config schema (default true; 0/false opt back in-cycle) with
    // precedence covered by the config tests.
}

/// Degenerate-path capture config parse (M6776W) — the owner side of the
/// `capture` config section (the gate itself reads no env). KAHU5W:
/// `gate_capture` is a typed bool (the presence-gated
/// `DEGENBOT_GATE_CAPTURE` legacy is retired; `0`/false disables).
#[must_use]
fn gate_capture_from_cfg(
    cfg: &::degenbot_config::BotConfig,
) -> Option<::degenbot_solvers::profit_envelope::GateCaptureCfg> {
    cfg.capture
        .gate_capture
        .then(|| ::degenbot_solvers::profit_envelope::GateCaptureCfg {
            out_path: cfg.capture.gate_capture_out.clone(),
            max_paths: u64::try_from(cfg.capture.gate_capture_cap).unwrap_or(u64::MAX),
        })
}

/// KAHU5W: the solver crate's runtime stance is INSTANCE-SCOPED — built
/// fresh per engine from the typed config and passed down; no `OnceLock`.
#[must_use]
pub fn solve_runtime_config_from_cfg(
    cfg: &::degenbot_config::BotConfig,
) -> ::degenbot_solvers::runtime::SolveRuntimeConfig {
    ::degenbot_solvers::runtime::SolveRuntimeConfig {
        event_solver_legacy: cfg.solve.walk_event_solver_legacy,
        walk_event_census: cfg.solve.walk_event_census,
        anchor_sweep: match cfg.solve.walk_anchor_sweep {
            ::degenbot_config::AnchorSweep::Off => ::degenbot_solvers::runtime::AnchorSweep::Off,
            ::degenbot_config::AnchorSweep::CenterOnly => {
                ::degenbot_solvers::runtime::AnchorSweep::CenterOnly
            }
            ::degenbot_config::AnchorSweep::Full => ::degenbot_solvers::runtime::AnchorSweep::Full,
        },
        max_tangent_lines: cfg.solve.envelope_max_tangent_lines,
        sampled_compose_lines: cfg.solve.envelope_sampled_compose_lines,
        memo_on: cfg.solve.solver_walk_memo,
        memo_stats: cfg.solve.solver_walk_memo_stats,
        gate_trace: cfg.trace.gate_trace,
    }
}

/// T4 (KAHU5W): the ONE config parse point for the engine's runtime stances —
/// called at engine construction with the typed `BotConfig`; hot paths read
/// the parsed statics. The crate performs ZERO environment reads: every stance
/// is a schema key (env or TOML loads into it via the degenbot-config loader).
/// The solver-runtime stance is NOT installed globally anymore — the engine
/// holds an instance value built by [`solve_runtime_config_from_cfg`] and
/// threads it down (KAHU5W: the solver `OnceLock` is retired).
pub fn install_engine_stances(cfg: &::degenbot_config::BotConfig) {
    // ADR-042 Q6: the fleet.stance migration flag. Under `fleet` the solve
    // bins ride the fleet-hosted executor; the typed boot descriptor
    // (quota + overrides + posture) is parsed here once.
    let fleet_hosted = fleet_stance_enabled(cfg);
    if fleet_hosted {
        let boot = degenbot_workers::dispatcher::FleetBoot::from_config(cfg);
        crate::arb_engine::fleet_solve_executor::install_boot(boot);
        // ADR-042 F4: the SimDriver seat pool shares the boot descriptor
        // (same quota + overrides + posture as the Solver-side host).
        crate::arb_engine::fleet_sim_executor::install_boot(boot);
        // PRG-3: the registration intake station shares the same boot
        // descriptor (duty-counted PoolStateUpdater slots, Deferrable
        // cordon class).
        crate::arb_engine::fleet_registration_executor::install_boot(boot);
    }
    STREAMING_DELIVERY_ENABLED.store(
        cfg.pump.streaming_delivery,
        std::sync::atomic::Ordering::Relaxed,
    );
    // J4HN66: streaming/detached stances are per-engine cfg values now
    // (packed at construction); this install keeps only the statics that
    // still have non-construction consumers (STREAMING; INLINE_SIM).
    INLINE_SIM_ENABLED.store(
        cfg.solve.solve_inline_sim,
        std::sync::atomic::Ordering::Relaxed,
    );
    let min_profit = U256::from(cfg.solve.min_profit_wei);
    let _ = MIN_PROFIT_FLOOR_WEI.set(min_profit);
    crate::bot_core::resolve::install_projection_memo_stance(cfg.solve.cl_projection_cache);
    // 7LV6VN T2: chunked parallel resolve stance, parsed once at construction.
    RESOLVE_PAR_STANCE.store(
        cfg.solve.solve_resolve_par,
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// K-slowest-path attribution record: (`time_us`, `pieces_visited`,
/// `path_sims`, `word_steps`, `refine_sims`, `gate_us`, `gate_derive_us`,
/// `gate_compose_us`, `gate_search_us`, `path_id`) — lets the completion
/// event name the cost driver of the slowest routes: gate-envelope bound
/// composition (with its derive/compose/search phase split) vs the walk
/// proper, not just wall time.
type PathTimeRecord = (u128, u64, u64, u64, u64, u64, u64, u64, u64, u64);
/// Min-heap (via `Reverse`) keeping only the K slowest paths in O(K) memory.
pub(crate) type PathTimesHeap = std::collections::BinaryHeap<std::cmp::Reverse<PathTimeRecord>>;

/// Per-path solve + diagnostics (epic BXUSGL T1): the former `solve_fn`
/// closure moved out verbatim so every dispatch arm (the legacy
/// the dedicated tokio executor - dispatch a path identically. Takes the
/// shared per-cycle context by reference; workers touch NO engine state
/// and NO core.lock (engine-then-core lock ordering preserved unchanged),
/// and the passed span is re-entered per item exactly as the `par_iter`
/// closure did (MQUKB6-T0: worker threads have no ambient context). Each
/// item also emits a `degenbot.arb.path` DEBUG child span parented under
/// that re-entered cycle span (MQUKB6-T2: per-path latency as attributes).
#[expect(clippy::too_many_lines)] // the moved solve + diagnostics pipeline is one narrative
pub(crate) fn solve_one_path(
    ctx: &SolveCycleShared,
    solve_span: &tracing::Span,
    pid: u64,
    resolved: &ResolvedMixedPath,
) -> Option<(u64, SolvePathResult)> {
    // Test-only deterministic slowen hook (the streaming-merge test).
    #[cfg(test)]
    if let Some(delay) = ctx.test_solve_delay.as_ref() {
        delay(pid);
    }
    // Worker-local view of the cycle gate deps (BXUSGL T1): the Arc-d
    // memo + owned capture cfg land in the shared ctx per cycle; the
    // prefix cache is generationed by the block epoch - same semantics
    // as the old single borrowed GateDeps shared by the scope workers.
    let gate_deps = ::degenbot_solvers::profit_envelope::GateDeps {
        epoch: ctx.epoch,
        prefix_cache: true,
        capture: ctx.gate_capture.as_ref(),
        walk_memo: Some(&*ctx.walk_memo),
        runtime: ctx.runtime,
    };
    let _solve_ctx = solve_span.enter();
    // MQUKB6-T2: per-path child span. Created BEFORE the walk (the exported
    // duration is the real solve wall) and recorded after; the walk counters
    // ride span ATTRIBUTES on this `degenbot.arb.path` node instead of
    // events on the cycle span, making per-path latency a Jaeger query
    // rather than a log grep. DEBUG level is the volume guard: production
    // INFO runs keep one node per CYCLE (a 200-path solve must not fan out
    // 200 Jaeger nodes by default); `RUST_LOG=degenbot::solver=debug` opts
    // into per-path nodes.
    let path_span = tracing::debug_span!(
        target: "degenbot::solver",
        "degenbot.arb.path",
        path.id = pid,
        path.us = tracing::field::Empty,
        path.sims = tracing::field::Empty,
        path.pieces = tracing::field::Empty,
        gate.us = tracing::field::Empty,
        path.profit = tracing::field::Empty,
    );
    let _path_ctx = path_span.enter();
    ::degenbot_solvers::profit_envelope::reset_gate_stats();
    let t0 = std::time::Instant::now();
    let outcome = ::degenbot_solvers::mixed::solve_path_with_min_profit(
        resolved,
        min_profit_floor(),
        &gate_deps,
    );
    let micros = t0.elapsed().as_micros();
    ctx.solve_cpu_us.fetch_add(
        u64::try_from(micros).unwrap_or(u64::MAX),
        std::sync::atomic::Ordering::Relaxed,
    );
    let gs = ::degenbot_solvers::profit_envelope::take_last_gate_stats();
    let gate_us = u64::try_from(gs.duration_ns / 1_000).unwrap_or(u64::MAX);
    if let Some(p) = crate::instruments::pipeline() {
        #[expect(clippy::cast_precision_loss)]
        {
            p.observe_per_path_solve_duration(micros as f64 / 1e6);
            p.observe_per_path_gate_duration(gs.duration_ns as f64 / 1e9);
        }
    }
    ctx.gate_total.lock().merge(&gs);
    // Walk telemetry OUT the return path (SU7MAE T2): the
    // outcome carries this path's counters — no TLS
    // read-back. The Q3 dense one-shot alert is the
    // CONSUMER's decision.
    let outcome_stats = &outcome.stats;
    if outcome_stats.max_dense_words >= ::degenbot_solvers::mobius_v3_int::DENSE_OBSERVE_THRESHOLD
        && !WALK_DENSE_ALERTED.swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        tracing::warn!(
            max_dense_words = outcome_stats.max_dense_words,
            threshold = ::degenbot_solvers::mobius_v3_int::DENSE_OBSERVE_THRESHOLD,
            "Q3-DENSE: a CL range crossed the dense-word threshold; harvest a real capture"
        );
    }
    let ws = *outcome_stats;
    let (pieces, sims, word_steps, refine_sims, ternary_sims, grid_sims) = (
        ws.pieces,
        ws.sims,
        ws.word_steps,
        ws.refine_sims,
        ws.ternary_sims,
        ws.grid_sims,
    );
    // Record this block's measured walk sims for the next
    // block's LPT cost (loop-12 KUKHMX).
    ctx.sims_recorder
        .lock()
        .insert(pid, u64::try_from(sims).unwrap_or(0));
    // Loop-18: record measured gate time for the LPT cost too
    // (gate-heavy paths carry sims≈0 and were bin-packed cheap).
    ctx.gate_recorder.lock().insert(pid, gate_us);
    let (gate_derive_us, gate_compose_us, gate_search_us) = (
        u64::try_from(gs.derive_ns / 1_000).unwrap_or(u64::MAX),
        u64::try_from(gs.compose_ns / 1_000).unwrap_or(u64::MAX),
        u64::try_from(gs.search_ns / 1_000).unwrap_or(u64::MAX),
    );
    ctx.walk_ternary_total.fetch_add(
        u64::try_from(ternary_sims).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    ctx.walk_grid_total.fetch_add(
        u64::try_from(grid_sims).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    ctx.walk_pieces_total.fetch_add(
        u64::try_from(pieces).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    ctx.walk_sims_total.fetch_add(
        u64::try_from(sims).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    ctx.walk_word_steps_total.fetch_add(
        u64::try_from(word_steps).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    ctx.walk_refine_sims_total.fetch_add(
        u64::try_from(refine_sims).unwrap_or(0),
        std::sync::atomic::Ordering::Relaxed,
    );
    // MQUKB6-T2: seal the per-path span - walk counters become attributes
    // on the `degenbot.arb.path` node (guard drops at fn end, so the
    // recorded values are inside the exported duration).
    path_span.record("path.us", u64::try_from(micros).unwrap_or(u64::MAX));
    path_span.record("path.sims", u64::try_from(sims).unwrap_or(u64::MAX));
    path_span.record("path.pieces", u64::try_from(pieces).unwrap_or(u64::MAX));
    path_span.record("gate.us", gate_us);
    if let Some(r) = outcome.result.as_ref() {
        path_span.record("path.profit", tracing::field::display(r.profit));
    }
    let mut heap = ctx.path_times.lock();
    {
        let worst = heap.peek().map_or(
            u128::MAX,
            |std::cmp::Reverse((w, _, _, _, _, _, _, _, _, _))| *w,
        );
        if heap.len() < SLOWEST_PATHS_K || micros > worst {
            heap.push(std::cmp::Reverse((
                micros,
                u64::try_from(pieces).unwrap_or(0),
                u64::try_from(sims).unwrap_or(0),
                u64::try_from(word_steps).unwrap_or(0),
                u64::try_from(refine_sims).unwrap_or(0),
                gate_us,
                gate_derive_us,
                gate_compose_us,
                gate_search_us,
                pid,
            )));
            if heap.len() > SLOWEST_PATHS_K {
                heap.pop();
            }
        }
    }
    if let Some(cap) = ctx.capture.as_ref() {
        cap.maybe_capture(
            pid,
            ctx.solve_block,
            u64::try_from(micros).unwrap_or(u64::MAX),
            u64::try_from(sims).unwrap_or(0),
            u64::try_from(pieces).unwrap_or(0),
            outcome.result.as_ref(),
            resolved,
        );
    }
    if let Some(cap) = ctx.capture_mixed.as_ref() {
        cap.maybe_capture(
            pid,
            ctx.solve_block,
            u64::try_from(micros).unwrap_or(u64::MAX),
            u64::try_from(sims).unwrap_or(0),
            u64::try_from(pieces).unwrap_or(0),
            outcome.result.as_ref(),
            resolved,
        );
    }
    outcome.result.map(|r| (pid, r))
}

/// `DEGENBOT_SOLVE_INLINE_SIM` (SIMPIPE2 T2 → T4, task PIRX3W / AK7VJB):
/// relocate the CL-hop clamp from the engine-Mutex merge site INTO the
/// per-path solve worker, so the worker can simulate on the clamp-committed
/// inputs without an engine-lock round-trip (the M1 seam T1/T3 build on).
/// Parsed ONCE at engine construction.
///
/// **Default ON since the T4 mainnet soak** (2026-09-05): payload counts
/// matched solved paths per cycle, ~99% of sim batches skipped the FFI
/// dispatch, header→first-payload-render p50 1ms / p90 31ms (vs the option-A
/// FFI pipeline's ~26ms solve wall + 49ms async sim tail), and the 46-minute
/// soak ran with zero deadlocks/panics/storage-key incidents through a
/// 200k-path registration flood. `DEGENBOT_SOLVE_INLINE_SIM=0`/`false`
/// opts OUT (restores the legacy merge-site clamp for a run); unset keeps
/// the inline stance. Later 0.7 hardening may remove the env entirely.
pub(crate) static INLINE_SIM_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// SIMPIPE2 T2: the WORKER-side clamp — drive the merge-site-identical
/// clamp from the solve worker's `to_solve`-aligned pool-ref snapshot, so a
/// result is clamp-committed BEFORE the streaming handoff (the T1/T3 seam
/// simulates on a payload whose `consumed_inputs` are the merge-committed
/// values with no engine-lock round-trip). Stance-gated: with
/// `DEGENBOT_SOLVE_INLINE_SIM` unset the merge-site clamp runs exactly as
/// before (this fn is a no-op returning 0).
/// Invariant: twins > 0 ⟺ the clamp mutated the result (every clamp path
/// runs its twin first) — the merge site treats twins > 0 as
/// already-clamped and never re-clamps (a second pass would re-apply the
/// margin and corrupt the committed inputs).
fn clamp_result_in_worker(
    ctx: &SolveCycleShared,
    idx: usize,
    pid: u64,
    result: &mut SolvePathResult,
) -> u64 {
    if !ctx.worker_clamp || idx >= ctx.pool_refs.len() {
        return 0;
    }
    let core = ctx.core.read();
    ArbitrageEngine::clamp_result_with_state(&core, pid, &ctx.pool_refs[idx].pools, result)
}

/// SIMPIPE2 T3: the WORKER-side inline sim — resolve the per-path payload
/// from the clamp-committed result on the SAME worker context the T2 clamp
/// opened (shared core + to_solve-aligned pool refs; stance + hook gated).
/// The payload rides the result handoff so the merge never calls out — the
/// merge only stores/forwards. `None` = stance off, no hook, or the hook
/// reported failure-without-payload.
#[cfg(all(test, feature = "otel"))]
fn inline_sim_payload(
    ctx: &SolveCycleShared,
    idx: usize,
    pid: u64,
    result: &SolvePathResult,
    parent_span: &tracing::Span,
) -> Option<crate::arb_engine::inline_sim::SimulatedPathResult> {
    if !ctx.worker_clamp || idx >= ctx.pool_refs.len() {
        return None;
    }
    let sim = ctx.inline_sim.as_ref()?;
    // RKXN5Z/IJUBV3 (G6HSIS parity): the REAL per-path EVM sim rides the
    // `degenbot.bundle.simulate` span, parented under the entered cycle span
    // (both executor arms re-enter solve_span around `solve_one_path`), with
    // the terminal verdict recorded at close. This is the only remaining
    // owner of the name - the merge-site marker that used to borrow it is a
    // merge-span event now, so Jaeger's `bundle.simulate` spans are all
    // genuine ms-class simulations again.
    // 7LV6VN T1b: EXPLICIT parent at creation. TLS re-entry alone proved
    // insufficient on the detached bin threads (worker-side spans still
    // forked their own trace with a dangling parent id - 1041 roots/60s
    // live-probed). The macro `parent:` form binds the identity directly,
    // independent of the thread-local current span.
    let span = tracing::info_span!(
        parent: parent_span.clone(),
        "degenbot.bundle.simulate",
        sim.path = "worker_inline",
        path_id = pid,
        sim_block = ctx.solve_block,
        simulate.verdict = tracing::field::Empty,
        simulate.expected_profit = tracing::field::Empty,
        // SIMSPANDUP: declared so the seam-reused span keeps the ADR-040
        // error classification on the inline arm too.
        simulate.error_reason = tracing::field::Empty,
    );
    let _enter = span.enter();
    let payload = sim.simulate_path(crate::arb_engine::inline_sim::InlineSimRequest {
        path_id: pid,
        hops: std::clone::Clone::clone(&ctx.pool_refs[idx].pools),
        optimal_input: result.optimal_input,
        consumed_inputs: std::clone::Clone::clone(&result.consumed_inputs),
        hop_outputs: std::clone::Clone::clone(&result.hop_outputs),
        state_nonces: std::clone::Clone::clone(&result.state_nonces),
        sim_block: ctx.solve_block,
        block_timestamp: ctx.metadata.timestamp,
        parent_base_fee: ctx.metadata.base_fee_per_gas.unwrap_or(0),
        parent_gas_used: ctx.metadata.gas_used,
        parent_gas_limit: ctx.metadata.gas_limit,
    })?;
    // SIMSPANDUP: on failure the seam's SimSpanVerdict Drop (inside the
    // inline hook's task) already stamped this span with
    // `not_profitable`/`error` (+ error_reason) before the payload returns -
    // don't clobber the richer classification with the bare string.
    if payload.failure.is_none() {
        span.record("simulate.verdict", "profitable");
    }
    span.record(
        "simulate.expected_profit",
        tracing::field::display(result.profit),
    );
    Some(payload)
}

/// One scheduled sim: pid + the receipt the worker polls/joins.
#[derive(Default)]
struct PipelinedSims {
    pending: Vec<(u64, PendingSim)>,
}

impl PipelinedSims {
    fn schedule_one(
        &mut self,
        ctx: &SolveCycleShared,
        idx: usize,
        pid: u64,
        result: &SolvePathResult,
        parent_span: &tracing::Span,
    ) -> bool {
        // No hook / clamp stance off: no sim can ever land, so the caller
        // must flush the item immediately (payload None) — otherwise the
        // held item would wait on a receipt that never exists.
        if !ctx.worker_clamp || idx >= ctx.pool_refs.len() {
            return false;
        }
        let Some(sim) = ctx.inline_sim.as_ref() else {
            return false;
        };
        // 7LV6VN T1b: EXPLICIT parent at creation (TLS re-entry alone forked
        // orphan roots on worker threads). The span is created and entered
        // ON THE DRIVER THREAD (std thread context = no inherited span),
        // mirroring the legacy `inline_sim_payload` worker span byte for
        // byte so Jaeger nesting and the verdict records are unchanged:
        // the span stays open until the sim completes instead of closing
        // when the bin's synchronous call returns.
        let request = crate::arb_engine::inline_sim::InlineSimRequest {
            path_id: pid,
            hops: std::clone::Clone::clone(&ctx.pool_refs[idx].pools),
            optimal_input: result.optimal_input,
            consumed_inputs: std::clone::Clone::clone(&result.consumed_inputs),
            hop_outputs: std::clone::Clone::clone(&result.hop_outputs),
            state_nonces: std::clone::Clone::clone(&result.state_nonces),
            sim_block: ctx.solve_block,
            block_timestamp: ctx.metadata.timestamp,
            parent_base_fee: ctx.metadata.base_fee_per_gas.unwrap_or(0),
            parent_gas_used: ctx.metadata.gas_used,
            parent_gas_limit: ctx.metadata.gas_limit,
        };
        let sim = std::sync::Arc::clone(sim);
        let parent = parent_span.clone();
        let expected_profit = result.profit;
        // Two-runtime pacing (7LV6VN T5): the slot is acquired INSIDE the
        // driver thread, so a saturated sim pipeline parks queued sims at
        // zero CPU cost instead of stalling the bins mid-walk (T5 window:
        // schedule-time blocking starved the walks). Concurrent EXECUTING
        // sims stay bounded by the budget-derived cap - the explicit
        // The sim EXECUTION body is stance-invariant: one span
        // (`degenbot.bundle.simulate`, explicitly parented under the
        // caller's span — 7LV6VN T1b), one `simulate_path` call, the
        // SIMSPANDUP verdict records, and the receipt send. The arms differ
        // ONLY in the machinery that runs it.
        let (tx, rx) = std::sync::mpsc::channel();
        let run_sim_body = move || {
            let span = tracing::info_span!(
                target: "degenbot::solver",
                parent: parent,
                "degenbot.bundle.simulate",
                sim.path = "worker_inline",
                path_id = request.path_id,
                sim_block = request.sim_block,
                simulate.verdict = tracing::field::Empty,
                simulate.expected_profit = tracing::field::Empty,
                // SIMSPANDUP: declared so the seam-reused span keeps the
                // ADR-040 error classification on the inline arm too.
                simulate.error_reason = tracing::field::Empty,
            );
            let _enter = span.enter();
            let payload = sim.simulate_path(request);
            // SIMSPANDUP: as in the sync arm - the seam's SimSpanVerdict
            // Drop stamps the failure verdict (`not_profitable`/`error` +
            // error_reason) before the payload returns; not clobbering it
            // keeps the richer classification. A `None` payload = hook
            // miss (no sim ran), so honestly no verdict stamp at all.
            if payload.as_ref().is_some_and(|p| p.failure.is_none()) {
                span.record("simulate.verdict", "profitable");
            }
            span.record(
                "simulate.expected_profit",
                tracing::field::display(expected_profit),
            );
            let _ = tx.send(payload);
        };
        if ctx.sim_fleet_hosted {
            // ADR-042 F4: the fleet is the sole executor of inline sims —
            // the request rides a pooled SimDriver unit (dispatch lane 2:
            // queued sims drain before new Solver intake; cordon floors the
            // sim intake and never cancels in-flight sims). The seat pool
            // is the budget's sim slot cap — the fleet-side bound that
            // replaces the SimSlots semaphore. Receipts ride the SAME
            // per-request channel, so the poll/join contract is untouched.
            crate::arb_engine::fleet_sim_executor::global_fleet_sim_executor().spawn(run_sim_body);
        } else {
            // Two-runtime pacing (7LV6VN T5): the slot is acquired INSIDE the
            // detached sim thread, so a saturated sim pipeline parks queued
            // sims at zero CPU cost instead of stalling the bins mid-walk
            // (T5 window: schedule-time blocking starved the walks).
            // Concurrent EXECUTING sims stay bounded by the budget-derived
            // cap. The guard releases exactly when the sim finishes.
            let slots = crate::arb_engine::sim_slots::sim_slots_global();
            // PE4FPM: the per-path detached sim threads are a burst resource; the
            // census row declares the pacing bound (the sim-slot cap) as the
            // sustained count and documents the burst in `sizing`. One short
            // registration at first spawn (hot path stays off the census lock).
            if ARB_SIM_CENSUS_SEEDED.get().is_none() {
                // First detached-sim spawn of the process: register the burst
                // resource. The OnceLock short-circuit keeps the hot path off
                // the census mutex; a lost race just upserts the same row.
                ARB_SIM_CENSUS_SEEDED.set(()).ok();
                degenbot_core::worker_census::register(
                    degenbot_core::worker_census::WorkerCensusEntry {
                        resource: "arb_sim_workers",
                        kind: "detached per-path sim threads (burst; one per scheduled sim, joined per cycle)",
                        count: crate::arb_engine::sim_slots::sim_slot_capacity(),
                        thread_name: "arb-sim-{pid}",
                        sizing: "burst paced by the sim_slots semaphore; sustained concurrent cap = the slot cap (leftover x 2, DEGENBOT_SOLVE_SIM_INFLIGHT override)",
                    },
                );
            }
            let spawned = std::thread::Builder::new()
                .name(format!("arb-sim-{pid}"))
                .spawn(move || {
                    slots.acquire();
                    let _slot = crate::arb_engine::sim_slots::SlotGuard::acquired(slots);
                    run_sim_body();
                });
            if spawned.is_err() {
                // Liveness: a failed spawn must not strand the receipt (the
                // poll/join would block forever on an empty channel). The
                // payload slot empties — the merge treats it as sim-failed.
                return false;
            }
        }
        self.pending.push((pid, PendingSim::new(rx)));
        true
    }

    /// Non-blocking sweep: hand back every sim that finished while the bin
    /// kept walking. Each pid surfaces exactly once.
    fn drain_ready(
        &mut self,
    ) -> Vec<(
        u64,
        Option<crate::arb_engine::inline_sim::SimulatedPathResult>,
    )> {
        let mut ready = Vec::new();
        let mut still = Vec::with_capacity(self.pending.len());
        for (pid, ps) in self.pending.drain(..) {
            match ps.try_result() {
                SimPoll::Ready(payload) => ready.push((pid, payload.map(|b| *b))),
                SimPoll::InFlight => still.push((pid, ps)),
            }
        }
        self.pending = still;
        ready
    }

    /// Bin-tail join: block for every outstanding sim. Order preserved.
    fn join_all(
        self,
    ) -> impl Iterator<
        Item = (
            u64,
            Option<crate::arb_engine::inline_sim::SimulatedPathResult>,
        ),
    > {
        self.pending.into_iter().map(|(pid, p)| (pid, p.result()))
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Send ONE held detached item after stamping its sim payload (7LV6VN T5).
/// The in-flight gauge bumps at SEND time exactly as the legacy inline
/// send did (a bin that dies before sending never leaks a count).
fn flush_detached_item(
    held: &mut Vec<DetachedMergeItem>,
    tx: &std::sync::mpsc::Sender<DetachedMergeItem>,
    outstanding_bin: &std::sync::atomic::AtomicU64,
    done_pid: u64,
    payload: Option<SimulatedPathResult>,
) {
    let Some(ix) = held
        .iter()
        .position(|it| matches!(it, DetachedMergeItem::Solved { pid, .. } if *pid == done_pid))
    else {
        return;
    };
    let DetachedMergeItem::Solved { payload: slot, .. } = &mut held[ix];
    *slot = payload;
    let item = held.remove(ix);
    if tx.send(item).is_ok() {
        outstanding_bin.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Tokio-arm twin of [`flush_detached_item`]: stamp the sim payload onto
/// the held outcome and stream it to the drain (7LV6VN T5).
fn flush_tokio_item(
    held: &mut Vec<(u64, Option<SolveArmOutcome>)>,
    lane: &mut SolveLane,
    done_pid: u64,
    payload: Option<SimulatedPathResult>,
) {
    let Some(ix) = held.iter().position(|(pid, _)| *pid == done_pid) else {
        return;
    };
    let Some(o) = held[ix].1.as_mut() else {
        return; // already flushed
    };
    o.3 = payload;
    let item = held.remove(ix);
    if let Some(outcome) = item.1 {
        lane.solved(outcome);
    }
}

/// Per-cycle shared solve context (epic BXUSGL T1): everything the
/// per-path dispatch touches besides the resolved snapshot. Bundled once
/// per cycle so a worker handle is static for the dedicated-executor
/// arm; the caller retains its own Arc for the drain + tail telemetry.
pub(crate) struct SolveCycleShared {
    solve_block: u64,
    epoch: u64,
    gate_capture: Option<::degenbot_solvers::profit_envelope::GateCaptureCfg>,
    walk_memo: std::sync::Arc<::degenbot_solvers::mobius_v3_int::WalkMemo>,
    /// KAHU5W: the instance-scoped solver runtime stance, threaded down —
    /// the solver crate has no process-global config anymore.
    runtime: ::degenbot_solvers::runtime::SolveRuntimeConfig,
    capture: Option<std::sync::Arc<HeavyClPathCapture>>,
    capture_mixed: Option<std::sync::Arc<HeavyMixedPathCapture>>,
    path_times: parking_lot::Mutex<PathTimesHeap>,
    gate_total: parking_lot::Mutex<::degenbot_solvers::profit_envelope::GateStats>,
    solve_cpu_us: std::sync::atomic::AtomicU64,
    walk_pieces_total: std::sync::atomic::AtomicU64,
    walk_sims_total: std::sync::atomic::AtomicU64,
    walk_word_steps_total: std::sync::atomic::AtomicU64,
    walk_refine_sims_total: std::sync::atomic::AtomicU64,
    walk_ternary_total: std::sync::atomic::AtomicU64,
    walk_grid_total: std::sync::atomic::AtomicU64,
    /// Engine-owned per-path measured-sims recorder (Arc-d engine field).
    sims_recorder: std::sync::Arc<parking_lot::Mutex<HashMap<u64, u64>>>,
    /// Engine-owned per-path gate-us recorder (Arc-d engine field).
    gate_recorder: std::sync::Arc<parking_lot::Mutex<HashMap<u64, u64>>>,
    /// Test-only deterministic per-path delay hook (epic test knob).
    #[cfg(test)]
    test_solve_delay: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync>>,
    /// SIMPIPE2 T2: the shared core (Arc-cloned from the engine at cycle
    /// build) — the WORKER-side clamp takes the same short core read the
    /// merge-site clamp took; no engine state is touched (MQUKB6-T3 intact:
    /// engine-then-core ordering, short read, no guard across awaits).
    core: std::sync::Arc<crate::bot_core::state_lock::StateLock<BotState>>,
    /// Per-path pool-ref snapshot, ALIGNED TO `to_solve` ORDER (index i in
    /// every bin mirrors `to_solve[i]`): the worker clamp's pool list, taken
    /// under the cycle's engine Mutex (stable for the whole cycle).
    pool_refs: Vec<std::sync::Arc<MixedPath>>,
    /// The cycle's block metadata (Copy) — the inline-sim request's block env
    /// (solve block from `solve_block`; timestamp/base-fee from here).
    metadata: BlockMetadata,
    /// The stance copy (construction-time static read at cycle build).
    worker_clamp: bool,
    /// SIMPIPE2 T3: the engine's inline-sim hook snapshot. `Some` + stance ON
    /// → the worker resolves the per-path payload right after the clamp (no
    /// engine lock — the same off-lock seam the worker clamp opened).
    inline_sim: Option<std::sync::Arc<dyn crate::arb_engine::inline_sim::InlineSimulator>>,
    /// ADR-042 F4 stance copy: `fleet.stance=fleet` routes the pipelined
    /// sims through the fleet-hosted `SimDriver` executor (the fleet is the
    /// sole executor of sim units — the arb-sim detached threads retire).
    sim_fleet_hosted: bool,
}

// ---------------------------------------------------------------------------
// DETACHED SOLVE CYCLE (epic SRQEK5, task WV62TX)
// ---------------------------------------------------------------------------
// DETACH-ALWAYS (design locked 2026-09-02): under `detached_solving`
// (`DEGENBOT_DETACHED_SOLVES`, construction-time stance) the whole solve
// cycle RETURNS at ENQUEUE end — every result then flows through an
// UNBOUNDED mpsc to the merge sidecar, a plain `std::thread` (see the
// epic DEADLOCK note: a JOINING scope (a scoped rayon install of old, or
// against a held `parking_lot` guard; `std::thread` cannot deadlock with
// impatient pool) would starve against the Mutex; the sidecar cannot. The Q1a stale policy
// (apply-if-unchanged / drop-on-touched) makes the enqueue-time per-hop
// `update_block` snapshot a complete staleness oracle: a price-neutral
// liquidity event (V3 Mint/Burn, V4 ModifyLiquidity) advances the pool
// clock AND re-solves the path, so any stamp mismatch at merge time means
// the straggler's intake is stale and the result is DROPPED, never applied.
// This gate is now the SOLE staleness guard on the solve path (the ADR-021
// in-process solver-state tripwire retired with task 2UVG3E; only the
// upstream RPC-disagreement check survives at the Published edge).

/// Design-locked in-flight cap (~8): more than this many un-merged detached
/// results outstanding degrades the issuing cycle to the pre-epic in-cycle
/// path (backpressure via fallback — a lagging sidecar must not accumulate
/// unbounded stragglers). The A/B probe measured healthy detached cycles
/// draining inside the merge makespan, so the cap is a safety valve, not the
/// steady-state controller.
pub(crate) const DETACHED_INFLIGHT_CAP: u64 = 8;

/// One unit of detached-merge work: a solved result that passed the same
/// profitless filter the in-cycle arms apply. `update_stamp` is the
/// per-hop `pool_update_block` snapshot taken at the enqueue resolve —
/// the Q1a staleness oracle compared against the LIVE clocks at merge.
pub(crate) enum DetachedMergeItem {
    Solved {
        /// Issuing cycle's detached sequence (straggler-age telemetry).
        cycle_seq: u64,
        /// The solve block the result was computed against.
        solve_block: u64,
        /// Cycle metadata for the streaming emission (Copy).
        metadata: BlockMetadata,
        pid: u64,
        /// Per-hop `pool_update_block` snapshot at the enqueue resolve.
        update_stamp: Vec<u64>,
        result: SolvePathResult,
        /// SIMPIPE2 T2: twins from the WORKER-side clamp (stance-gated);
        /// > 0 tells the merge the result is already clamp-committed.
        worker_clamp_twins: u64,
        /// SIMPIPE2 T3: the WORKER-side inline-sim payload (None = stance
        /// off / no hook / hook failure-without-payload).
        payload: Option<crate::arb_engine::inline_sim::SimulatedPathResult>,
        /// The solve-cycle span at ENQUEUE time (MQUKB6-T2). The sidecar
        /// std-thread has NO ambient tracing context, so the item carries
        /// the issuing `degenbot.arb.solve` span and the merge enters it
        /// per item - merge-time events and the Q1a drop/apply decisions
        /// parent under the issuing cycle instead of orphaning into Jaeger
        /// roots. `Span::none()` in unit tests is an inert no-op.
        solve_span: tracing::Span,
    },
}

/// The detached-merge SIDECAR thread body (epic SRQEK5 WV62TX): owns the
/// unbounded mpsc `Receiver` of the merge pipe and applies each item under
/// the engine Mutex — Q1a stale gate + the SAME merge/emit path as the
/// in-cycle drain (`merge_one_result`, which carries the streaming
/// delivery emission). Spawned by `EngineStages::solve_dirty` at the FIRST
/// detached enqueue; runs until every `Sender` drops (engine teardown),
/// so the pipe never strands items across the engine's lifetime.
pub(crate) fn detached_merge_sidecar(
    engine: &std::sync::Arc<parking_lot::Mutex<ArbitrageEngine>>,
    merge_rx: std::sync::mpsc::Receiver<DetachedMergeItem>,
) {
    hotpath::measure_block!("arb_solve.detached_merge", {
        for item in merge_rx {
            engine.lock().merge_detached_item(item);
        }
    });
}
impl ArbitrageEngine {
    /// The CL-hop clamp margin (absolute wei, subtracted from `input_consumed`
    /// before it is committed). VAASFM decision: 1 wei — commit
    /// `input_consumed - 1` so the exact-in loop converts nearly everything and
    /// stops on `amountRemaining==0` at the last funded tick. 1 wei is the
    /// maximum-extraction choice; a larger margin can be revisited if runaway
    /// swaps recur. Override via the `CLAMP_MARGIN` env var for sensitivity
    /// sweeps (twin of the `path5000_v2v4v3_solver_fixture` fixture).
    ///
    /// ## Measured basis (ergo 7E5D7W)
    ///
    /// The margin must be strictly larger than the worst solver-vs-engine
    /// (solver `hop_outputs[i]` vs the tier-3-proven `v4_simulate_swap`/
    /// `v3_simulate_swap` pool twin) OVER-prediction, so the clamp never lands
    /// exactly on an over-predicted tight value and re-enters the EMPTY march
    /// (UO3JM4). The `v4_crossing_solver_vs_sim_parity`/
    /// `v4_word_boundary_solver_divergence`/`v4_fee1_solver_path_matches_v4_simulate_swap`
    /// suites assert byte-exact solver==twin across the fee-3000/ts-60 multi-tick
    /// corpus AND the fee-1/ts-1 low-fee topology in both swap directions — i.e.
    /// the worst observed over-prediction is **0 wei**. The historical live
    /// `+1..+3` wei residuals (fee-1, ts=1) were localized to crossing-math
    /// rounding and fixed (the zfo step-0 current-tick flooring), not absorbed
    /// by margin. A dedicated sweep
    /// (`cl_hop_clamp_margin_exceeds_worst_solver_over_prediction`) measures the
    /// strict over-prediction direction across the corpus and asserts
    /// `margin > worst`, guarding this choice against regression. 1 wei is the
    /// smallest positive integer > 0, giving zero extraction loss (path-5000
    /// fixture: clamped output == solver output byte-identical).
    fn cl_hop_clamp_margin() -> U256 {
        std::env::var("CLAMP_MARGIN")
            .ok()
            .and_then(|s| s.parse::<u128>().ok())
            .map_or_else(|| U256::from(1u128), U256::from)
    }

    /// Merge ONE solved result under the single engine-Mutex hold of the
    /// solve cycle (epic BXUSGL T1): clamp twins, emit the profitable-
    /// solve event, insert into the result map, and (test-only) probe-
    /// record the merge. BOTH dispatch arms funnel here - the tokio arm
    /// calls it per streamed result (before the slowest path lands),
    /// the batched path from the tail loop. Returns the twin-simulation
    /// count (clamp.twins).
    ///
    /// SIMPIPE2 T2: `worker_clamp_twins > 0` means the solving worker
    /// ALREADY ran the clamp (`clamp_result_in_worker`) - the merge skips its
    /// own pass (a second clip would re-apply the 1-wei margin and corrupt
    /// count (clamp.twins).
    fn merge_one_result(
        &mut self,
        solve_block: u64,
        metadata: &BlockMetadata,
        pid: u64,
        result: SolvePathResult,
        worker_clamp_twins: u64,
        payload: Option<SimulatedPathResult>,
    ) -> u64 {
        let mut result = result;
        let twins = if worker_clamp_twins > 0 {
            worker_clamp_twins
        } else {
            self.clamp_cl_hop_capacity(pid, &mut result)
        };
        // Telemetry: profitable solves are the signal in the noise -
        // emit the economics + the concrete hop list on the solve span.
        // M2 probe: per-result events cross the Python log bridge (GIL) -
        // size that cost vs the map/delivery work (hotpath-attributed).
        hotpath::measure_block!("merge.telemetry_event", {
            tracing::info!(
                target: "degenbot::solver",
                block_number = solve_block,
                path.id = pid,
                input = %result.optimal_input,
                profit = %result.profit,
                path.hops = %self.describe_path_cached(pid),
                "[path] profitable solve"
            );
        });
        // SIMPIPE2 T3: the inline-sim payload — store it so the delivery
        // diff ships it with the batch (`None` = the path re-solved without a
        // payload this cycle — stance off or hook failure — so any stale
        // entry MUST drop). RKXN5Z/IJUBV3: the merge emits the terminal
        // verdict as an EVENT on the enclosing merge span (both arms hold
        // `degenbot.arb.merge` here; the detached sidecar re-enters the solve
        // span). The former `degenbot.bundle.simulate` marker span collided
        // with the real EVM-sim spans of the same name and flooded every
        // block trace with 90-300 microsecond lookalikes. The span name now
        // belongs to simulation work only (worker seam + the FFI seam in
        // degenbot-arbitrage/simulator.rs).
        if let Some(p) = &payload {
            let verdict = if p.failure.is_some() {
                "not_profitable"
            } else {
                "profitable"
            };
            // DEBUG-gated (log-volume cut OPBD7L): this event duplicates the
            // `[path] profitable solve` event above (same path.id/profit) and
            // the Python-side `[sim]` summary; the settle verdict is also
            // observable as an event on the enclosing `degenbot.arb.merge`
            // OTel span. Re-enable with `RUST_LOG=degenbot_bot=debug`.
            tracing::debug!(
                target: "degenbot::solver",
                { path.id = pid, verdict, expected_profit = %result.profit, sim.seam = "inline_payload_store" },
                "[bundle] inline payload settle"
            );
        }
        hotpath::measure_block!("merge.payload_store", {
            match payload {
                Some(p) => {
                    self.inline_payloads.insert(pid, p);
                }
                None => {
                    self.inline_payloads.remove(&pid);
                }
            }
        });
        // T3 (epic BXUSGL): DEGENBOT_STREAMING_DELIVERY - each above-threshold
        // merged result is emitted IMMEDIATELY (before the slowest path can
        // possibly delay it). The per-entry emission composes with the
        // debounce sweep, which still owns expired/removed + the end-of-cycle
        // metadata batch.
        let payload_now = self.inline_payloads.get(&pid).map(|e| e.value().clone());
        if self.streaming_delivery {
            hotpath::measure_block!("merge.delivery_emit", {
                self.delivery.emit_single_result_batch(
                    solve_block,
                    metadata,
                    pid,
                    &result,
                    payload_now.as_ref(),
                );
            });
        }
        self.results.insert(pid, result);
        #[cfg(test)]
        if let Some(probe) = &self.merge_probe {
            probe.lock().push(pid);
        }
        twins
    }

    /// Terminal disposition of ONE detached straggler, under the single
    /// engine-Mutex acquisition the sidecar makes per item (epic SRQEK5
    /// WV62TX). Q1a policy: apply-if-unchanged, drop-on-touched — ANY live
    /// stamp advance since enqueue (swap OR price-neutral liquidity event)
    /// drops the straggler; a deregistered path drops too. The in-flight
    /// gauge was bumped at SEND time in the enqueue half and is decremented
    /// here exactly once per item, so a bin that dies before sending never
    /// leaks a count.
    pub(crate) fn merge_detached_item(&mut self, item: DetachedMergeItem) {
        let outstanding_now = self
            .detached_outstanding
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(1);
        if let Some(p) = crate::instruments::pipeline() {
            p.set_detached_in_flight(outstanding_now);
        }
        hotpath::gauge!("detached_solve_in_flight").set(f64::from(
            u32::try_from(outstanding_now).unwrap_or(u32::MAX),
        ));
        let DetachedMergeItem::Solved {
            cycle_seq,
            solve_block,
            metadata,
            pid,
            update_stamp,
            result,
            worker_clamp_twins,
            payload,
            solve_span,
        } = item;
        // MQUKB6-T2: re-enter the enqueue-time cycle span for the whole
        // merge (Q1a drop/apply events + any profit emit parent there).
        // Inert without a subscriber or for `Span::none()` test items.
        let _merge_ctx = solve_span.enter();
        let age_cycles = self.detached_issued_seq.saturating_sub(cycle_seq);
        // Q1a deregister: nothing to merge into — drop, never re-create.
        let Some(registered) = self.path_pools.get(&pid) else {
            self.detached_dropped_deregistered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(p) = crate::instruments::pipeline() {
                p.count_detached_stale_dropped();
            }
            tracing::debug!(
                target: crate::telemetry::DIAGNOSTIC_TARGET,
                path_id = pid,
                detached_seq = cycle_seq,
                "[detached] straggler dropped (path deregistered)"
            );
            return;
        };
        // Q1a stale: re-read the LIVE per-hop clocks; any advance since the
        // enqueue resolve invalidates the straggler's intake.
        let live_stamp: Vec<u64> = {
            let core = self.core.read();
            registered
                .pools
                .iter()
                .map(|pool_ref| core.pool_update_block(pool_ref.pool_key))
                .collect()
        };
        if live_stamp != update_stamp {
            self.detached_dropped_stale
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(p) = crate::instruments::pipeline() {
                p.count_detached_stale_dropped();
            }
            tracing::info!(
                target: crate::telemetry::DIAGNOSTIC_TARGET,
                path_id = pid,
                detached_seq = cycle_seq,
                detached_age_cycles = age_cycles,
                "[detached] straggler dropped (stale: pools moved during the solve)"
            );
            return;
        }
        self.detached_applied
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(p) = crate::instruments::pipeline() {
            p.count_detached_applied();
        }
        tracing::debug!(
            target: crate::telemetry::DIAGNOSTIC_TARGET,
            path_id = pid,
            detached_seq = cycle_seq,
            detached_age_cycles = age_cycles,
            "[detached] straggler merged (unchanged intake)"
        );
        self.merge_one_result(
            solve_block,
            &metadata,
            pid,
            result,
            worker_clamp_twins,
            payload,
        );
        // ADR-021 publish-verifier scoping retired (task 2UVG3E): the
        // solver-state verifier (and its publish change set) is gone — merges
        // apply the Q1a stale policy only.
    }

    /// Hand the parked merge-pipe Receiver to the spawner (epic SRQEK5
    /// WV62TX): `EngineStages::solve_dirty` takes it ONCE, at the FIRST
    /// Hand the parked merge-pipe Receiver to the spawner (epic SRQEK5
    /// WV62TX): `EngineStages::solve_dirty` takes it ONCE, at the FIRST
    /// detached enqueue, and owns it inside the sidecar thread. `None` = the
    /// sidecar is already running (or no detached cycle ever enqueued).
    pub(crate) fn take_detached_merge_rx(
        &mut self,
    ) -> Option<std::sync::mpsc::Receiver<DetachedMergeItem>> {
        self.detached_merge_rx.lock().take()
    }

    /// Post-solve, pool-state-aware reconciliation of each CL hop's committed
    /// input against the pool's true max-convertible capacity — the tier-3-
    /// validated `v3_simulate_swap`/`v4_simulate_swap` twin (UO3JM4: the pure
    /// solver's frozen int walk can over-predict the pools twin by a few wei,
    /// so the authoritative bound comes from pool state, not the solver).
    ///
    /// `solve_path` runs lock-free on its `IntV3TickRangeSequence` snapshot
    /// (ADR-015: the guard drops before parallel work) and reports
    /// `consumed_inputs[i] = hop_outputs[i-1]` — the FULL forward, which can
    /// over-feed a CL pool past its on-chain capacity. When that happens the
    /// exact-in loop cannot exhaust the input and marches empty bitmap words to
    /// `MAX_SQRT_PRICE` (the path-5000 20.7M-gas / 5M-ceiling EMPTY-HALT class,
    /// AGENTS.md UO3JM4). This method re-reads the live
    /// `V3PoolState`/`V4PoolState` from the core at the solve→result merge seam
    /// and caps each CL hop's committed input to `input_consumed - margin`, so
    /// the on-chain loop exits on `amountRemaining==0` at the last funded tick.
    ///
    /// `hop_outputs[i]` is left untouched: for an over-feeding CL pool,
    /// `output(capacity) == output(over-feed)`, so the solver's predicted output
    /// is already correct (verified byte-exact by the path-5000 fixture). Only
    /// CL hops (V3/V4) have the word-boundary empty-march class; V2 / Curve /
    /// Balancer / Solidly consume their full input at the boundary and need no
    /// clamp.
    /// Returns the number of twin simulations executed (telemetry:
    /// `clamp.twins` on the solve-cycle completion event).
    pub(crate) fn clamp_cl_hop_capacity(&self, path_id: u64, result: &mut SolvePathResult) -> u64 {
        let Some(path) = self.path_pools.get(&path_id) else {
            return 0; // Unknown path → nothing to clamp
        };
        let core = self.core.read();
        Self::clamp_result_with_state(&core, path_id, &path.pools, result)
    }

    /// The clamp shared by the merge-site gate and the SIMPIPE2 T2 worker
    /// relocation — the pool list is a parameter so the WORKER can drive the
    /// identical logic from its `to_solve`-aligned snapshot. The worker takes
    /// the SAME short core read the merge-site clamp took (MQUKB6-T3 intact:
    /// engine-then-core, short read, no guard across awaits).
    #[expect(clippy::too_many_lines)] // multi-hop CL twin loop + post-clamp profit recompute
    fn clamp_result_with_state(
        core: &BotState,
        path_id: u64,
        pools: &[MixedPoolRef],
        result: &mut SolvePathResult,
    ) -> u64 {
        if pools.len() != result.consumed_inputs.len() {
            return 0; // Index misalignment — never clamp a wrong hop
        }
        let margin = Self::cl_hop_clamp_margin();
        // D63GSE: successful twin simulations executed this call (returned to
        // the caller for the solve-cycle completion event).
        let mut twins_executed: u64 = 0;
        for (i, pool_ref) in pools.iter().enumerate() {
            let requested = result.consumed_inputs[i];
            // Run the tier-3-validated twin once per clamped family so we can
            // (a) clamp this hop's INPUT (CL marching empty-word EMPTY-HALT
            // class), (b) clamp this hop's FORWARD (`consumed_inputs[i+1]` =
            // the next hop's input, which the composer's V4 take/exchange
            // derives from this hop's OUTPUT) to the byte-exact twin output,
            // and (c) re-align this hop's REPORTED output. (b)/(c) close the
            // path-73385 class: the solver OVer-predicted the V4 output by
            // 3 wei, so the take (`consumed_inputs[i+1]`) over-took the pool's
            // actual output and the trailing V4_SETTLE_ALL repaid the 3-wei
            // residual via a `USDT.transfer(PM,3)` that halted (0xfe). (c) is
            // equally load-bearing for a V2 hop whose INPUT the upstream hop's
            // (b) just reduced: the walk-frozen `hop_outputs[i]` would
            // otherwise keep the pre-clamp input's output — the
            // path-182449/110302 1-wei over-prediction that failed on-chain
            // with `UniswapV2: K`.
            let (out, input_clamp): (U256, Option<U256>) = match pool_ref.hop_type {
                HopType::V3 => {
                    let (Some(state), Some(identity)) = (
                        core.get_v3_pool(pool_ref.pool_key),
                        core.get_v3_identity(pool_ref.pool_key),
                    ) else {
                        continue; // Pool state unavailable → can't clamp
                    };
                    let Ok(amount) = I256::try_from(requested) else {
                        continue; // Input too large for i256 → skip
                    };
                    let limit = V3PoolState::default_sqrt_price_limit(pool_ref.zero_for_one);
                    let Some(twin) = v3_simulate_swap(
                        state,
                        identity.fee,
                        identity.tick_spacing,
                        pool_ref.zero_for_one,
                        amount,
                        limit,
                    )
                    .ok() else {
                        continue;
                    };
                    let out = if pool_ref.zero_for_one {
                        twin.amount1
                    } else {
                        twin.amount0
                    };
                    twins_executed += 1;
                    (out, twin.exact_input_clamp_bound(requested, margin))
                }
                HopType::V4 => {
                    let (Some(state), Some(identity)) = (
                        core.get_v4_pool(pool_ref.pool_key),
                        core.get_v4_identity(pool_ref.pool_key),
                    ) else {
                        continue;
                    };
                    let Ok(amount) = I256::try_from(requested) else {
                        continue;
                    };
                    // V4 exact-in passes a NEGATIVE amount (opposite sign to V3).
                    let Some(neg) = amount.checked_neg() else {
                        continue; // MIN_i256 (no positive twin) → skip
                    };
                    let limit = V3PoolState::default_sqrt_price_limit(pool_ref.zero_for_one);
                    let Some(twin) = v4_simulate_swap(
                        state,
                        identity.pool_key.fee,
                        identity.pool_key.tick_spacing,
                        pool_ref.zero_for_one,
                        neg,
                        limit,
                    )
                    .ok() else {
                        continue;
                    };
                    let out = if pool_ref.zero_for_one {
                        twin.amount1
                    } else {
                        twin.amount0
                    };
                    twins_executed += 1;
                    (out, twin.exact_input_clamp_bound(requested, margin))
                }
                HopType::V2 => {
                    // V2 has no empty-march class (no input clamp), but its
                    // byte-exact twin output must still be the authoritative
                    // report once (b) has forward-clamped its input upstream.
                    // Orientation mirrors `simulate_swap`'s V2 arm.
                    let (Some(state), Some(identity)) = (
                        core.get_v2_pool_state(pool_ref.pool_key),
                        core.get_v2_identity(pool_ref.pool_key),
                    ) else {
                        continue; // Pool state unavailable → can't clamp
                    };
                    let (reserve_in, reserve_out, gamma_numer, fee_denom) = if pool_ref.zero_for_one
                    {
                        (
                            state.reserve0.to::<U256>(),
                            state.reserve1.to::<U256>(),
                            identity.fee_token0.0,
                            identity.fee_token0.1,
                        )
                    } else {
                        (
                            state.reserve1.to::<U256>(),
                            state.reserve0.to::<U256>(),
                            identity.fee_token1.0,
                            identity.fee_token1.1,
                        )
                    };
                    let Some(out) = degenbot_math::v2::IntHopState::new(
                        reserve_in,
                        reserve_out,
                        gamma_numer,
                        fee_denom,
                    )
                    .swap(requested)
                    .ok() else {
                        continue; // overflow reverts on-chain → nothing to align
                    };
                    twins_executed += 1;
                    (out, None)
                }
                // Curve / Balancer / Solidly — no byte-exact twin at this
                // seam; their reported outputs stand (see module note).
                _ => continue,
            };
            // (a) Input clamp: cap this CL hop's committed input at
            // `input_consumed - margin` when over-fed (the empty-march class).
            if let Some(clamped) = input_clamp {
                if clamped < requested {
                    if let Some(p) = crate::instruments::pipeline() {
                        p.count_clamp();
                    }
                    tracing::info!(
                        target: crate::telemetry::DIAGNOSTIC_TARGET,
                        "[clamp-cl] path_id={path_id} hop={i} family={:?} input requested={requested} \
                         clamped={clamped} reduction={}",
                        pool_ref.hop_type,
                        requested - clamped
                    );
                    result.consumed_inputs[i] = clamped;
                }
            }
            // (c) Align the solver's REPORTED output (`hop_outputs[i]`) to the
            // byte-exact twin output, so the solver is exact (not merely the
            // consumed forward). This is the path-73385 fix: the solver
            // over-predicted the V4 output by 3 wei; the twin is the on-chain
            // truth, so the published hop_outputs become byte-exact too.
            if let Some(hop_out) = result.hop_outputs.get_mut(i) {
                if *hop_out != out {
                    if let Some(p) = crate::instruments::pipeline() {
                        p.count_clamp();
                    }
                    tracing::info!(
                        target: crate::telemetry::DIAGNOSTIC_TARGET,
                        "[clamp-cl-hop] path_id={path_id} hop={i} family={:?} hop_outputs={hop_out} \
                         twin_out={out} delta={}",
                        pool_ref.hop_type,
                        if *hop_out > out { *hop_out - out } else { out - *hop_out }
                    );
                    *hop_out = out;
                }
            }
            // (b) Forward clamp: the next hop's executable input
            // (`consumed_inputs[i+1]` — what the composer's V4 take/exchange
            // withdraws from this hop's output) must not exceed this hop's
            // actual yield, or the pool is over-taken and a residual delta is
            // repaid via a failing USDT transfer (path-73385).
            if i + 1 < pools.len() {
                let forward = result.consumed_inputs[i + 1];
                if out < forward {
                    if let Some(p) = crate::instruments::pipeline() {
                        p.count_clamp();
                    }
                    tracing::info!(
                        target: crate::telemetry::DIAGNOSTIC_TARGET,
                        "[clamp-cl-out] path_id={path_id} hop={i} family={:?} forward={forward} \
                         twin_out={out} reduction={}",
                        pool_ref.hop_type,
                        forward - out
                    );
                    result.consumed_inputs[i + 1] = out;
                }
            }
        }

        // BUG-B FIX (path-142603 `no-profit` crash): the solver's `profit` is
        // computed on its RAW (over-predicted) hop outputs; the CL clamp above
        // realigns execution to the twin but was not feeding back a recomputed
        // profit, so an actually-unprofitable path stayed `> min_profit` and was
        // dispatched → executed to a loss → `no-profit` abort. Recompute the
        // selection profit from the clamped values (see `recompute_clamped_profit`);
        // a post-clamp loss saturates to 0 and is dropped.
        if let Some(recomputed) = Self::recompute_clamped_profit(result) {
            let profit_before = result.profit;
            if recomputed != profit_before {
                tracing::info!(
                    path_id,
                    profit_before = %profit_before,
                    profit_after = %recomputed,
                    profit_delta = %profit_before.saturating_sub(recomputed),
                    "[profit-clamp] recomputed selection profit from twin-aligned outputs"
                );
                result.profit = recomputed;
            }
        }
        twins_executed
    }

    /// Recompute a path result's selection profit from its CLAMPED
    /// (twin-aligned) outputs, per the documented `SolvePathResult::profit`
    /// semantics `final_output - consumed_inputs[0]` (with
    /// `final_output = hop_outputs[last]`), evaluated on the corrected values
    /// so it reflects the executable state rather than the solver's pre-clamp
    /// over-prediction. A post-clamp loss saturates to `0`, which is dropped by
    /// the `profit > min_profit` delivery gate. Returns `None` for a degenerate
    /// path (no `hop_outputs` / `consumed_inputs`). Pure (no env, no `core`
    /// lock) so it is directly unit-testable independent of the CL-twin
    /// machinery.
    #[must_use]
    fn recompute_clamped_profit(result: &SolvePathResult) -> Option<U256> {
        let final_output = result.hop_outputs.last().copied()?;
        let first_consumed = result.consumed_inputs.first().copied()?;
        Some(final_output.saturating_sub(first_consumed))
    }

    /// Re-resolve and re-solve only paths that contain updated pools.
    ///
    /// Uses the `pool_to_paths` reverse index to identify `affected_path_ids`,
    /// then re-resolves and re-solves only those. Unaffected paths carry
    /// their previous results forward.
    #[expect(clippy::too_many_lines)] // telemetry events + solve pipeline are one narrative
    /// # Panics
    /// When the merged drain's outcome accounting undercounts (exactness
    /// fuse, QR3NUS/LW-T7): the cycle thread fails loudly, never silently
    /// mis-sizes.
    pub fn rebuild_and_solve_affected(
        &mut self,
        affected: &[degenbot_solvers::affected_keys::AffectedKey],
        block_number: u64,
        metadata: &BlockMetadata,
    ) {
        // MQUKB6-T0: worker threads (executor bins, resolve chunk threads,
        // solve executor's workers, AND the detached solve-bin std-threads)
        // have no ambient tracing context — any span emitted inside a
        // dispatch closure would orphan into a root trace. Capture the
        // caller's span (the drainer's `degenbot.arb.solve`) once and
        // re-enter it per work item; the detached arm additionally carries
        // it into `DetachedMergeItem` so the sidecar's merges inherit it
        // (MQUKB6-T2).
        let solve_span = tracing::Span::current();
        // D63GSE visibility: phase timing so a multi-second solve EXPLAINS
        // itself — fan-out / resolve / par-solve / clamp are separate events,
        // and the K slowest paths name where the wall-clock went.
        let cycle_start = std::time::Instant::now();
        // Collect affected path IDs from the reverse index
        let mut affected_path_ids: HashSet<u64> = HashSet::new();

        // R522XA: the state machine decides which touched paths actually need a
        // (re)resolve. Solvable/Unresolved re-check on any hop dirty; an Invalid
        // path re-checks ONLY when a responsible pool goes dirty AND the
        // container empties (last faulty pool cleared). Unrelated co-hop dirt
        // leaves an Invalid path untouched — no 100k-path re-resolve churn.
        hotpath::measure_block!("arb_solve.dirty_status_scan", {
            // LXDY4C: the affected keys ARE the delta's taken (HopType,
            // pool_id) reverse-index keys — one loop, no per-family intake.
            for key in affected {
                if let Some(path_ids) = self.pool_to_paths.get(&key.path_index_key()) {
                    for &path_id in path_ids {
                        if self
                            .path_status
                            .entry(path_id)
                            .or_default()
                            .on_pool_dirty(key.path_index_key())
                        {
                            affected_path_ids.insert(path_id);
                        }
                    }
                }
            }
        });

        // Also re-solve any paths registered via register_and_solve_path that
        // haven't been through rebuild_and_solve_affected yet. These paths were
        // eagerly solved at registration time, but the pump's process_block
        // replaces self.results entirely — so we must include them to avoid
        // dropping their results.
        affected_path_ids.extend(&self.pending_new_paths);
        self.pending_new_paths.clear();

        // Solve-block anchor (rule owner + history: `crate::bot_core::solve_anchor`):
        // the batch's `solve_block` (= `results_block`) is the block the pool
        // state actually reflects — the pool-state head, NOT the
        // (possibly-lagging) drain `block_number`. Since BO5FBS the pump
        // pre-promotes `active_block` before calling `on_drain`, so
        // `block_number` here is already >= the head and the re-anchor is a
        // defensive no-op on the pump path — it stays the guard for callers
        // that bypass the pump (e.g. tests driving `solve_dirty` directly).
        let anchor =
            crate::bot_core::solve_anchor::SolveAnchor::resolve(block_number, &self.core.read());
        let solve_block = anchor.block();
        // Cross-block walk-composition census: advance the epoch BEFORE the
        // per-path probes so a path solved both this block and the previous
        // one reports a hit (the engine-owned WalkMemo handle, SU7MAE T3).
        self.walk_memo.begin_block(solve_block);
        // If no paths are affected, just update the block number
        if affected_path_ids.is_empty() {
            self.results_block = solve_block;
            return;
        }

        // Telemetry: name EVERY path the dirty-pool fan-out just activated,
        // with its concrete hop list — a Jaeger trace now answers "which pools
        // are in this path" without cross-referencing Python state. Runs under
        // the drainer's `degenbot.arb.solve` span, so the events parent there.
        // MQUKB6-T2: phase span - the fan-out activation telemetry gets its
        // own Jaeger node under the cycle span (matches `arb_solve.*`
        // hotpath labels 1:1); the aggregate rides span attributes, so even
        // the phase-summary EVENT below can no longer orphan phase data.
        let fanout_ctx = tracing::info_span!(
            target: "degenbot::solver",
            "degenbot.arb.fanout",
            block.number = solve_block,
            paths.affected = affected_path_ids.len(),
        )
        .entered();
        hotpath::measure_block!("arb_solve.fanout_activate_telemetry", {
            // Per-path activation events are debug-level now (N225ET): the
            // per-event span plumbing dominated the fan-out phase; the
            // diagnostic remains reachable via RUST_LOG degenbot::engine=debug.
            if tracing::enabled!(target: "degenbot::engine", tracing::Level::DEBUG) {
                for &path_id in &affected_path_ids {
                    tracing::debug!(
                        target: "degenbot::engine",
                        block_number = solve_block,
                        path.id = path_id,
                        path.hops = %self.describe_path_cached(path_id),
                        dirty.keys = affected.len(),
                        "[path] activated by dirty pool"
                    );
                }
            }
        });

        // Telemetry: fan-out summary (activations above can be hundreds of
        // events; this one line carries the aggregate).
        let fanout_us = u64::try_from(cycle_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        tracing::info!(
            target: "degenbot::solver",
            block_number = solve_block,
            paths.affected = affected_path_ids.len(),
            dirty.keys = affected.len(),
            phase_us = fanout_us,
            "[solve-phase] fanned out to affected paths"
        );
        drop(fanout_ctx);

        // Re-resolve and solve only affected paths — update results in-place
        // without cloning unchanged entries.

        // Re-derive resolved hop states under the core lock — a single
        // consistent snapshot of BotState for the whole re-derive (ADR-003
        // Option A: one core-lock window per `solve_dirty`). V3/V4 state still
        // reads from the per-family block engines here; Slices 2/3 migrate
        // those into BotState too. The guard drops before `solve_path` runs,
        // which is pure `&self`.
        //
        // AV42C7 lesson: a per-path `update_block`-MIX freshness gate was
        // attempted here and REVERTED — it deferred every legitimate
        // single-pool-update arb (Sync pool A, solve with a stable reference
        // pool B at an older `update_block`). A zero-tolerance `update_block`
        // check cannot distinguish "this pool had no block-N event" (normal)
        // from "this pool is genuinely far behind" (missed swap events), so
        // its false-positive rate is catastrophic.
        //
        // YXHHKR (resolved QNFYR5): NO solve-time staleness gate here. The former
        // TQ43TU bounded-window gate deferred a whole path on any co-hop trailing
        // >10 blocks, but `update_block` is a last-activity clock, so a quiet-but-
        // current pool was falsely deferred (QNFYR5 proved 3,550 of them live).
        // `deferred_paths` is now reserved for the genuinely illegitimate future-
        // price case below; genuine chain/solver divergence is left to the ADR-021
        // verifier, which fatal-aborts loudly (the preferred failure, esp. in dev).
        self.paths_same_state_this_cycle = 0;
        // 7LV6VN T2: these accumulate from the chunk merges inside the resolve
        // block below (same content the serial loop used to produce inline).
        let mut deferred_paths: HashSet<u64> = HashSet::new();
        let mut invalid_reasons: HashMap<String, u64> = HashMap::new();
        // MQUKB6-T2: phase span for the core-lock re-derive window (the
        // summary event after the block stays on the same node).
        let resolve_ctx = tracing::info_span!(
            target: "degenbot::solver",
            "degenbot.arb.resolve",
            block.number = solve_block,
            paths.affected = affected_path_ids.len(),
        )
        .entered();
        // 7LV6VN T2: chunked fan-out over the affected paths. The
        // sharded hop cache keeps cross-path hit reuse intact (a chunk-local
        // cache would multiply the expensive CL tick-walk per shared pool),
        // so chunks contend only on shard locks, never on each other's work.
        // Per-path chunk outputs merge serially below in deterministic order;
        // `resolve_hops` semantics are byte-identical (same core-read window,
        // same deficits, same memo validation).

        hotpath::measure_block!("arb_solve.resolve", {
            // Violated only while a writer is queued (parking_lot read acquire):
            // nonzero = core-lock congestion, not compute.
            let core = hotpath::measure_block!("resolve.core_read_acquire", self.core.read());
            let resolve_chunk = |path_ids: &[u64]| -> ResolveChunkOut {
                let mut out = ResolveChunkOut {
                    resolved: Vec::new(),
                    status: Vec::new(),
                    snapshots: Vec::new(),
                    same_state: 0u64,
                    projections: 0u64,
                    invalid_reasons: HashMap::new(),
                    deferred: Vec::new(),
                };
                for (chunk_pos, &path_id) in path_ids.iter().enumerate() {
                    let Some(path) = self.path_pools.get(&path_id) else {
                        continue;
                    };
                    // U6RNHH T1 solve-stage future-price tripwire: a hop whose PRICE
                    // clock runs ahead of the solve anchor is never legitimate and is
                    // rejected loudly (deferred + logged), not solved — a future-price
                    // solve reports a misleading downstream IIA. Rule owner:
                    // `crate::bot_core::solve_anchor`; after the head floor a hop can
                    // beat the anchor only on a mid-solve state advance (belt +
                    // suspenders, normally unreachable).
                    // Reuse ceiling probe (epic RZRORC last leaf): compare the
                    // hop update-block snapshot against the previous cycle's
                    // recorded one. Byte-identical ⇒ the solve intake (every hop
                    // state) is unchanged since the stored result was produced.
                    // RLVDUP T3: one pool walk builds the snapshot, the
                    // same-state comparison AND the future-price check -
                    // the per-path future probe re-walked all pools.
                    let mut update_snapshot: Vec<u64> = Vec::with_capacity(path.pools.len());
                    let mut future = false;
                    for pool_ref in &path.pools {
                        let ub = core.pool_update_block(pool_ref.pool_key);
                        if anchor.is_future(ub) {
                            future = true;
                        }
                        update_snapshot.push(ub);
                    }
                    let same_state = self
                        .resolved_update_snapshot
                        .get(&path_id)
                        .is_some_and(|prev| *prev == update_snapshot);
                    out.snapshots.push((path_id, update_snapshot));
                    if same_state {
                        out.same_state += 1;
                    }
                    if future {
                        out.deferred.push(path_id);
                        tracing::error!(
                            "[future-price] path_id={path_id} rejected at solve block {solve_block}: \
                             a hop price clock runs AHEAD of the solve block (update_block > \
                             solve_block) — never legitimate"
                        );
                        continue;
                    }
                    let mut resolved = ResolvedMixedPath::default();
                    let mut chunk_projections = out.projections;
                    let deficits = resolve_hops(
                        &core,
                        &path.pools,
                        &mut resolved,
                        &self.hop_projection_cache,
                        Some(&mut chunk_projections),
                        self.cl_projection_memo,
                    );
                    out.projections = chunk_projections;
                    for d in &deficits {
                        *out.invalid_reasons
                            .entry(d.reason.to_string())
                            .or_insert(0u64) += 1u64;
                        tracing::debug!(
                            %path_id,
                            hop_type = ?d.hop_type,
                            pool_key = d.pool_key,
                            reason = %d.reason,
                            "[resolve] path invalid at resolve"
                        );
                    }
                    out.resolved.push((path_id, std::sync::Arc::new(resolved)));
                    // R522XA: drive the path state machine from the full deficit set.
                    out.status.push((path_id, deficits));
                    let _ = chunk_pos;
                }
                out
            };

            // Deterministic chunking: hashbrown iteration order varies per
            // process; a sorted snapshot keeps chunk boundaries (and thus
            // debug-log ordering) identical across runs for ~microsecond cost.
            let mut affected_vec: Vec<u64> = affected_path_ids.iter().copied().collect();
            affected_vec.sort_unstable();
            let chunk_outs: Vec<ResolveChunkOut> = hotpath::measure_block!("resolve.chunks", {
                if !resolve_parallel_enabled() || affected_vec.len() < RESOLVE_PAR_MIN {
                    vec![resolve_chunk(&affected_vec)]
                } else {
                    // P6YXA6: the resolve chunk fan-out leaves rayon with
                    // the hard cutover (the rayon global pool retires).
                    // Chunk boundaries stay byte-identical (the sorted
                    // snapshot chunked at RESOLVE_CHUNK); the parallel
                    // window moves onto short-lived scoped std::threads —
                    // thread t takes chunks t, t+N, … and the index-keyed
                    // collect restores the deterministic merge order.
                    let chunk_ids: Vec<&[u64]> = affected_vec.chunks(RESOLVE_CHUNK).collect();
                    let n_threads =
                        degenbot_core::cpu_budget::solve_worker_count().min(chunk_ids.len());
                    // A panicking child propagates out of `thread::scope` (the
                    // join is implicit) — the loud-failure posture the rayon
                    // join used to have. Results stage behind a mutex AFTER
                    // each chunk resolves (never held during the resolve)
                    // and the index sort restores the deterministic merge
                    // order; slot coverage is structural (round-robin over
                    // the chunk list), so no slot dummy is needed.
                    let staged: std::sync::Mutex<Vec<(usize, ResolveChunkOut)>> =
                        std::sync::Mutex::new(Vec::new());
                    std::thread::scope(|scope| {
                        for t in 0..n_threads {
                            let staged_ref = &staged;
                            let chunk_ids_ref = &chunk_ids;
                            scope.spawn(move || {
                                let local: Vec<(usize, ResolveChunkOut)> = (t..chunk_ids_ref.len())
                                    .step_by(n_threads)
                                    .map(|i| (i, resolve_chunk(chunk_ids_ref[i])))
                                    .collect();
                                let mut ready =
                                    staged_ref.lock().unwrap_or_else(PoisonError::into_inner);
                                ready.extend(local);
                            });
                        }
                    });
                    let mut chunk_pairs =
                        staged.into_inner().unwrap_or_else(PoisonError::into_inner);
                    chunk_pairs.sort_unstable_by_key(|(i, _)| *i);
                    chunk_pairs
                        .into_iter()
                        .map(|(_, out)| out)
                        .collect::<Vec<ResolveChunkOut>>()
                }
            });

            // Serial, deterministic merge (the engine mutex is held by this
            // cycle, so no other task can race these stores).
            let mut same_state_total = 0u64;
            let mut projections_total = 0u64;
            hotpath::measure_block!("resolve.merge", {
                for ResolveChunkOut {
                    resolved,
                    status,
                    snapshots,
                    same_state,
                    projections,
                    invalid_reasons: chunk_invalid,
                    deferred,
                } in chunk_outs
                {
                    same_state_total += same_state;
                    projections_total += projections;
                    deferred_paths.extend(deferred);
                    for (path_id, snapshot) in snapshots {
                        self.resolved_update_snapshot.insert(path_id, snapshot);
                    }
                    for (path_id, arc) in resolved {
                        self.path_resolved.insert(path_id, arc);
                    }
                    for (path_id, deficits) in status {
                        self.path_status
                            .entry(path_id)
                            .or_default()
                            .set_resolved(&deficits);
                    }
                    for (reason, count) in chunk_invalid {
                        *invalid_reasons.entry(reason).or_insert(0u64) += count;
                    }
                }
                self.paths_same_state_this_cycle = same_state_total;
                // Lifetime counter (the serial loop accumulated in place).
                self.hop_projection_count += projections_total;
            });
        });
        // Per-cycle resolve funnel (hotpath_gauge{key=...}). Reason keys come
        // from the closed HopDeficit-reason set, so the series family stays
        // bounded.
        hotpath::gauge!("resolve_paths_affected").set(f64::from(
            u32::try_from(affected_path_ids.len()).unwrap_or(u32::MAX),
        ));
        hotpath::gauge!("resolve_paths_same_state").set(f64::from(
            u32::try_from(self.paths_same_state_this_cycle).unwrap_or(u32::MAX),
        ));
        hotpath::gauge!("resolve_paths_deferred").set(f64::from(
            u32::try_from(deferred_paths.len()).unwrap_or(u32::MAX),
        ));
        // Dynamic-key gauge: the no-op `gauge!` discards its `$key` tokens, so
        // this loop only compiles in instrumented builds (`reason` would be
        // unused otherwise — zero-cost default builds are the design contract).
        #[cfg(feature = "hotpath")]
        for (reason, count) in &invalid_reasons {
            hotpath::gauge!(format!("resolve_invalid_{reason}"))
                .set(f64::from(u32::try_from(*count).unwrap_or(u32::MAX)));
        }
        tracing::info!(
            target: "degenbot::solver",
            block_number = solve_block,
            paths.resolved = affected_path_ids.len(),
            paths.same_state = self.paths_same_state_this_cycle,
            hop.projections = self.hop_projection_count,
            paths.deferred_future_price = deferred_paths.len(),
            invalid.reasons = %invalid_reasons.iter().map(|(r, c)| format!("{c}x {r}")).collect::<Vec<_>>().join(", "),
            phase_us = u64::try_from(cycle_start.elapsed().as_micros()).unwrap_or(u64::MAX),
            "[solve-phase] resolved hop snapshots"
        );
        drop(resolve_ctx);

        // MQUKB6-T2 follow (trace f701ccd36f4ecf80d671e798df218fa4, block
        // 25906841): the window between the close of `arb.resolve` and the
        // open of `arb.lpt` was uninstrumented — 647 ms of wall time on that
        // cold-ramp cycle, ~25 µs/path steady state. The work (results sweep
        // + resolved-snapshot staging) now runs under its own phase span so
        // the pre-LPT cost stays attributable in Jaeger like its fanout/
        // resolve/lpt/merge siblings.
        let stage_span = tracing::info_span!(
            target: "degenbot::solver",
            "degenbot.arb.stage",
            block.number = solve_block,
            paths.affected = affected_path_ids.len(),
            paths.staged = tracing::field::Empty,
        );
        let stage_ctx = stage_span.enter();

        // Remove old results for affected paths (they'll be re-solved below).
        // A deferred path's result is dropped too: it is excluded from this
        // live solve (its pool is stale, so its prior result is stale as well).
        for &path_id in &affected_path_ids {
            self.results.remove(&path_id);
        }

        // Solve only the non-deferred affected set.
        let solve_path_ids: HashSet<u64> = affected_path_ids
            .iter()
            .filter(|&&p| !deferred_paths.contains(&p))
            .copied()
            .collect();

        // Solve affected paths and insert new results.
        //
        // ADR-005 slice 15b-1: the solve fans out across executor bins
        // the affected-path set. `Self::solve_path` is a free-standing dispatch
        // (no `&self` read); each work item takes the `path_id` + an **Arc-
        // shared** `ResolvedMixedPath` snapshot (f701ccd3 staging fix: the
        // former per-path deep clone copied every CL
        // `IntV3TickRangeSequence` every cycle — the "clone is cheap" claim
        // was disproven by telemetry at ~25 µs/path steady state, 150-420
        // µs/path on the cold-heap ramp), `path_resolved` entries being
        // immutable between resolve passes. Workers then write — under the
        // parallel closure — into the engine-level result-set via a
        // `Mutex`-free pattern: collect `(path_id, SolvePathResult)` pairs
        // into a Vec, then merge sequentially into `self.results`. The
        // parallel workers touch NO engine state and NO core.lock —
        // engine-then-core lock ordering is preserved unchanged (the
        // internal thread pool never re-enters the engine `Mutex`). For tiny
        // batches the dispatch overhead is bounded by the lazy
        // split (see `par_iter` docs); the sequential cost dominates below
        // executor internals.
        //
        // Pre-collect the work items (path_id + resolved-snapshot). The Arc
        // clones drop the immutable borrow on `self.path_resolved` that
        // would block parallel dispatch.
        let mut invalid_count: u64 = 0;
        let to_solve: Vec<(u64, std::sync::Arc<ResolvedMixedPath>)> = solve_path_ids
            .iter()
            .filter_map(|&pid| {
                let resolved = self.path_resolved.get(&pid)?;
                if !resolved.valid {
                    invalid_count += 1;
                    return None;
                }
                // A path whose `max_update_block` is AHEAD of the drain
                // `block_number` is LIVE head state (the pools advanced by
                // backfill), not poison — it is correctly re-anchored at
                // `solve_block` above (B2); skipping it would DROP a capturable
                // opportunity. The genuinely-future case (`update_block >
                // solve_block`) is already rejected by the U6RNHH T1 belt-and-
                // suspenders guard in the gate loop above, which removes the
                // path from `solve_path_ids` entirely.
                Some((pid, std::sync::Arc::clone(resolved)))
            })
            .collect();

        drop(stage_ctx);
        stage_span.record("paths.staged", to_solve.len());
        drop(stage_span);

        // Filter out empty/profitless results in the same pass that produces
        // them — the contract is identical to the prior serial loop.
        // D63GSE: per-path wall time is captured so the K slowest paths can be
        // named on the completion event (a min-heap keeps this O(K) memory;
        // the closure itself only does one Instant pair + map insert).
        // (time_us, pieces_visited, path_sims, pid) for the K-slowest
        // attribution — lets the completion event name the walk-combinatorial
        // cost driver of the slowest routes, not just their wall time.
        // -----------------------------------------------------------------
        // BXUSGL T1: per-cycle shared solve context. The pure solver phase
        // is a pure function of the resolved snapshots + this context:
        // workers (the dedicated tokio executor bins) hold Arc
        // CLONES and touch NO engine state, NO core.lock - engine-then-core
        // lock ordering is preserved unchanged. The SINGLE engine-Mutex hold
        // still covers the whole cycle (the drain-side merge happens before
        // this method returns), so cycle atomicity - the results.remove
        // above, pending_new_paths, results_block stamping - is preserved
        // by construction with NO epoch guards.
        // -----------------------------------------------------------------
        let path_times: parking_lot::Mutex<PathTimesHeap> =
            parking_lot::Mutex::new(PathTimesHeap::new());
        let solve_cpu_us: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let walk_pieces_total: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let walk_sims_total: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let walk_word_steps_total: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let walk_refine_sims_total: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let walk_ternary_total: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let walk_grid_total: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Degenerate-path capture config (M6776W): env parsed ONCE per cycle
        // at the owner; the gate itself reads no environment. The prefix-
        // composition cache is generationed by the block epoch inside the
        // gate deps (no public reset to call anymore).
        let gate_capture = gate_capture_from_cfg(&self.cfg);
        // Optional offline CL-solver capture (DEGENBOT_SOLVER_CAPTURE=1): dump
        // the exact all-CL pool state the solver consumed for heavy paths so
        // the CL solver can be optimized offline. None (no-op) unless gated.
        let capture = HeavyClPathCapture::from_capture(&self.cfg.capture);
        // Optional mixed V2+CL solver capture (same gate): heavy
        // mixed paths (e.g. path 7042 V2->V3->V3) dispatch to
        // `exact_solve_mixed_path_n_cached`, which the all-CL capture skips.
        // Defaults OUT of the fixtures dir (loop-18: working rows never
        // accrete there; goldens are produced only by cl_capture_gen).
        let capture_mixed = HeavyMixedPathCapture::from_capture(&self.cfg.capture);
        // SIMPIPE2 T2: pool-ref snapshot aligned to `to_solve` order (the
        // worker clamp's pool list) — captured under this cycle's engine
        // Mutex so it cannot interleave with a re-registration.
        // RLVDUP2 T6: the snapshot is one Arc bump per path - path_pools
        // values are Arc<MixedPath>, immutable between register/deregister.
        let pool_refs: Vec<std::sync::Arc<MixedPath>> = to_solve
            .iter()
            .map(|(pid, _)| {
                self.path_pools
                    .get(pid)
                    .cloned()
                    .unwrap_or_else(|| std::sync::Arc::new(MixedPath { pools: Vec::new() }))
            })
            .collect();
        let shared = std::sync::Arc::new(SolveCycleShared {
            solve_block,
            epoch: solve_block,
            metadata: *metadata,
            gate_capture,
            walk_memo: std::sync::Arc::clone(&self.walk_memo),
            runtime: self.runtime_cfg,
            capture: capture.map(std::sync::Arc::new),
            capture_mixed: capture_mixed.map(std::sync::Arc::new),
            path_times,
            gate_total: parking_lot::Mutex::new(
                ::degenbot_solvers::profit_envelope::GateStats::default(),
            ),
            solve_cpu_us,
            walk_pieces_total,
            walk_sims_total,
            walk_word_steps_total,
            walk_refine_sims_total,
            walk_ternary_total,
            walk_grid_total,
            sims_recorder: std::sync::Arc::clone(&self.last_walk_sims),
            gate_recorder: std::sync::Arc::clone(&self.last_gate_us),
            #[cfg(test)]
            test_solve_delay: self.test_solve_delay.clone(),
            core: std::sync::Arc::clone(self.core()),
            pool_refs,
            worker_clamp: INLINE_SIM_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
            inline_sim: self.inline_sim.clone(),
            sim_fleet_hosted: self.fleet_hosted,
        });
        // The LPT bins are Arc-shared for every arm; the
        // arms index through the same deref (byte-identical semantics).
        let to_solve = std::sync::Arc::new(to_solve);

        // LPT cost + binning shared by the LPT arms (both dispatch modes);
        // called lazily inside the arms so the per-mode hotpath labels
        // keep each arm measured span (a few us of bin-pack included, as
        // before).
        let compute_bins = || {
            // MQUKB6-T2: phase span for the LPT bin-pack (shared by both
            // dispatch arms - and the detached arm's identical binning).
            let _lpt_ctx = tracing::info_span!(
                target: "degenbot::solver",
                "degenbot.arb.lpt",
                paths = to_solve.len(),
            )
            .entered();
            // VPD5ZH: bins follow the cgroup-budget worker count (not the
            // old pool width) so each arm is exactly n_bins tasks
            // on n_workers persistent threads, per the solve_executor doc.
            // P6YXA6 sizing reconciliation: fleet-hosted cycles bin at the
            // fleet's STRUCTURAL seat count — pins and bins are the same
            // number, so every bin owns a warm keyed seat across cycles.
            let n_threads = solve_bin_count(self.fleet_hosted);
            // LW-T7 (Seam F): the bin fan-out goes through the typed runtime
            // fallback decision — a capability narrower than the intended
            // width is a NAMED-AND-LOGGED plan, never a silent narrower bin.
            let plan = plan_bins(n_threads, n_threads);
            let n_threads = plan.bins;
            // Loop-12 KUKHMX: previous-block measured walk sims refine the
            // LPT cost; snapshot once (single lock) before binning.
            // Loop-18: measured gate us rides the same snapshot - gate-heavy
            // paths (dense-CL compose) need the bins to know they are
            // expensive despite sims~0.
            let last_sims_snapshot: HashMap<u64, u64> = shared.sims_recorder.lock().clone();
            let last_gate_snapshot: HashMap<u64, u64> = shared.gate_recorder.lock().clone();
            let costs: Vec<usize> = to_solve
                .iter()
                .map(|(pid, r)| {
                    sims_aware_cost(
                        path_cost_proxy(r),
                        last_sims_snapshot.get(pid).copied(),
                        last_gate_snapshot.get(pid).copied(),
                    )
                })
                .collect();
            lpt_partition(to_solve.len(), n_threads, |i| costs[i])
        };

        // -----------------------------------------------------------------
        // DETACHED arm (epic SRQEK5 WV62TX): under `detached_solving` with
        // in-flight backpressure satisfied (< `DETACHED_INFLIGHT_CAP`
        // un-merged stragglers), enqueue the SOLVES on a plain std::thread
        // per LPT bin — since the P6YXA6 hard cutover those bins ride the
        // fleet executor (fleet.stance=fleet) or the dedicated tokio solve
        // executor, never a scoped install — and RETURN at
        // enqueue end. Every result flows through the unbounded mpsc to the
        // merge sidecar, which applies the Q1a stale policy under the engine
        // Mutex. The detached arm is INDEPENDENT of the in-cycle dispatch
        // below.
        // -----------------------------------------------------------------
        if self.detached_solving
            && self
                .detached_outstanding
                .load(std::sync::atomic::Ordering::Relaxed)
                < DETACHED_INFLIGHT_CAP
        {
            hotpath::measure_block!("arb_solve.detached_enqueue", {
                self.detached_seq_ctr += 1;
                let cycle_seq = self.detached_seq_ctr;
                self.detached_issued_seq = cycle_seq;
                if self.detached_merge_tx.is_none() {
                    // First detached cycle: open the merge pipe. The sidecar
                    // thread is spawned by EngineStages::solve_dirty right
                    // after this enqueue half returns; the Receiver parks in
                    // the engine until then.
                    let (merge_tx, merge_rx) = std::sync::mpsc::channel();
                    self.detached_merge_tx = Some(merge_tx);
                    *self.detached_merge_rx.lock() = Some(merge_rx);
                }
                // Clone the Sender out so the 'static bin threads never
                // borrow `self` (they outlive the call).
                let merge_tx = if let Some(existing) = &self.detached_merge_tx {
                    existing.clone()
                } else {
                    // unreachable-by-construction (opened above); a vanished
                    // pipe would strand every result, so die loudly.
                    tracing::error!(
                        target: crate::telemetry::DIAGNOSTIC_TARGET,
                        "[detached] merge pipe vanished between open and clone — aborting"
                    );
                    std::process::abort();
                };
                // The Q1a freshness oracle: each item rides the per-hop
                // `pool_update_block` snapshot stamped by THIS cycle's
                // resolve phase (above); the sidecar re-reads the live
                // clocks and drops on ANY mismatch.
                let enqueue_stamps: std::sync::Arc<HashMap<u64, Vec<u64>>> = std::sync::Arc::new(
                    to_solve
                        .iter()
                        .filter_map(|(pid, _)| {
                            self.resolved_update_snapshot
                                .get(pid)
                                .map(|stamp| (*pid, stamp.clone()))
                        })
                        .collect(),
                );
                // LPT binning is shared with the in-cycle arms — the
                // detached arm bins identically (independent of the
                // executor stance, which governs the in-cycle arms only).
                let bins = compute_bins();
                // Copy the cycle metadata out: the 'static bin threads
                // outlive the caller's &BlockMetadata borrow.
                let cycle_metadata = *metadata;
                // The gauge is Arc-shared: bin threads bump it at SEND time
                // (so a bin that dies before sending NEVER leaks a count);
                // the sidecar decrements after each terminal disposition.
                let outstanding_in_bins = std::sync::Arc::clone(&self.detached_outstanding);
                let n_bins = bins.len();
                for (bin_idx, bin) in bins.into_iter().enumerate() {
                    let shared_bin = std::sync::Arc::clone(&shared);
                    let to_solve_bin = std::sync::Arc::clone(&to_solve);
                    let stamps_bin = std::sync::Arc::clone(&enqueue_stamps);
                    let outstanding_bin = std::sync::Arc::clone(&outstanding_in_bins);
                    let tx = merge_tx.clone();
                    let solve_span_bin = solve_span.clone();
                    // ergo INYMDG: bin jobs ride the fleet executor
                    // (fleet.stance=fleet) or the dedicated tokio solve
                    // executor (persistent warm workers, BXUSGL T1). The body
                    // is unchanged; same 'static + Send move semantics, and
                    // concurrent detached cycles share the persistent
                    // worker set instead of forking one thread per bin.
                    let run_bin = move || {
                        // 7LV6VN T5 (pipelined arm): results park until
                        // their sim lands; the walk never waits on a
                        // sim. Sims pace on the budget-derived global
                        // slot pool (sim_slots), so walk + sim demand
                        // never exceeds the CPU quota by construction.
                        let mut held: Vec<DetachedMergeItem> = Vec::new();
                        let mut pending = PipelinedSims::default();
                        for &idx in &bin {
                            let (pid, resolved) = &to_solve_bin[idx];
                            // SIMPIPE2 T2: clamp in the bin thread
                            // (stance-gated; BEFORE the profitless filter
                            // — the profit-clamp recompute can zero a
                            // candidate) so the committed inputs are
                            // merge-ready with no sidecar round-trip.
                            let Some((pid, result, worker_clamp_twins)) =
                                solve_one_path(&shared_bin, &solve_span_bin, *pid, resolved).map(
                                    |(pid, mut r)| {
                                        let twins =
                                            clamp_result_in_worker(&shared_bin, idx, pid, &mut r);
                                        (pid, r, twins)
                                    },
                                )
                            else {
                                continue;
                            };
                            {
                                // The profitless filter runs BEFORE the
                                // sim is scheduled - a clamp-zeroed
                                // candidate never needs its payload.
                                // the sim is scheduled — a clamp-zeroed
                                // candidate never needs its payload (the
                                // legacy path simmed first, then filtered).
                                if result.optimal_input.is_zero() || result.profit.is_zero() {
                                    continue;
                                }
                                if !pending.schedule_one(
                                    &shared_bin,
                                    idx,
                                    pid,
                                    &result,
                                    &solve_span_bin,
                                ) {
                                    // No sim rides this item (no hook /
                                    // clamp off) — flush immediately.
                                    let update_stamp =
                                        stamps_bin.get(&pid).cloned().unwrap_or_default();
                                    held.push(DetachedMergeItem::Solved {
                                        cycle_seq,
                                        solve_block,
                                        metadata: cycle_metadata,
                                        pid,
                                        update_stamp,
                                        result,
                                        worker_clamp_twins,
                                        payload: None,
                                        solve_span: solve_span_bin.clone(),
                                    });
                                    flush_detached_item(
                                        &mut held,
                                        &tx,
                                        &outstanding_bin,
                                        pid,
                                        None,
                                    );
                                    continue;
                                }
                                if !result.solver_pool_states.is_empty() {
                                    tracing::debug!(
                                        "[solver-st] path_id={pid} hops=[{}]",
                                        result.solver_pool_states.join(";")
                                    );
                                }
                                let update_stamp =
                                    stamps_bin.get(&pid).cloned().unwrap_or_default();
                                held.push(DetachedMergeItem::Solved {
                                    cycle_seq,
                                    solve_block,
                                    metadata: cycle_metadata,
                                    pid,
                                    update_stamp,
                                    result,
                                    worker_clamp_twins,
                                    payload: None,
                                    solve_span: solve_span_bin.clone(),
                                });
                                // Fan while walking: send every sim that
                                // landed during this iteration's solve.
                                for (done_pid, payload) in pending.drain_ready() {
                                    flush_detached_item(
                                        &mut held,
                                        &tx,
                                        &outstanding_bin,
                                        done_pid,
                                        payload,
                                    );
                                }
                            }
                        }
                        // Tail: join every outstanding sim and send.
                        if !pending.is_empty() {
                            for (done_pid, payload) in pending.join_all() {
                                flush_detached_item(
                                    &mut held,
                                    &tx,
                                    &outstanding_bin,
                                    done_pid,
                                    payload,
                                );
                            }
                        }
                    };
                    if self.fleet_hosted {
                        // ADR-042 F3: the fleet is the sole executor of
                        // solve bins — this bin submits as a keyed Solver
                        // unit (per-bin pin, per-path streaming preserved).
                        crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor()
                            .spawn(bin_idx, run_bin);
                    } else {
                        // P6YXA6: the executor-stance gate is gone — the
                        // private tokio solve executor hosts every non-fleet
                        // bin (the per-cycle std-thread fallback retired with
                        // the rayon stance it existed for).
                        crate::arb_engine::solve_executor::global_solve_executor().spawn(run_bin);
                    }
                }
                if let Some(p) = crate::instruments::pipeline() {
                    p.set_detached_in_flight(
                        self.detached_outstanding
                            .load(std::sync::atomic::Ordering::Relaxed),
                    );
                }
                hotpath::gauge!("detached_solve_in_flight").set(f64::from(
                    u32::try_from(
                        self.detached_outstanding
                            .load(std::sync::atomic::Ordering::Relaxed),
                    )
                    .unwrap_or(u32::MAX),
                ));
                tracing::info!(
                    target: "degenbot::solver",
                    block_number = solve_block,
                    detached_seq = cycle_seq,
                    detached_bins = n_bins,
                    paths.enqueued = to_solve.len(),
                    paths.invalid = invalid_count,
                    paths.deferred_future_price = deferred_paths.len(),
                    phase_us = u64::try_from(cycle_start.elapsed().as_micros()).unwrap_or(u64::MAX),
                    "[solve-phase] detached cycle enqueued (merge runs on the sidecar)"
                );
            });
            self.results_block = solve_block;
            // ENQUEUE-END return semantics (T2 acceptance: "return is
            // enqueue-end, not apply-end"): the engine Mutex hold ENDS here;
            // the sidecar re-acquires it per merged straggler.
            return;
        }

        // P6YXA6 hard cutover: ONE in-cycle dispatch. The LPT bins ride the
        // fleet-hosted executor under `fleet.stance=fleet`, else the
        // dedicated private tokio runtime — no executor-stance gate
        // (`solve.executor` is retired with the `DEGENBOT_SOLVE_EXECUTOR`
        // loud load error), and the rayon arms are gone.
        let mut clamp_twin_count: u64 = 0;
        let mut solved_count: usize = 0;
        let mut suppressed_count: usize = 0;
        let mut failed_count: usize = 0;
        // Seen-pid ledger (QR3NUS): every outcome names its path, so a
        // duplicate delivery — or a double-count — trips the fuse loudly.
        let mut outcome_pids: HashSet<u64> = HashSet::new();
        hotpath::measure_block!("arb_solve.tokio_solve", {
            // BXUSGL T1: the dedicated executor streams PER-PATH
            // results to the caller result queue - one bin task per
            // persistent worker (no splitting/stealing: RAYPAR T3),
            // and this drain merges each path result CLAMP-AND-ALL
            // as its own solve completes. Fast paths land in
            // `self.results` while heavy bins still run. The engine
            // Mutex stays held by THIS cycle, so merging here cannot
            // overlap the next block cycle; the drain runs on the
            // calling thread (T2 moves it to spawn_blocking for the
            // async seam).
            // ADR-042 F3: under the fleet stance the fleet-hosted
            // executor owns these bins (the same keyed Solver units the
            // detached arm submits — the fleet is the SOLE executor of
            // solve bins); the legacy arm keeps the private runtime.
            let fleet_executor = self
                .fleet_hosted
                .then(crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor);
            let executor = crate::arb_engine::solve_executor::global_solve_executor();
            // QR3NUS (Seam D): the pipe carries one typed `LaneOutcome`
            // per submitted path — a `None` never vanishes on the floor
            // and a panicked bin's undelivered paths arrive as typed
            // `Failed` records.
            let (res_tx, res_rx) = std::sync::mpsc::channel::<LaneOutcome>();
            let bins = compute_bins();
            for (bin_idx, bin) in bins.iter().enumerate() {
                let bin = bin.clone();
                let res_tx = res_tx.clone();
                let shared_bin = std::sync::Arc::clone(&shared);
                let to_solve_bin = std::sync::Arc::clone(&to_solve);
                let solve_span_bin = solve_span.clone();
                // QR3NUS (Seam D): the bin's exact owed-pid list at
                // dispatch — the lane witness uses it to keep outcome
                // accounting exact even when a unit panics mid-bin.
                let lane_pids: Vec<u64> = bin.iter().map(|&i| to_solve[i].0).collect();
                let run_bin = move |lane: &mut SolveLane| {
                    // 7LV6VN T5 (pipelined arm): outcomes park until
                    // their sim lands; the walk never waits on a sim.
                    let mut held: Vec<(u64, Option<SolveArmOutcome>)> = Vec::new();
                    let mut pending = PipelinedSims::default();
                    for &i in &bin {
                        let (pid, resolved) = &to_solve_bin[i];
                        let outcome = solve_one_path(&shared_bin, &solve_span_bin, *pid, resolved)
                            .map(|(pid, mut result)| {
                                // SIMPIPE2 T2: clamp in the worker
                                // (stance-gated) BEFORE the
                                // profitless filter — the
                                // profit-clamp recompute can zero a
                                // candidate, and the filter must see
                                // the commit-ready values.
                                let twins =
                                    clamp_result_in_worker(&shared_bin, i, pid, &mut result);
                                (pid, result, twins)
                            });
                        {
                            // The profitless filter runs BEFORE the sim
                            // is scheduled - a clamp-zeroed candidate
                            // never needs its payload.
                            match outcome {
                                Some((pid, result, twins)) => {
                                    if result.optimal_input.is_zero() || result.profit.is_zero() {
                                        if !result.solver_pool_states.is_empty() {
                                            tracing::debug!(
                                                "[solver-st] path_id={pid} hops=[{}]",
                                                result.solver_pool_states.join(";")
                                            );
                                        }
                                        continue;
                                    }
                                    if !pending.schedule_one(
                                        &shared_bin,
                                        i,
                                        pid,
                                        &result,
                                        &solve_span_bin,
                                    ) {
                                        // No sim rides this item — flush
                                        // immediately.
                                        held.push((pid, Some((pid, result, twins, None))));
                                        flush_tokio_item(&mut held, lane, pid, None);
                                        continue;
                                    }
                                    if !result.solver_pool_states.is_empty() {
                                        tracing::debug!(
                                            "[solver-st] path_id={pid} hops=[{}]",
                                            result.solver_pool_states.join(";")
                                        );
                                    }
                                    held.push((pid, Some((pid, result, twins, None))));
                                    // Fan: flush sims that landed mid-walk.
                                    for (done_pid, payload) in pending.drain_ready() {
                                        flush_tokio_item(&mut held, lane, done_pid, payload);
                                    }
                                }
                                // Same failed-solve stream the legacy arm
                                // sends — a None IS an outcome (QR3NUS):
                                // counted at the drain, never merged; it
                                // carries its pid so accounting stays exact.
                                None => {
                                    lane.suppressed(*pid);
                                }
                            }
                        }
                    }
                    if !pending.is_empty() {
                        for (done_pid, payload) in pending.join_all() {
                            flush_tokio_item(&mut held, lane, done_pid, payload);
                        }
                    }
                };
                let lane_key =
                    SOLVE_BIN_KEY_BASE.saturating_add(u64::try_from(bin_idx).unwrap_or(u64::MAX));
                // The spawn job (QR3NUS decision A): every solve bin runs
                // under the lane witness on BOTH executor arms — the panic
                // stays loud AND typed, the seat survives, and every
                // undelivered path patches onto the pipe as `Failed`. The
                // unit/seat record names the stable pin key at this seam
                // (LW-T2 refines it with the live seat context).
                let spawn_job = move || {
                    let mut lane = SolveLane::new(lane_key, lane_key, lane_pids, res_tx);
                    run_solve_lane(&mut lane, &SeatSurvivesPolicy, run_bin);
                };
                if let Some(fleet) = fleet_executor {
                    fleet.spawn(bin_idx, spawn_job);
                } else {
                    executor.spawn(spawn_job);
                }
            }
            drop(res_tx);
            // MQUKB6-T2: the drain-side merge is its own phase node
            // under the cycle span - fast paths merge here WHILE the
            // executor workers still solve, and `merge.paths` records
            // on completion (handle dropped at scope end, so the node
            // closes with the drain).
            let merge_span = tracing::info_span!(
                target: "degenbot::solver",
                "degenbot.arb.merge",
                merge.paths = tracing::field::Empty,
            );
            let merge_ctx = merge_span.enter();
            while let Ok(item) = res_rx.recv() {
                // QR3NUS fuse: EVERY drained item is exactly one attempted
                // path outcome — solved paths merge; a `Suppressed` None IS
                // an outcome (counted, never merged); a `Failed` arrives
                // typed with its unit + seat payload. No item is ever
                // silently skipped.
                match item {
                    LaneOutcome::Solved((pid, solve_result, worker_clamp_twins, payload)) => {
                        if !outcome_pids.insert(pid) {
                            tracing::error!(
                                target: "degenbot::solver",
                                path_id = pid,
                                "[solve-merge] duplicate lane outcome for path — exactness fuse tripped (QR3NUS)"
                            );
                        }
                        if !solve_result.solver_pool_states.is_empty() {
                            tracing::debug!(
                                "[solver-st] path_id={pid} hops=[{}]",
                                solve_result.solver_pool_states.join(";")
                            );
                        }
                        clamp_twin_count += self.merge_one_result(
                            solve_block,
                            metadata,
                            pid,
                            solve_result,
                            worker_clamp_twins,
                            payload,
                        );
                        solved_count += 1;
                    }
                    LaneOutcome::Suppressed { pid } => {
                        if !outcome_pids.insert(pid) {
                            tracing::error!(
                                target: "degenbot::solver",
                                path_id = pid,
                                "[solve-merge] duplicate suppressed outcome for path — exactness fuse tripped (QR3NUS)"
                            );
                        }
                        suppressed_count += 1;
                    }
                    LaneOutcome::Failed { pid, failure } => {
                        if !outcome_pids.insert(pid) {
                            tracing::error!(
                                target: "degenbot::solver",
                                path_id = pid,
                                "[solve-merge] duplicate failed outcome for path — exactness fuse tripped (QR3NUS)"
                            );
                        }
                        tracing::error!(
                            target: "degenbot::solver",
                            path_id = pid,
                            failure = ?failure,
                            "[solve-merge] path outcome lost to a seat panic — typed failure record (QR3NUS)"
                        );
                        failed_count += 1;
                    }
                }
            }
            drop(merge_ctx);
            merge_span.record("merge.paths", solved_count);
            // LW-T7 (Seam F, promotion gate): the merged drain ASSERTS exact
            // totals — outcomes == submissions, failures and all. The assert
            // IS the gate: a mismatch fails the cycle thread loudly (the
            // promoted parity fixture additionally cross-checks its own
            // solved-vs-suspended accounting against its submissions).
            let drained = solved_count + suppressed_count + failed_count;
            assert_eq!(
                drained,
                to_solve.len(),
                "[solve-merge] outcome accounting undercount — exactness fuse \
                 tripped (QR3NUS/LW-T7): solved {solved_count} + suppressed \
                 {suppressed_count} + failed {failed_count} != submitted {}",
                to_solve.len()
            );
        });
        if let Some(c) = shared.capture.as_ref() {
            tracing::info!(
                target: "degenbot::solver",
                captured = c.count.load(std::sync::atomic::Ordering::Relaxed),
                out = %c.out_path.display(),
                "[solve-capture] heavy all-CL path capture active"
            );
        }

        // Telemetry: pure solver phase done - name the K slowest paths.
        let memo_stats = self.walk_memo.take_stats();
        let gate_tots = *shared.gate_total.lock();
        let slowest: Vec<String> = shared.path_times.lock().iter()
            .map(
                |std::cmp::Reverse((
                    us,
                    pieces,
                    sims,
                    word_steps,
                    refine_sims,
                    gate_us,
                    gate_derive_us,
                    gate_compose_us,
                    gate_search_us,
                    pid,
                ))| {
                    format!(
                        "{pid}:{us}us:sims={sims}:pieces={pieces}:steps={word_steps}:refine={refine_sims}:gate={gate_us}us(g={gate_derive_us}/c={gate_compose_us}/s={gate_search_us})"
                    )
                },
            )
            .collect();
        tracing::info!(
            target: "degenbot::solver",
            block_number = solve_block,
            paths.solved = to_solve.len(),
            paths.invalid = invalid_count,
            solve.cpu_us = shared.solve_cpu_us.load(std::sync::atomic::Ordering::Relaxed),
            walk.pieces = shared.walk_pieces_total.load(std::sync::atomic::Ordering::Relaxed),
            walk.sims = shared.walk_sims_total.load(std::sync::atomic::Ordering::Relaxed),
            walk.steps = shared.walk_word_steps_total.load(std::sync::atomic::Ordering::Relaxed),
            walk.refine_sims = shared.walk_refine_sims_total.load(std::sync::atomic::Ordering::Relaxed),
            walk.ternary = shared.walk_ternary_total.load(std::sync::atomic::Ordering::Relaxed),
            walk.grid = shared.walk_grid_total.load(std::sync::atomic::Ordering::Relaxed),
            gate.derive_us = u64::try_from(gate_tots.derive_ns / 1_000).unwrap_or(u64::MAX),
            gate.compose_us = u64::try_from(gate_tots.compose_ns / 1_000).unwrap_or(u64::MAX),
            gate.search_us = u64::try_from(gate_tots.search_ns / 1_000).unwrap_or(u64::MAX),
            gate.prefix_hits = gate_tots.prefix_hits,
            gate.boundaries_composed = gate_tots.boundaries_composed,
            gate.product_us = u64::try_from(gate_tots.product_ns / 1_000).unwrap_or(u64::MAX),
            gate.merge_selected = gate_tots.merge_selected,
            gate.merge_enum = gate_tots.pairs_enumerated,
            gate.merge_fallbacks = gate_tots.merge_legacy_fallbacks,
            gate.fb_flat = gate_tots.merge_fb_flat,
            gate.fb_b_sign = gate_tots.merge_fb_b_sign,
            gate.fb_y_disorder = gate_tots.merge_fb_y_disorder,
            gate.fb_empty = gate_tots.merge_fb_empty_pieces + gate_tots.merge_fb_empty_selection,
            gate.prune_stage1_us = u64::try_from(gate_tots.prune_stage1_ns / 1_000).unwrap_or(u64::MAX),
            gate.prune_hull_us = u64::try_from(gate_tots.prune_hull_ns / 1_000).unwrap_or(u64::MAX),
            gate.evaluated = gate_tots.evaluated,
            gate.skipped = gate_tots.skipped,
            gate.unsupported = gate_tots.unsupported,
            gate.none_hop_unmapped = gate_tots.none_hop_unmapped,
            gate.none_degenerate = gate_tots.none_degenerate,
            gate.none_overflow = gate_tots.none_overflow,
            gate.min_profit = %min_profit_floor(),
            profitable = solved_count,
            slowest.paths = %slowest.join(","),
            phase_us = u64::try_from(cycle_start.elapsed().as_micros()).unwrap_or(u64::MAX),
            memo.probes = memo_stats.probes,
            memo.hits = memo_stats.hits,
            memo.distinct = memo_stats.distinct,
            memo.cache_plays = memo_stats.cache_plays,
            memo.negative = memo_stats.negative_entries,
            memo.sims = memo_stats.probes_sims,
            memo.hit_sims = memo_stats.hits_sims,
            "[solve-phase] streaming solve complete"
        );

        let clamp_twins_start = std::time::Instant::now();
        // Telemetry: clamp phase done - the twin simulations are a known
        // multi-second contributor on CL-heavy batches, so they get their own
        // line item.
        tracing::info!(
            target: "degenbot::solver",
            block_number = solve_block,
            clamp.paths = solved_count,
            clamp.twins = clamp_twin_count,
            clamp.phase_us = u64::try_from(clamp_twins_start.elapsed().as_micros()).unwrap_or(u64::MAX),
            total_us = u64::try_from(cycle_start.elapsed().as_micros()).unwrap_or(u64::MAX),
            inline.stance = INLINE_SIM_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
            inline.hook = self.inline_sim.is_some(),
            inline.payloads = self.inline_payloads.len(),
            solve.entry = self.solve_entry,
            "[solve-phase] cycle complete (clamp done)"
        );

        self.results_block = solve_block;
        // Note: no compute_diff_and_send here — the pump controls when
        // batches are dispatched (debounce timer or block boundary).
    }

    /// Solve all registered paths using `solve_path`.
    ///
    /// `solve_all` is not currently used live — the pump calls
    /// `solve_all_paths` which calls this only at cold start; subsequent
    /// re-solves go through `rebuild_and_solve_affected`.
    ///
    /// P6YXA6 hard cutover: the cold start rides the SAME executors as the
    /// in-cycle arms — the fleet-hosted Solver pins under
    /// `fleet.stance=fleet`, else the dedicated private tokio runtime —
    /// with LPT binning over the structural bin count kept. Bins are
    /// 'static closures over Arc-cloned state: they take NO engine lock
    /// (engine-then-core invariant intact), stream each profitable result
    /// over an mpsc as its OWN solve completes, and the caller drains the
    /// pipe into the fresh result map. Bin jobs clamp each result against
    /// the pool state (UO3JM4) exactly as the merge-site clamp did.
    #[must_use]
    pub fn solve_all(&self) -> HashMap<u64, SolvePathResult> {
        // MQUKB6-T0: same span-context re-entry as rebuild_and_solve_affected:
        // bin jobs re-enter this cycle span per work item, so per-path child
        // spans parent under the cold-start cycle instead of forking roots.
        let solve_span = tracing::Span::current();

        // Pre-collect work items (path_id + Arc-shared resolved). The Arc
        // clones drop the immutable borrow on self.path_resolved so the
        // 'static bin jobs don't capture &self at all (f701ccd3 staging fix:
        // Arc clones are refcount bumps, not deep clones of the CL
        // tick-range sequences).
        let to_solve: Vec<(u64, std::sync::Arc<ResolvedMixedPath>)> = self
            .path_resolved
            .iter()
            .filter(|(_, r)| r.valid)
            .map(|(&pid, r)| (pid, std::sync::Arc::clone(r)))
            .collect();

        // RAYPAR T3: LPT-pre-balanced partition. The cold start has the
        // same cost skew as the hot path, so it bins over the structural
        // bin count too — the fleet's Solver seats when fleet-hosted
        // (pins == bins), else solve_worker_count's dedicated-runtime bins.
        let n_bins = solve_bin_count(self.fleet_hosted);
        // Cold start has no previous-block sims/gate yet: structural proxy only.
        let costs: Vec<usize> = to_solve
            .iter()
            .map(|(_, r)| sims_aware_cost(path_cost_proxy(r), None, None))
            .collect();
        let bins = lpt_partition(to_solve.len(), n_bins, |i| costs[i]);

        // Bin jobs are 'static over Arc-cloned state: walk memo, core and
        // the pool-ref map for the UO3JM4 clamp. No engine state is touched
        // (engine-then-core invariant intact; the mixer only reads core).
        let memo = std::sync::Arc::clone(&self.walk_memo);
        let path_pools: HashMap<u64, std::sync::Arc<MixedPath>> = self.path_pools.clone();
        let core = std::sync::Arc::clone(self.core());
        let results_block = self.results_block;
        let runtime_cfg = self.runtime_cfg;
        let (tx, rx) = std::sync::mpsc::channel::<(u64, SolvePathResult)>();
        for (bin_idx, bin) in bins.iter().enumerate() {
            let bin = bin.clone();
            let to_solve_bin = to_solve.clone();
            let memo = std::sync::Arc::clone(&memo);
            let path_pools = path_pools.clone();
            let core = std::sync::Arc::clone(&core);
            let tx = tx.clone();
            let solve_span_bin = solve_span.clone();
            let run_bin = move || {
                // Cold start: no capture wiring — deps with the
                // registered-epoch guard + the engine walk-memo handle.
                let mut gate_deps = ::degenbot_solvers::profit_envelope::GateDeps::per_block_with(
                    results_block,
                    None,
                    runtime_cfg,
                );
                gate_deps.walk_memo = Some(&memo);
                for &i in &bin {
                    let (path_id, resolved) = &to_solve_bin[i];
                    let _solve_ctx = solve_span_bin.enter();
                    if let Some(mut r) = ::degenbot_solvers::mixed::solve_path_with_min_profit(
                        resolved,
                        min_profit_floor(),
                        &gate_deps,
                    )
                    .result
                    .filter(|r| !r.optimal_input.is_zero() && !r.profit.is_zero())
                    .inspect(|r| {
                        if !r.solver_pool_states.is_empty() {
                            tracing::debug!(
                                "[solver-st] path_id={path_id} hops=[{}]",
                                r.solver_pool_states.join(";")
                            );
                        }
                    }) {
                        if let Some(path) = path_pools.get(path_id) {
                            let core_read = core.read();
                            let _ = Self::clamp_result_with_state(
                                &core_read,
                                *path_id,
                                &path.pools,
                                &mut r,
                            );
                        }
                        let _ = tx.send((*path_id, r));
                    }
                }
            };
            if self.fleet_hosted {
                crate::arb_engine::fleet_solve_executor::global_fleet_solve_executor()
                    .spawn(bin_idx, run_bin);
            } else {
                crate::arb_engine::solve_executor::global_solve_executor().spawn(run_bin);
            }
        }
        drop(tx);
        rx.into_iter().collect()
    }
}

impl Default for ArbitrageEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// One-shot capture of the exact all-CL solver input for heavy paths, so the
/// CL solver (`int_solve_cl_path` / active-set walk) can be optimized against
/// real captured pool state offline, without a full bot run.
///
/// Gated by `DEGENBOT_SOLVER_CAPTURE=1` (`from_env` yields `None` otherwise).
/// For each heavy all-CL path (the first `DEGENBOT_SOLVER_CAPTURE_CAP`, deduped
/// by path id, heavy = `time_us >= MIN_US` or `sims >= MIN_SIMS`) it appends one
/// JSON line to `DEGENBOT_SOLVER_CAPTURE_OUT` with the per-hop
/// `IntV3TickRangeSequence` ranges, the measured (time, walk sims, pieces), and
/// the golden result - so the offline replay harness asserts determinism.
struct HeavyClPathCapture {
    min_us: u64,
    min_sims: u64,
    max_captures: u64,
    out_path: std::path::PathBuf,
    seen: std::sync::Mutex<std::collections::HashSet<u64>>,
    count: std::sync::atomic::AtomicU64,
}

impl HeavyClPathCapture {
    fn from_capture(capture: &::degenbot_config::schema::CaptureConfig) -> Option<Self> {
        capture.solver_capture.then_some(()).map(|()| Self {
            min_us: capture.solver_capture_min_us,
            min_sims: capture.solver_capture_min_sims,
            max_captures: u64::try_from(capture.solver_capture_cap).unwrap_or(u64::MAX),
            out_path: match capture.solver_capture_out.clone() {
                Some(p) => p,
                None => {
                    // Loop-18: production captures are WORKING rows (state and
                    // recorded answer come from different contexts) — they
                    // must NEVER accrete into the exact-wei fixtures: that
                    // accretion (513 null-golden rows, 9 stale epochs) was
                    // what red the F2 gate pre-re-anchor. Default out of the
                    // fixtures dir; exact-wei goldens are produced ONLY by
                    // cl_capture_gen (see its doc: the sanctioned producer).
                    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../../logs/solver_capture/cl_heavy_paths.jsonl")
                }
            },
            seen: std::sync::Mutex::new(std::collections::HashSet::new()),
            count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Append the CL pool state for a resolved heavy path, if it is a heavy,
    /// not-yet-captured all-CL path.
    // The capture record is a flat diagnostic tuple; a params struct would
    // obscure the field-for-field mapping to the JSONL schema.
    #[expect(clippy::too_many_arguments)]
    fn maybe_capture(
        &self,
        pid: u64,
        block: u64,
        micros_us: u64,
        sims: u64,
        pieces: u64,
        golden: Option<&SolvePathResult>,
        resolved: &ResolvedMixedPath,
    ) {
        if self.count.load(std::sync::atomic::Ordering::Relaxed) >= self.max_captures {
            return;
        }
        if micros_us < self.min_us && sims < self.min_sims {
            return;
        }
        // Must be a pure-CL path (every hop resolves to an int sequence, at
        // least 2 hops) to replay `int_solve_cl_path` directly offline.
        if resolved.hops.len() < 2 || !resolved.hops.iter().all(|h| h.as_int_sequence().is_some()) {
            return;
        }
        let Ok(mut seen) = self.seen.lock() else {
            return;
        };
        if !seen.insert(pid) {
            return;
        }
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Per path hop -> its `IntV3TickRangeSequence.ranges`; each range as the
        // 8 primitive fields (big ints as decimal strings, so no alloy serde).
        let hops = resolved
            .hops
            .iter()
            .filter_map(|h| {
                // Every hop carries an int sequence (checked above), so this
                // never drops an element — the `?` just satisfies the type
                // checker without an `unwrap`.
                Some(
                    h.as_int_sequence()?
                        .ranges
                        .iter()
                        .map(|r| {
                            serde_json::json!({
                                "liquidity": r.liquidity.to_string(),
                                "sqrt_price_x96": r.sqrt_price_x96.to_string(),
                                "sqrt_price_lower_x96": r.sqrt_price_lower_x96.to_string(),
                                "sqrt_price_upper_x96": r.sqrt_price_upper_x96.to_string(),
                                "gamma_numer": r.gamma_numer,
                                "fee_denom": r.fee_denom,
                                "zero_for_one": r.zero_for_one,
                                "word_boundary_prices": r.word_boundary_prices
                                    .iter()
                                    .map(std::string::ToString::to_string)
                                    .collect::<Vec<_>>(),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let golden_json = golden.map(|g| {
            serde_json::json!({
                "optimal_input": g.optimal_input.to_string(),
                "profit": g.profit.to_string(),
                "hop_outputs": g.hop_outputs.iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
            })
        });
        let doc = serde_json::json!({
            "path_id": pid,
            "block": block,
            "n_hops": resolved.hops.len(),
            "hops": hops,
            "measured": { "time_us": micros_us, "sims": sims, "pieces": pieces },
            "golden": golden_json,
        });
        if let Some(parent) = self.out_path.parent() {
            // The default OUT path lives under logs/solver_capture/ — a
            // directory that only exists if someone created it. A missing
            // parent previously failed every append SILENTLY (the in-process
            // `captured` counter kept advancing), losing the whole run's
            // corpus.
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.out_path)
        {
            use std::io::Write;
            let _ = writeln!(f, "{doc}");
        }
    }
}

/// One-shot capture of heavy *mixed* V2+CL solver inputs (the sibling of
/// [`HeavyClPathCapture`] for paths that dispatch to
/// `exact_solve_mixed_path_n_cached`). Records the V2 `IntHopState` per V2 hop
/// and the `IntV3TickRangeSequence` ranges per CL hop (plus `hop_order`) so
/// `examples/mixed_solve_replay.rs` can reconstruct the exact solver call,
/// assert golden determinism, and profile the bottleneck offline.
///
/// Gated by the same `DEGENBOT_SOLVER_CAPTURE=1` env. Writes to
/// `heavy_mixed_solve_captures.jsonl` (override via
/// `DEGENBOT_SOLVER_CAPTURE_OUT`). Captures only paths that mix ≥1 V2 and
/// ≥1 CL hop; all-CL and all-V2 paths are left to the existing captures.
struct HeavyMixedPathCapture {
    min_us: u64,
    min_sims: u64,
    max_captures: u64,
    out_path: std::path::PathBuf,
    seen: std::sync::Mutex<std::collections::HashSet<u64>>,
    count: std::sync::atomic::AtomicU64,
}

impl HeavyMixedPathCapture {
    fn from_capture(capture: &::degenbot_config::schema::CaptureConfig) -> Option<Self> {
        capture.solver_capture.then_some(()).map(|()| Self {
            min_us: capture.solver_capture_min_us,
            min_sims: capture.solver_capture_min_sims,
            max_captures: u64::try_from(capture.solver_capture_cap).unwrap_or(u64::MAX),
            out_path: match capture.solver_capture_out.clone() {
                Some(p) => {
                    // If the caller overrides the out path for both captures,
                    // disambiguate the mixed corpus into a sibling filename
                    // rather than overwriting the all-CL fixture.
                    let mut pb = p;
                    if pb.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                        if let Some(stem) = pb.file_stem().and_then(|s| s.to_str()) {
                            pb.set_file_name(format!("{stem}_mixed.jsonl"));
                        }
                    }
                    pb
                }
                None => {
                    // Loop-18: mixed captures default OUT of the fixtures dir
                    // (working rows; see the Cl-side comment).
                    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../../logs/solver_capture/cl_mixed_paths.jsonl")
                }
            },
            seen: std::sync::Mutex::new(std::collections::HashSet::new()),
            count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Append the mixed V2+CL solver input for a resolved heavy path, iff it
    /// is a mixed (≥1 V2 and ≥1 CL) not-yet-captured path.
    #[expect(clippy::too_many_arguments)]
    fn maybe_capture(
        &self,
        pid: u64,
        block: u64,
        micros_us: u64,
        sims: u64,
        pieces: u64,
        golden: Option<&SolvePathResult>,
        resolved: &ResolvedMixedPath,
    ) {
        if self.count.load(std::sync::atomic::Ordering::Relaxed) >= self.max_captures {
            return;
        }
        if micros_us < self.min_us && sims < self.min_sims {
            return;
        }
        if resolved.hops.len() < 2 {
            return;
        }
        // Only mixed paths: ≥1 V2 hop AND ≥1 CL hop. The all-CL capture owns
        // pure-CL; all-V2 dispatches to the closed-form Möbius solver.
        let has_v2 = resolved
            .hops
            .iter()
            .any(|h| matches!(h, ResolvedHop::V2 { .. }));
        let has_cl = resolved.hops.iter().any(|h| h.as_int_sequence().is_some());
        if !has_v2 || !has_cl {
            return;
        }
        let Ok(mut seen) = self.seen.lock() else {
            return;
        };
        if !seen.insert(pid) {
            return;
        }
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Per-hop serialization: a `kind` discriminant + the hop's raw fields.
        // V2 → reserve_in/out + gamma + fee_denom (decimal strings, no alloy
        // serde). CL → the same `IntV3TickRangeSequence.ranges` shape the
        // all-CL fixture uses, so the replay harness shares a CL-range parser.
        let hop_order: Vec<bool> = resolved
            .hops
            .iter()
            .map(|h| matches!(h, ResolvedHop::V2 { .. }))
            .collect();
        let hops = resolved
            .hops
            .iter()
            .map(|h| match h {
                ResolvedHop::V2 { state } => serde_json::json!({
                    "kind": "V2",
                    "reserve_in": state.reserve_in.to_string(),
                    "reserve_out": state.reserve_out.to_string(),
                    "gamma_numer": state.gamma_numer.to_string(),
                    "fee_denom": state.fee_denom.to_string(),
                }),
                ResolvedHop::V3 { int_seq, .. } | ResolvedHop::V4 { int_seq, .. } => {
                    serde_json::json!({
                        "kind": "CL",
                        "ranges": int_seq.ranges.iter().map(|r| serde_json::json!({
                            "liquidity": r.liquidity.to_string(),
                            "sqrt_price_x96": r.sqrt_price_x96.to_string(),
                            "sqrt_price_lower_x96": r.sqrt_price_lower_x96.to_string(),
                            "sqrt_price_upper_x96": r.sqrt_price_upper_x96.to_string(),
                            "gamma_numer": r.gamma_numer,
                            "fee_denom": r.fee_denom,
                            "zero_for_one": r.zero_for_one,
                            "word_boundary_prices": r.word_boundary_prices
                                .iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
                        })).collect::<Vec<_>>(),
                    })
                }
                _ => serde_json::Value::Null,
            })
            .collect::<Vec<_>>();
        let golden_json = golden.map(|g| {
            serde_json::json!({
                "optimal_input": g.optimal_input.to_string(),
                "profit": g.profit.to_string(),
                "hop_outputs": g.hop_outputs.iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
            })
        });
        let doc = serde_json::json!({
            "path_id": pid,
            "block": block,
            "n_hops": resolved.hops.len(),
            "hop_order": hop_order,
            "hops": hops,
            "measured": { "time_us": micros_us, "sims": sims, "pieces": pieces },
            "golden": golden_json,
        });
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.out_path)
        {
            use std::io::Write;
            let _ = writeln!(file, "{doc}");
        }
    }
}

#[cfg(test)]
mod profit_clamp_recompute_tests {
    #![expect(clippy::expect_used)] // tests assert recompute invariants
    use super::clamp_result_in_worker;
    use super::{
        ArbitrageEngine, BlockMetadata, HashMap, PathTimesHeap, SolveCycleShared, SolvePathResult,
        U256,
    };
    use crate::bot_core::{TickInfo, V4PoolKey};
    use degenbot_solvers::mixed::MixedPath;
    use std::sync::Arc;

    /// Path-142603 (V4-V4-V3 @25723658) regression: the solver reported a
    /// phantom +346,369,630 wei profit because its V3 hop2 output
    /// (351,476,391,576,684) over-predicted the byte-exact twin
    /// (351,475,872,056,229) by 519,520,455 wei. After the CL clamp aligns
    /// `hop_outputs`/`consumed_inputs` to the twin, the selection profit MUST
    /// be recomputed from the clamped values: the round trip nets
    /// -173,150,825 wei -> saturates to 0 -> dropped by the `profit > min_profit`
    /// delivery gate instead of being selected and executing to a `no-profit`
    /// trap. (Regression for the BUG-B fix in `clamp_cl_hop_capacity`.)
    #[test]
    fn post_clamp_last_hop_loss_saturates_profit_to_zero() {
        let mut r = SolvePathResult {
            optimal_input: U256::from(351_476_045_207_054u64),
            // Profit the SOLVER computed on its over-predicted raw hop2 output
            // (= 351_476_391_576_684 - 351_476_045_207_054 = +346,369,630).
            profit: U256::from(346_369_630u64),
            // Twin-aligned outputs after the CL clamp: hop2 (last) clamped
            // DOWN to the byte-exact twin 351,475,872,056,229.
            hop_outputs: vec![
                U256::from(676_293u64),
                U256::from(676_607u64),
                U256::from(351_475_872_056_229u64),
            ],
            consumed_inputs: vec![U256::from(351_476_045_207_054u64)],
            ..Default::default()
        };
        let recomputed = ArbitrageEngine::recompute_clamped_profit(&r).expect("has outputs");
        // final_output - consumed_inputs[0] = -173,150,825 -> saturating 0.
        assert_eq!(recomputed, U256::ZERO, "post-clamp loss must saturate to 0");
        // The clamp writes the recomputed value back (the fix).
        r.profit = recomputed;
        assert!(
            r.profit.is_zero(),
            "selection profit must be zero (dropped)"
        );
    }

    /// The recompute is a no-op safety for a genuinely-profitable path whose
    /// outputs were twin-aligned with no net change: profit is preserved.
    #[test]
    fn genuine_profit_preserved_after_clamp() {
        let r = SolvePathResult {
            optimal_input: U256::from(1000u64),
            profit: U256::from(50u64),
            hop_outputs: vec![U256::from(200u64), U256::from(1050u64)],
            consumed_inputs: vec![U256::from(1000u64), U256::from(200u64)],
            ..Default::default()
        };
        let recomputed = ArbitrageEngine::recompute_clamped_profit(&r).expect("has outputs");
        assert_eq!(
            recomputed,
            U256::from(50u64),
            "genuine profit must be preserved"
        );
    }

    /// `profit = final_output - consumed_inputs[0]` (the documented semantics):
    /// a first hop that partial-fills at a range boundary consumes less than the
    /// full `optimal_input`, so the recompute must key off `consumed_inputs[0]`.
    #[test]
    fn recompute_uses_consumed_inputs_zero_not_optimal_input() {
        let r = SolvePathResult {
            optimal_input: U256::from(1000u64),
            profit: U256::from(0u64),
            hop_outputs: vec![U256::from(300u64), U256::from(1050u64)],
            // hop0 consumes 900, not the full 1000 (partial fill at boundary).
            consumed_inputs: vec![U256::from(900u64), U256::from(300u64)],
            ..Default::default()
        };
        let recomputed = ArbitrageEngine::recompute_clamped_profit(&r).expect("has outputs");
        assert_eq!(
            recomputed,
            U256::from(150u64),
            "1050 - 900, not 1050 - 1000"
        );
    }

    /// A degenerate path (no hop outputs / consumed inputs) recomputes to None
    /// and is left untouched by the clamp.
    #[test]
    fn degenerate_path_returns_none() {
        let r = SolvePathResult::default();
        assert!(ArbitrageEngine::recompute_clamped_profit(&r).is_none());
    }

    // ---------------- SIMPIPE2 T2 acceptance (task PIRX3W) ----------------

    /// Narrow single-position V4 pool (±60 ticks, 1e6 liquidity) + a one-hop
    /// path: the over-fed committed input is the empty-march class. Returns
    /// (engine, `path_id`, the to_solve-aligned pool-ref snapshot).
    fn overfed_v4_engine() -> (ArbitrageEngine, u64, Vec<std::sync::Arc<MixedPath>>) {
        use crate::arb_engine::PoolTickCoverage;
        use crate::bot_core::RegisterV4PoolParams;
        fn usdc_local(amount: u64) -> alloy::primitives::Uint<112, 2> {
            (U256::from(amount) * U256::from(10u64).pow(U256::from(6)))
                .to::<alloy::primitives::Uint<112, 2>>()
        }
        fn weth_local(amount: u64) -> alloy::primitives::Uint<112, 2> {
            (U256::from(amount) * U256::from(10u64).pow(U256::from(18)))
                .to::<alloy::primitives::Uint<112, 2>>()
        }
        const GAMMA_03: u64 = 997;
        const FEE_DENOM_03: u64 = 1000;
        let mut engine = ArbitrageEngine::new();
        // V2 pool: large reserves so its output dwarfs the V4 hop's capacity —
        // the V4 hop is the over-fed one (this isolates hop1's input clamp).
        let v2 = engine.register_v2_pool(
            alloy::primitives::Address::from([0x11u8; 20]),
            usdc_local(1_500_000),
            weth_local(20_000_000_000),
            GAMMA_03,
            FEE_DENOM_03,
        );
        let mut tick_data = HashMap::new();
        tick_data.insert(
            60,
            TickInfo {
                liquidity_gross: alloy::primitives::U128::from(300),
                liquidity_net: 150i128,
                block: 0,
            },
        );
        tick_data.insert(
            -60,
            TickInfo {
                liquidity_gross: alloy::primitives::U128::from(200),
                liquidity_net: -100i128,
                block: 0,
            },
        );
        let v4_id = engine
            .register_v4_pool(&RegisterV4PoolParams {
                pool_manager: alloy::primitives::Address::from([0x44u8; 20]),
                pool_id: [0xabu8; 32],
                pool_key: V4PoolKey {
                    currency0: alloy::primitives::Address::from([0x30u8; 20]),
                    currency1: alloy::primitives::Address::from([0x31u8; 20]),
                    fee: 500,
                    tick_spacing: 10,
                    hooks: alloy::primitives::Address::ZERO,
                },
                hook_flags: 0,
                protocol_fee: 0,
                sqrt_price_x96: U256::from(1u128) << 96,
                liquidity: 1_000_000,
                tick: 0,
                tick_data,
                update_block: 0,
                tick_data_block: None,
                coverage: PoolTickCoverage::Tracked,
                fetcher: None,
            })
            .expect("V4 registration failed");
        let path_id = engine
            .register_path(vec![
                ::degenbot_solvers::mixed::PoolHop {
                    pool_id: v2,
                    zero_for_one: true,
                },
                ::degenbot_solvers::mixed::PoolHop {
                    pool_id: v4_id,
                    zero_for_one: false,
                },
            ])
            .expect("two-hop path registers");
        let pool_refs =
            std::iter::once(engine.path_pools.get(&path_id).expect("registered").clone())
                .collect::<Vec<_>>();
        (engine, path_id, pool_refs)
    }

    fn worker_probe_ctx(
        core: Arc<crate::bot_core::state_lock::StateLock<crate::bot_core::BotState>>,
        pool_refs: Vec<std::sync::Arc<MixedPath>>,
    ) -> Arc<SolveCycleShared> {
        Arc::new(SolveCycleShared {
            core,
            pool_refs,
            worker_clamp: true,
            inline_sim: None,
            sim_fleet_hosted: false,
            solve_block: 0,
            epoch: 0,
            metadata: BlockMetadata::default(),
            runtime: ::degenbot_solvers::runtime::SolveRuntimeConfig::default(),
            gate_capture: None,
            walk_memo: Arc::new(::degenbot_solvers::mobius_v3_int::WalkMemo::new(
                false, false,
            )),
            capture: None,
            capture_mixed: None,
            path_times: parking_lot::Mutex::new(PathTimesHeap::new()),
            gate_total: parking_lot::Mutex::new(
                ::degenbot_solvers::profit_envelope::GateStats::default(),
            ),
            solve_cpu_us: std::sync::atomic::AtomicU64::new(0),
            walk_pieces_total: std::sync::atomic::AtomicU64::new(0),
            walk_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_word_steps_total: std::sync::atomic::AtomicU64::new(0),
            walk_refine_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_ternary_total: std::sync::atomic::AtomicU64::new(0),
            walk_grid_total: std::sync::atomic::AtomicU64::new(0),
            sims_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            gate_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            test_solve_delay: None,
        })
    }

    /// The engine clamp and the WORKER clamp are the same computation from
    /// two call sites: byte-identical result + twin count on identical input.
    #[test]
    fn worker_clamp_matches_engine_clamp_bit_for_bit() {
        use ::degenbot_solvers::mixed::MixedPoolRef;
        let (engine, path_id, pool_refs) = overfed_v4_engine();
        let mk = || {
            let committed = U256::from(1u128) << 120;
            SolvePathResult {
                optimal_input: U256::from(1_000_000_000u64),
                profit: U256::from(1_000u64),
                hop_outputs: vec![committed, committed],
                consumed_inputs: vec![committed, committed],
                state_nonces: vec![],
                solver_pool_states: Vec::new(),
            }
        };
        let (mut r_engine, mut r_worker) = (mk(), mk());
        let twins_engine = engine.clamp_cl_hop_capacity(path_id, &mut r_engine);
        assert!(twins_engine > 0, "premise: the over-fed input must clamp");
        assert!(
            r_engine.consumed_inputs[1] < U256::from(1u128) << 120,
            "premise: the V4 hop input clamp fired"
        );
        let ctx = worker_probe_ctx(Arc::clone(engine.core()), pool_refs);
        let twins_worker = clamp_result_in_worker(&ctx, 0, path_id, &mut r_worker);
        assert_eq!(twins_worker, twins_engine, "twin count must match");
        assert_eq!(r_engine, r_worker, "clamped result must be byte-identical");
        // The pool-ref SNAPSHOT path (worker side) is exercised; the MixedPoolRef _ unused is intentional.
        let _: Vec<Vec<MixedPoolRef>> = Vec::new();
    }

    // The merge honors the worker's twin report: twins > 0 = the result is
    // already clamp-committed (no second clip); twins = 0 = the merge clips
    // the over-fed input itself (the legacy path — bit-identical).

    /// SIMPIPE2 T3: a payload riding `merge_one_result` is stored at the
    /// engine (`inline_payloads`) and a re-merge WITHOUT the payload drops the
    /// stale entry — per-entry presence decides Python-side. (The delivery
    /// drain into `ResultBatch.payloads` is covered by the `delivery_policy`
    /// tests + the FFI conversion; this pins the merge-site store/drop.)
    #[test]
    fn merge_stores_payload_and_drops_it_without_one() {
        use crate::arb_engine::inline_sim::{InlineSwapFamily, SimulatedPathResult};
        use alloy::primitives::{Address, I256, U256};

        let (mut engine, path_id, _pool_refs) = overfed_v4_engine();
        let metadata = BlockMetadata::default();
        let mk = || SolvePathResult {
            optimal_input: U256::from(1_000_000_000u64),
            profit: U256::from(1_000u64),
            hop_outputs: vec![U256::from(1u64)],
            consumed_inputs: vec![U256::from(1u64)],
            state_nonces: vec![0],
            solver_pool_states: Vec::new(),
        };
        let payload = SimulatedPathResult {
            path_id,
            gross_profit: U256::from(1_000u64),
            net_profit: U256::from(900u64),
            gas_used: 300_000,
            priority_fee: 2,
            base_fee_next: 30,
            execute_calldata: vec![1, 2, 3],
            access_list: None,
            captured_swaps: vec![crate::arb_engine::inline_sim::CapturedSwapRow {
                emitter: Address::from([0x11u8; 20]),
                family: InlineSwapFamily::V4,
                amount0: I256::MINUS_ONE,
                amount1: I256::ONE,
                sqrt_price_x96: U256::ZERO,
                liquidity: U256::ZERO,
                tick: 0,
            }],
            hop_count: 1,
            failure: None,
        };

        engine.merge_one_result(42, &metadata, path_id, mk(), 0, Some(payload));
        assert!(
            engine.inline_payloads.contains_key(&path_id),
            "the payload must be stored at merge"
        );

        // The path re-solves WITHOUT a payload (stance off or hook silence):
        // the stale entry must drop — presence decides per entry.
        engine.merge_one_result(43, &metadata, path_id, mk(), 0, None);
        assert!(
            !engine.inline_payloads.contains_key(&path_id),
            "a payload-less re-merge must drop the stale payload"
        );
    }

    #[test]
    fn merge_reports_worker_twins_and_never_reclips() {
        let (mut engine, path_id, pool_refs) = overfed_v4_engine();
        let metadata = BlockMetadata::default();
        let overfed = || {
            let committed = U256::from(1u128) << 120;
            SolvePathResult {
                optimal_input: U256::from(1_000_000_000u64),
                profit: U256::from(1_000u64),
                hop_outputs: vec![committed, committed],
                consumed_inputs: vec![committed, committed],
                state_nonces: vec![],
                solver_pool_states: Vec::new(),
            }
        };

        // Worker arm: clamp once (the worker report = committed truth), then
        // merge with twins > 0 — the stored result stays byte-identical.
        let mut worker_result = overfed();
        let ctx = worker_probe_ctx(Arc::clone(engine.core()), pool_refs);
        let twins = clamp_result_in_worker(&ctx, 0, path_id, &mut worker_result);
        assert!(twins > 0, "premise: worker clamp fired");
        let committed = worker_result.clone();
        engine.merge_one_result(42, &metadata, path_id, worker_result, twins, None);
        {
            let stored = engine.results.get(&path_id).expect("worker-merged");
            assert_eq!(
                stored.consumed_inputs, committed.consumed_inputs,
                "twins>0 must not re-clip the committed inputs"
            );
            assert_eq!(stored.profit, committed.profit, "profit untouched on skip");
        }

        // Legacy arm (twins=0): the merge clips the over-fed V4 hop input
        // itself (index 1 — the V2 hop has no input clamp by design).
        let legacy = overfed();
        let pre = legacy.consumed_inputs[1];
        engine.merge_one_result(42, &metadata, path_id, legacy, 0, None);
        let stored = engine.results.get(&path_id).expect("legacy-merged");
        assert_ne!(
            stored.consumed_inputs[1], pre,
            "twins=0 must run the merge-site clamp"
        );
    }

    // ----------------- RKXN5Z / IJUBV3: bundle.simulate span hygiene -----------------

    /// RED-gate (IJUBV3): the merge-site microsecond `degenbot.bundle.simulate`
    /// "verdict bookmark" spans collided with the REAL per-path EVM sim spans
    /// of the same name (traces 98f7cf52 / ab13f75fad50: 90-300 markers per
    /// block drowned the ms-scale sims). The merge must create NO span with
    /// that name - the verdict is an `info!` event on the enclosing merge
    /// span, and the span name now belongs solely to simulation work.
    ///
    /// DEFAULT-GATE VISIBLE (no otel cfg), on the K4ETHF pattern: the marker
    /// flood was what made Jaeger unreadable, so the regression gate must not
    /// hide behind --features otel.
    #[test]
    fn merge_payload_store_emits_no_bundle_simulate_span() {
        use std::sync::Mutex;

        struct SpanNameCapture {
            names: std::sync::Arc<Mutex<Vec<String>>>,
        }
        impl<S> tracing_subscriber::Layer<S> for SpanNameCapture
        where
            S: tracing::Subscriber,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::span::Id,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.names
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(attrs.metadata().name().to_string());
            }
        }

        use tracing_subscriber::layer::SubscriberExt as _;
        let names = std::sync::Arc::new(Mutex::new(Vec::<String>::new()));
        let capture = SpanNameCapture {
            names: std::sync::Arc::clone(&names),
        };
        let subscriber = tracing_subscriber::registry().with(capture);

        let (mut engine, path_id, _pool_refs) = overfed_v4_engine();
        let metadata = BlockMetadata::default();
        let mk = || SolvePathResult {
            optimal_input: U256::from(1_000_000_000u64),
            profit: U256::from(1_000u64),
            hop_outputs: vec![U256::from(1u64)],
            consumed_inputs: vec![U256::from(1u64)],
            state_nonces: vec![0],
            solver_pool_states: Vec::new(),
        };
        let payload = crate::arb_engine::inline_sim::SimulatedPathResult {
            path_id,
            gross_profit: U256::from(1_000u64),
            net_profit: U256::from(900u64),
            gas_used: 300_000,
            priority_fee: 2,
            base_fee_next: 30,
            execute_calldata: vec![1, 2, 3],
            access_list: None,
            captured_swaps: Vec::new(),
            hop_count: 1,
            failure: None,
        };

        tracing::subscriber::with_default(subscriber, || {
            // Enclosing merge span, as in both production arms.
            let merge = tracing::info_span!("degenbot.arb.merge", merge.paths = 1u64);
            let _ctx = merge.enter();
            engine.merge_one_result(42, &metadata, path_id, mk(), 0, Some(payload));
        });

        let created = names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let offenders: Vec<_> = created
            .iter()
            .filter(|n| *n == "degenbot.bundle.simulate")
            .collect();
        assert!(
            offenders.is_empty(),
            "merge must not create bundle.simulate markers (the name belongs to real sims); \
             spans created: {created:?}"
        );
    }

    /// GREEN-gate (IJUBV3): the WORKER-side inline sim gets the honest
    /// `degenbot.bundle.simulate` span - a real ms-class EVM sim on the solve
    /// path, parented under the cycle span, with the terminal verdict.
    #[cfg(feature = "otel")]
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "single end-to-end span-emission assertion: stub + emit + export + attribute checks read best as one sequence"
    )]
    fn inline_sim_payload_emits_worker_sim_span_with_verdict() {
        use super::inline_sim_payload;
        use crate::otel;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
        use tracing_subscriber::layer::SubscriberExt;

        struct StubSim {
            fail: bool,
            path_id: u64,
        }
        impl crate::arb_engine::inline_sim::InlineSimulator for StubSim {
            fn simulate_path(
                &self,
                request: crate::arb_engine::inline_sim::InlineSimRequest,
            ) -> Option<crate::arb_engine::inline_sim::SimulatedPathResult> {
                assert_eq!(
                    request.path_id, self.path_id,
                    "stub receives the merged path id"
                );
                Some(crate::arb_engine::inline_sim::SimulatedPathResult {
                    path_id: request.path_id,
                    gross_profit: U256::from(1_000u64),
                    net_profit: U256::from(900u64),
                    gas_used: 300_000,
                    priority_fee: 2,
                    base_fee_next: 30,
                    execute_calldata: vec![7, 8, 9],
                    access_list: None,
                    captured_swaps: Vec::new(),
                    hop_count: 1,
                    failure: self
                        .fail
                        .then(|| crate::arb_engine::inline_sim::InlineSimFailure {
                            fail_index: None,
                            revert_data: Vec::new(),
                            bucket: "test".to_string(),
                        }),
                })
            }
        }

        let exporter = InMemorySpanExporter::default();
        let (provider, tracer) = otel::provider_with_exporter(exporter.clone());
        let subscriber = tracing_subscriber::registry().with(otel::layer(tracer));

        let (engine, path_id, pool_refs) = overfed_v4_engine();
        let mut ctx = worker_probe_ctx(Arc::clone(engine.core()), pool_refs);
        // Fresh Arc (refcount 1): install the stub via get_mut.
        Arc::get_mut(&mut ctx)
            .expect("probe ctx exclusively owned")
            .inline_sim = Some(Arc::new(StubSim {
            fail: false,
            path_id,
        }));

        let result = SolvePathResult {
            optimal_input: U256::from(1_000_000_000u64),
            profit: U256::from(1_000u64),
            hop_outputs: vec![U256::from(1u64)],
            consumed_inputs: vec![U256::from(1u64)],
            state_nonces: vec![0],
            solver_pool_states: Vec::new(),
        };

        tracing::subscriber::with_default(subscriber, || {
            let solve = tracing::info_span!("degenbot.arb.solve", block.number = 7u64);
            let _guard = solve.enter();
            let payload = inline_sim_payload(&ctx, 0, path_id, &result, &tracing::Span::current());
            assert!(
                payload.is_some(),
                "stub hook returns a payload; None only when the seam is off"
            );
        });

        provider.force_flush().expect("flush");
        let spans = exporter.get_finished_spans().expect("spans");
        let solve_id = spans
            .iter()
            .find(|sp| sp.name.as_ref() == "degenbot.arb.solve")
            .map(|sp| sp.span_context.span_id())
            .expect("solve span must be exported");
        let sims: Vec<_> = spans
            .iter()
            .filter(|sp| sp.name.as_ref() == "degenbot.bundle.simulate")
            .collect();
        assert_eq!(
            sims.len(),
            1,
            "exactly one worker-side sim span; all: {:?}",
            spans.iter().map(|sp| sp.name.as_ref()).collect::<Vec<_>>()
        );
        assert_eq!(
            sims[0].parent_span_id, solve_id,
            "the worker sim span must parent under the cycle span"
        );
        let attr = |k: &'static str| {
            sims[0]
                .attributes
                .iter()
                .find(|kv| kv.key == opentelemetry::Key::from_static_str(k))
                .map(|kv| kv.value.to_string())
        };
        assert_eq!(
            attr("path_id").as_deref(),
            Some(path_id.to_string().as_str()),
            "path_id attribute"
        );
        assert_eq!(
            attr("simulate.verdict").as_deref(),
            Some("profitable"),
            "verdict recorded at span close; attrs: {:?}",
            sims[0].attributes
        );
        assert_eq!(
            attr("sim.path").as_deref(),
            Some("worker_inline"),
            "seam discriminator distinguishes worker sims from the FFI seam"
        );
    }
}

#[cfg(test)]
mod lpt_partition_tests {
    use super::*;

    // ---- LW-T7 (Seam F): determinism, runtime fallback, promotion gate ----

    /// LW-T7 (Seam F): LPT is bit-stable — the same input & cost fn yields
    /// IDENTICAL bins across 50 invocations at widely varying shape, and
    /// equal-cost ties resolve by the FIXED rule (original index order) —
    /// the expected bins are computed by an independent reading of the
    /// documented rule (stable sort desc by cost with index-order ties,
    /// then item-by-item onto the first minimal-load bin).
    #[test]
    fn lpt_partition_is_bit_stable_across_invocations_and_ties_are_index_ordered() {
        let costs = vec![40, 40, 40, 70, 70, 30, 30, 30, 30, 55];
        let n = costs.len();
        // Independent reading of the documented rule (order-stable, ties by
        // ascending original index; min-load bin tie by lowest bin index).
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by_key(|&i| (std::cmp::Reverse(costs[i]), i));
        let mut loads = vec![0usize; 3];
        let expected: Vec<Vec<usize>> = {
            let mut bins: Vec<Vec<usize>> = vec![Vec::new(); 3];
            for i in idx {
                let mi = (0..3)
                    .min_by_key(|&bi| (loads[bi], bi))
                    .expect("bing shape is non-empty");
                bins[mi].push(i);
                loads[mi] += costs[i];
            }
            bins
        };
        for invocation in 0..50 {
            let bins = lpt_partition(n, 3, |i| costs[i]);
            assert_eq!(
                bins, expected,
                "invocation {invocation}: bins deviate from the documented rule"
            );
        }
    }

    /// LW-T7 (Seam F): a seat-capacity drop under a cordon drives a NAMED
    /// typed runtime fallback decision (typed enum, logged at INFO) —
    /// never silent narrower bins mid-drain (the runtime twin of LW-T4's
    /// boot-time capacity floor).
    #[test]
    fn seat_drop_under_cordon_drives_a_named_typed_runtime_fallback() {
        // Narrower capability: the plan NAMES the drop (intended → running).
        let plan = plan_bins(6, 4);
        assert_eq!(plan.bins, 4);
        assert_eq!(
            plan.decision,
            CordonFallbackDecision::Narrower {
                intended: 6,
                running: 4,
            },
            "the fallback must be a NAMED typed decision"
        );
        // Full capability: no fallback, full width.
        let full = plan_bins(6, 6);
        assert_eq!(full.decision, CordonFallbackDecision::FullCapacity);
        assert_eq!(full.bins, 6);
    }

    #[test]
    fn lpt_distributes_heavy_items_across_bins() {
        // Costs: [100, 100, 100, 1, 1, 1, 1, 1, 1, 1] — three heavy items
        // must go to three different bins (not clustered on one).
        let costs = [100, 100, 100, 1, 1, 1, 1, 1, 1, 1];
        let bins = lpt_partition(costs.len(), 3, |i| costs[i]);
        assert_eq!(bins.len(), 3);
        // Each bin should have exactly one heavy item.
        for bin in &bins {
            let heavy_count = bin.iter().filter(|&&i| costs[i] == 100).count();
            assert!(
                heavy_count <= 1,
                "bin has {heavy_count} heavy items, expected <= 1"
            );
        }
        // Total items preserved.
        let total: usize = bins.iter().map(Vec::len).sum();
        assert_eq!(total, costs.len());
    }

    #[test]
    fn lpt_empty_items_produces_empty_bins() {
        let bins = lpt_partition(0, 4, |_| 0);
        assert_eq!(bins.len(), 4);
        assert!(bins.iter().all(Vec::is_empty));
    }

    #[test]
    fn lpt_fewer_items_than_bins() {
        // 2 items, 8 bins — each item gets its own bin.
        let costs = [50, 30];
        let bins = lpt_partition(costs.len(), 8, |i| costs[i]);
        assert_eq!(bins.len(), 8);
        let non_empty: usize = bins.iter().filter(|b| !b.is_empty()).count();
        assert_eq!(non_empty, 2);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn lpt_balances_load() {
        // Costs: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1] on 3 bins.
        // LPT assignment: 10→bin0(10), 9→bin1(9), 8→bin2(8), 7→bin1(16),
        // 6→bin2(14), 5→bin0(15), 4→bin2(18), 3→bin1(19), 2→bin0(17),
        // 1→bin0(18). Max load = 19, min load = 18. Well-balanced.
        let costs = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        let bins = lpt_partition(costs.len(), 3, |i| costs[i]);
        let loads: Vec<usize> = bins
            .iter()
            .map(|b| b.iter().map(|&i| costs[i]).sum())
            .collect();
        let max_load = *loads.iter().max().unwrap();
        let min_load = *loads.iter().min().unwrap();
        // LPT guarantees max_load - min_load <= max_item_cost.
        assert!(
            max_load - min_load <= 10,
            "load spread {max_load}-{min_load}={spread} exceeds max_item",
            spread = max_load - min_load
        );
    }

    #[test]
    fn sims_aware_cost_prefers_measured_last_block_walk() {
        // No measured value → structural proxy governs.
        assert_eq!(sims_aware_cost(500, None, None), 500);
        // Measured below the proxy → proxy still governs (fresh pool state
        // can always cost at least the structural floor).
        assert_eq!(sims_aware_cost(500, Some(300), None), 500);
        // Measured above the proxy → measured wins (the last block's sims
        // predict the current block's cost better than structure alone).
        assert_eq!(sims_aware_cost(300, Some(900), None), 900);
        // Oversized measured values saturate to usize::MAX rather than wrap.
        assert_eq!(sims_aware_cost(1, Some(u64::MAX), None), usize::MAX);
        // Loop-18: gate-heavy paths (sims≈0, gate 14ms) now register real cost.
        assert_eq!(sims_aware_cost(1, Some(0), Some(14_000)), 14_000);
        // Sims + gate terms ADD (both µs-scale) before the proxy comparison.
        assert_eq!(sims_aware_cost(500, Some(300), Some(14_000)), 14_300);
    }

    #[test]
    fn lpt_zero_bins_returns_empty_vec() {
        let bins = lpt_partition(5, 0, |_| 1);
        assert!(bins.is_empty());
    }
}

// ----------------- PER-PATH SPAN TELEMETRY (MQUKB6-T2) -----------------
#[cfg(all(test, feature = "otel"))]
#[expect(clippy::expect_used)] // otel tests assert loudly, per telemetry.rs otel_tests
mod solve_path_span_tests {
    use super::*;
    use crate::otel;
    use degenbot_solvers::mixed::ResolvedMixedPath;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use tracing_subscriber::layer::SubscriberExt;

    /// `solve_one_path` emits one `degenbot.arb.path` child span parented
    /// under the (re-entered) cycle span, carrying `path.id`. Scoped LOCAL
    /// subscriber (`with_default`): no global-slot mutation, no leakage
    /// from other suites' spans into this exporter.
    #[test]
    fn solve_one_path_emits_child_path_span_under_the_cycle_span() {
        let exporter = InMemorySpanExporter::default();
        let (provider, tracer) = otel::provider_with_exporter(exporter.clone());
        let subscriber = tracing_subscriber::registry().with(otel::layer(tracer));

        let ctx = super::executor_ab_probe::probe_ctx();
        let resolved = ResolvedMixedPath {
            hops: Vec::new(),
            valid: true,
            state_nonces: Vec::new(),
            max_update_block: 0,
        };
        tracing::subscriber::with_default(subscriber, || {
            let solve = tracing::info_span!("degenbot.arb.solve", block.number = 7u64);
            let _guard = solve.enter();
            let _ = solve_one_path(&ctx, &tracing::Span::current(), 77, &resolved);
        });

        provider.force_flush().expect("flush");
        let spans = exporter.get_finished_spans().expect("spans");

        let solve_id = spans
            .iter()
            .find(|sp| sp.name.as_ref() == "degenbot.arb.solve")
            .map(|sp| sp.span_context.span_id())
            .expect("solve span must be exported");
        let paths: Vec<_> = spans
            .iter()
            .filter(|sp| sp.name.as_ref() == "degenbot.arb.path")
            .collect();
        assert_eq!(
            paths.len(),
            1,
            "exactly one per-path span; got: {:?}",
            spans.iter().map(|sp| sp.name.as_ref()).collect::<Vec<_>>()
        );
        assert_eq!(
            paths[0].parent_span_id, solve_id,
            "degenbot.arb.path must parent under the re-entered cycle span"
        );
        assert!(
            paths[0]
                .attributes
                .iter()
                .any(|kv| kv.key == opentelemetry::Key::from_static_str("path.id")),
            "path.id must ride as a span attribute"
        );
    }
}

// ----------------- OFFLINE A/B PROBE (epic BXUSGL T4) -----------------
// env-driven, #[ignore]d - see the module docs below; run manually:
//   cargo test --release -p degenbot-bot --lib executor_ab_probe -- --ignored --nocapture
#[cfg(test)]
#[expect(
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
// Offline probe: panics/prints ARE the measurement contract (loud failure on
// misload; CSV is the output). Outer allow/expects at module level are the
// documented-permitted form for cross-lint bulk suppression.
pub(super) mod executor_ab_probe {
    // Offline A/B probe (epic BXUSGL T4): emit-granularity A/B on the
    // dedicated tokio solve executor, over the heavy-CL capture corpus.
    // Since the P6YXA6 hard cutover the executor is unambiguous — the rayon
    // arms are gone — so the probe measures the STREAMING property: per-PATH
    // sends (production) vs per-BIN sends (the pre-streaming granularity).
    // NOT part of the normal suite: `#[ignore]`d, env-driven, run
    // manually with `cargo test --release -p degenbot-bot --lib executor_ab --
    // --ignored --nocapture`. Uses the crate-internal production components
    // (`solve_one_path`, `SolveCycleShared`, `SolveExecutor`, `lpt_partition`,
    // `path_cost_proxy`) so the arms differ ONLY in emit granularity. Fixture
    // parse replicates rust/crates/degenbot-solvers/examples/rayon_scale_probe.rs.
    //
    // Env:
    //   DEGENBOT_PROBE_FIXTURE  fixture jsonl path (default: the committed
    //                           heavy_cl_solve_captures.jsonl)
    //   DEGENBOT_PROBE_NS       comma thread counts (default 1,2,4,8,16)
    //   DEGENBOT_PROBE_PASSES   measurement passes per config (default 3)
    //
    // CSV columns (stdout):
    //   arm,threads,items,wall_ms,first_emit_ms,p50_emit_ms,p95_emit_ms
    // `emit` = wall offset when a path's result is AVAILABLE to the merge - the
    // streaming property. For the `tokio-perbin` control every emit lands at
    // cycle end by construction (per-BIN sends); `tokio` (production) sends
    // per PATH.

    use std::sync::Arc;
    use std::time::Instant;

    use crate::arb_engine::BlockMetadata;
    use alloy::primitives::U256;
    use degenbot_pools::int_v3_hop::{IntV3TickRangeHop, IntV3TickRangeSequence};
    use degenbot_solvers::mobius_v3_int::{build_cl_crossing_table, build_cl_word_profiles};
    use serde_json::Value;

    use super::{
        lpt_partition, path_cost_proxy, solve_one_path, BotState, PathTimesHeap, SolveCycleShared,
    };
    use crate::arb_engine::solve_executor::SolveExecutor;
    use hashbrown::HashMap;

    pub(super) fn pct(values: &[f64], q: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        let mut v = values.to_vec();
        v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v[((v.len() as f64) * q).floor() as usize % v.len()]
    }

    fn fixture_path() -> std::path::PathBuf {
        if let Ok(p) = std::env::var("DEGENBOT_PROBE_FIXTURE") {
            return std::path::PathBuf::from(p);
        }
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../degenbot-solvers/tests/fixtures/heavy_cl_solve_captures.jsonl")
    }

    /// Zst-aware corpus load for the BCA77G parity fixtures: the packaged
    /// `heavy_cl_solve_captures.jsonl.zst` decodes transparently via
    /// `capture_fixture::read_fixture` (same corpus the probe measures).
    pub(in crate::arb_engine) fn load_corpus_fixture(
    ) -> Vec<Arc<::degenbot_solvers::mixed::ResolvedMixedPath>> {
        load_corpus()
    }

    fn u256(s: &str) -> Result<U256, String> {
        s.trim().parse::<U256>().map_err(|e| e.to_string())
    }

    fn range(v: &Value) -> Result<IntV3TickRangeHop, String> {
        let wbp = v
            .get("word_boundary_prices")
            .and_then(Value::as_array)
            .ok_or("word_boundary_prices")?
            .iter()
            .map(|w| w.as_str().ok_or_else(|| "wbp".to_string()).and_then(u256))
            .collect::<Result<Vec<_>, String>>()?;
        Ok(IntV3TickRangeHop {
            liquidity: v
                .get("liquidity")
                .and_then(Value::as_str)
                .ok_or("liquidity")?
                .parse::<u128>()
                .map_err(|e| e.to_string())?,
            sqrt_price_x96: u256(
                v.get("sqrt_price_x96")
                    .and_then(Value::as_str)
                    .ok_or("sp")?,
            )?,
            sqrt_price_lower_x96: u256(
                v.get("sqrt_price_lower_x96")
                    .and_then(Value::as_str)
                    .ok_or("spl")?,
            )?,
            sqrt_price_upper_x96: u256(
                v.get("sqrt_price_upper_x96")
                    .and_then(Value::as_str)
                    .ok_or("spu")?,
            )?,
            gamma_numer: v
                .get("gamma_numer")
                .and_then(Value::as_u64)
                .ok_or("gamma")?,
            fee_denom: v.get("fee_denom").and_then(Value::as_u64).ok_or("fee")?,
            zero_for_one: v
                .get("zero_for_one")
                .and_then(Value::as_bool)
                .ok_or("zfo")?,
            word_boundary_prices: wbp,
        })
    }

    pub(in crate::arb_engine) fn load_corpus(
    ) -> Vec<Arc<::degenbot_solvers::mixed::ResolvedMixedPath>> {
        let path = fixture_path();
        // Zst-aware (packaged fixtures decode transparently; regenerated
        // plain captures still win the resolution order).
        let content = ::degenbot_solvers::capture_fixture::read_fixture(&path);
        let mut items = Vec::new();
        for line in content.lines().filter(|l| !l.trim().is_empty()) {
            let Ok(doc) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Ok(hops_v) = doc.get("hops").and_then(Value::as_array).ok_or("hops") else {
                continue;
            };
            let mut hops = Vec::new();
            for hop in hops_v {
                let Ok(ra) = hop.as_array().ok_or("hop") else {
                    continue;
                };
                let mut ranges = Vec::new();
                for r in ra {
                    if let Ok(rh) = range(r) {
                        ranges.push(rh);
                    }
                }
                let seq = IntV3TickRangeSequence { ranges };
                hops.push(::degenbot_solvers::mixed::ResolvedHop::V3 {
                    word_profiles: Arc::from(build_cl_word_profiles(&seq)),
                    crossing_table: Arc::from(build_cl_crossing_table(&seq)),
                    int_seq: Arc::new(seq),
                });
            }
            items.push(Arc::new(::degenbot_solvers::mixed::ResolvedMixedPath {
                hops,
                valid: true,
                state_nonces: Vec::new(),
                max_update_block: 0,
            }));
        }
        assert!(!items.is_empty(), "fixture must load at least one path");
        items
    }

    pub(in crate::arb_engine) fn probe_ctx() -> Arc<SolveCycleShared> {
        Arc::new(SolveCycleShared {
            solve_block: 0,
            epoch: 0,
            metadata: BlockMetadata::default(),
            runtime: ::degenbot_solvers::runtime::SolveRuntimeConfig::default(),
            gate_capture: None,
            walk_memo: Arc::new(::degenbot_solvers::mobius_v3_int::WalkMemo::new(
                false, false,
            )),
            capture: None,
            capture_mixed: None,
            path_times: parking_lot::Mutex::new(PathTimesHeap::new()),
            gate_total: parking_lot::Mutex::new(
                ::degenbot_solvers::profit_envelope::GateStats::default(),
            ),
            solve_cpu_us: std::sync::atomic::AtomicU64::new(0),
            walk_pieces_total: std::sync::atomic::AtomicU64::new(0),
            walk_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_word_steps_total: std::sync::atomic::AtomicU64::new(0),
            walk_refine_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_ternary_total: std::sync::atomic::AtomicU64::new(0),
            walk_grid_total: std::sync::atomic::AtomicU64::new(0),
            sims_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            gate_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            core: Arc::new(crate::bot_core::state_lock::StateLock::new(BotState::new())),
            pool_refs: Vec::new(),
            worker_clamp: false,
            inline_sim: None,
            sim_fleet_hosted: false,
            #[cfg(test)]
            test_solve_delay: None,
        })
    }

    /// LPT bins per the production cost fn (empty measured-history = first-cycle
    /// structural cost, exactly like an engine cold bucket).
    pub(in crate::arb_engine) fn prod_lpt_bins(
        items: &[Arc<::degenbot_solvers::mixed::ResolvedMixedPath>],
        threads: usize,
    ) -> Vec<Vec<usize>> {
        lpt_partition(items.len(), threads, |i| path_cost_proxy(&items[i]))
    }

    /// One full cycle over the corpus, `emit` timestamps (offsets from
    /// cycle start) relative to when the result is AVAILABLE to the merge:
    ///  - tokio         : dedicated executor, per-PATH send (production)
    ///  - tokio-perbin  : per-BIN send (granularity control)
    fn run_cycle(
        items: &[Arc<::degenbot_solvers::mixed::ResolvedMixedPath>],
        threads: usize,
        arm: &str,
        ctx: &Arc<SolveCycleShared>,
    ) -> (f64, Vec<f64>) {
        let bins = prod_lpt_bins(items, threads);
        let (tx, rx) = std::sync::mpsc::channel::<f64>();
        let t0 = Instant::now();
        match arm {
            "tokio" => {
                // PE4FPM: the verification probe builds its own ephemeral
                // fleet; it census-registers under its own id so it can never
                // be confused with the production solve_executor_fleet.
                degenbot_core::worker_census::register(
                    degenbot_core::worker_census::WorkerCensusEntry {
                        resource: "solve_probe_executor",
                        kind: "ephemeral probe executor (solve-verify diagnostics)",
                        count: threads,
                        thread_name: "probe-solve-tokio",
                        sizing: "probe parameter (thread count passed to the verify run; torn down with the probe)",
                    },
                );
                let executor = SolveExecutor::new("probe-solve-tokio", threads);
                for bin in &bins {
                    let bin = bin.clone();
                    let tx = tx.clone();
                    let ctx = Arc::clone(ctx);
                    let items = items.to_vec();
                    executor.spawn(move || {
                        for &i in &bin {
                            let _ =
                                solve_one_path(&ctx, &tracing::Span::none(), i as u64, &items[i]);
                            let _ = tx.send(t0.elapsed().as_secs_f64() * 1000.0);
                        }
                        // Bin-complete sentinel (NaN): without it, the collector
                        // sees the channel close as soon as the last bin closure
                        // ends, and the runtime drop can cancel still-running bins
                        // mid-solve — an artificially short wall. Production is
                        // immune (process-lifetime executor singleton).
                        let _ = tx.send(f64::NAN);
                    });
                }
            }
            "tokio-perbin" => {
                // Granularity control: identical executor + bins, but the
                // whole BIN is visible to the merge only at bin completion
                // (the pre-streaming emit shape the rayon arm embodied).
                let executor = SolveExecutor::new("probe-solve-tokio", threads);
                for bin in &bins {
                    let bin = bin.clone();
                    let tx = tx.clone();
                    let ctx = Arc::clone(ctx);
                    let items = items.to_vec();
                    executor.spawn(move || {
                        for &i in &bin {
                            let _ =
                                solve_one_path(&ctx, &tracing::Span::none(), i as u64, &items[i]);
                        }
                        let _ = tx.send(t0.elapsed().as_secs_f64() * 1000.0);
                        // Bin-complete sentinel (NaN): see the "tokio" arm.
                        let _ = tx.send(f64::NAN);
                    });
                }
            }
            other => unreachable!("unknown arm {other}"),
        }
        drop(tx);
        let mut emits: Vec<f64> = Vec::new();
        let mut completed_bins = 0usize;
        for v in rx {
            if v.is_nan() {
                completed_bins += 1;
                if completed_bins == bins.len() {
                    break;
                }
            } else {
                emits.push(v);
            }
        }
        (t0.elapsed().as_secs_f64() * 1000.0, emits)
    }

    pub(super) fn raise_nice_for_lane_courtesy() {
        #[cfg(unix)]
        {
            // T4 courtesy: keep this offline probe off the live soak's cores.
            let _ = unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, 15) };
        }
    }

    #[test]
    #[ignore = "offline A/B probe; run with --ignored (T4); env: DEGENBOT_PROBE_FIXTURE/NS/PASSES"]
    fn executor_ab_probe_runs_and_prints_csv() {
        raise_nice_for_lane_courtesy();
        let items = load_corpus();
        eprintln!("corpus paths = {}", items.len());
        let threads: Vec<usize> = match std::env::var("DEGENBOT_PROBE_NS") {
            Ok(s) => s.split(',').filter_map(|t| t.trim().parse().ok()).collect(),
            Err(_) => vec![1, 2, 4, 8, 16],
        };
        let passes: usize = std::env::var("DEGENBOT_PROBE_PASSES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        println!("arm,threads,items,wall_ms,first_emit_ms,p50_emit_ms,p95_emit_ms");
        let ctx = probe_ctx();
        for &n in &threads {
            for arm in ["tokio", "tokio-perbin"] {
                for _ in 0..passes {
                    let (wall, emits) = run_cycle(&items, n, arm, &ctx);
                    println!(
                        "{arm},{n},{},{wall:.1},{first:.1},{p50:.1},{p95:.1}",
                        items.len(),
                        first = emits.iter().copied().fold(f64::INFINITY, f64::min),
                        p50 = pct(&emits, 0.5),
                        p95 = pct(&emits, 0.95),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod fleet_sim_stance_tests {
    //! ADR-042 F4 (task LTUE7I) fixtures: the inline-sim runtime port onto
    //! the fleet. Parity — fleet-seat sims return byte/field-equal results
    //! to the legacy detached-sim threads over requests derived from the
    //! committed heavy-CL capture corpus. Identity — the stance flips the
    //! hosting thread family: legacy `arb-sim-{pid}` detached threads vs
    //! fleet `work-fleet-sim-{n}` `SimDriver` seats (census row included).

    use alloy::primitives::{I256, U256};
    use degenbot_solvers::mixed::{HopType, MixedPath, MixedPoolRef, SolvePathResult};
    use hashbrown::HashMap;
    use std::sync::Arc;

    use super::executor_ab_probe::load_corpus_fixture;
    use super::{PathTimesHeap, PipelinedSims, SolveCycleShared};
    use crate::arb_engine::inline_sim::{
        AccessListRow, CapturedSwapRow, InlineSimFailure, InlineSimRequest, InlineSimulator,
        InlineSwapFamily, SimulatedPathResult,
    };
    use crate::arb_engine::BlockMetadata;

    // ---- deterministic sim stub ------------------------------------------------

    /// Deterministic primitive-payload sim: the payload is a pure function
    /// of the request (so both stances assert on identical request streams),
    /// and the executing thread's family is recorded for the identity
    /// fixture. Exercises the failure-payload contract through both arms.
    struct CorpusSim {
        thread_names: parking_lot::Mutex<Vec<String>>,
    }

    impl CorpusSim {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                thread_names: parking_lot::Mutex::new(Vec::new()),
            })
        }
    }

    impl InlineSimulator for CorpusSim {
        fn simulate_path(&self, request: InlineSimRequest) -> Option<SimulatedPathResult> {
            self.thread_names.lock().push(
                std::thread::current()
                    .name()
                    .map(str::to_owned)
                    .unwrap_or_default(),
            );
            let hop0 = (request.optimal_input % U256::from(u64::MAX)).to::<u64>();
            if request.path_id % 11 == 5 {
                // Exercise the failure-payload contract through both arms.
                return Some(SimulatedPathResult {
                    path_id: request.path_id,
                    gross_profit: U256::ZERO,
                    net_profit: U256::ZERO,
                    gas_used: 0,
                    priority_fee: 0,
                    base_fee_next: 0,
                    execute_calldata: Vec::new(),
                    access_list: None,
                    captured_swaps: Vec::new(),
                    hop_count: request.hops.len(),
                    failure: Some(InlineSimFailure {
                        fail_index: Some(1),
                        revert_data: vec![0x08, 0xc3, 0x79, 0xa0],
                        bucket: "revert".to_string(),
                    }),
                });
            }
            Some(SimulatedPathResult {
                path_id: request.path_id,
                gross_profit: U256::from(hop0 % 1_000_000 + 7),
                net_profit: U256::from(hop0 % 1_000_000 + 3),
                gas_used: 40_000
                    + u64::try_from(request.hops.len()).unwrap_or(u64::from(u8::MAX)) * 3_000,
                priority_fee: 3,
                base_fee_next: 31,
                execute_calldata: vec![
                    0xa9,
                    u8::try_from(request.path_id % 251).unwrap_or(1),
                    u8::try_from(request.hops.len()).unwrap_or(u8::MAX),
                ],
                access_list: request.path_id.is_multiple_of(2).then(|| {
                    vec![AccessListRow {
                        address: alloy::primitives::Address::from([0x7au8; 20]),
                        storage_keys: vec![U256::from(request.path_id)],
                    }]
                }),
                captured_swaps: vec![CapturedSwapRow {
                    emitter: alloy::primitives::Address::from([0x11u8; 20]),
                    family: if request.hops.len() > 2 {
                        InlineSwapFamily::V3
                    } else {
                        InlineSwapFamily::V2
                    },
                    amount0: I256::try_from(-i128::from(hop0 % 5_000_000_000u64))
                        .unwrap_or(I256::ZERO),
                    amount1: I256::try_from(i128::from(hop0 % 4_900_000_000u64))
                        .unwrap_or(I256::ZERO),
                    sqrt_price_x96: U256::from(1u128) << 96,
                    liquidity: U256::from(1_000_000u64),
                    tick: 0,
                }],
                hop_count: request.hops.len(),
                failure: None,
            })
        }
    }

    // ---- corpus-derived request fan-out ------------------------------------------

    const PARITY_REQUESTS: usize = 24;

    /// Stride the committed capture corpus down to `want` items (the corpus
    /// is the request-shape oracle — hop counts and magnitude spreads ride
    /// the real capture, not synthesized round numbers).
    fn strided_corpus(want: usize) -> Vec<Arc<degenbot_solvers::mixed::ResolvedMixedPath>> {
        let items = load_corpus_fixture();
        assert!(!items.is_empty(), "capture corpus must load");
        let stride = items.len().saturating_sub(1) / want + 1;
        items.into_iter().step_by(stride).take(want).collect()
    }

    fn pool_refs_for(
        items: &[Arc<degenbot_solvers::mixed::ResolvedMixedPath>],
    ) -> Vec<Arc<MixedPath>> {
        items
            .iter()
            .map(|item| {
                let hops = (0..item.hops.len().clamp(1, 4))
                    .map(|i| MixedPoolRef {
                        hop_type: HopType::V3,
                        pool_key: u64::try_from(i).unwrap_or(u64::MAX),
                        zero_for_one: i % 2 == 0,
                    })
                    .collect();
                Arc::new(MixedPath { pools: hops })
            })
            .collect()
    }

    fn make_ctx(
        sim: Arc<CorpusSim>,
        pool_refs: Vec<Arc<MixedPath>>,
        sim_fleet_hosted: bool,
    ) -> Arc<SolveCycleShared> {
        Arc::new(SolveCycleShared {
            solve_block: 42,
            epoch: 0,
            metadata: BlockMetadata {
                base_fee_per_gas: Some(30),
                ..BlockMetadata::default()
            },
            runtime: ::degenbot_solvers::runtime::SolveRuntimeConfig::default(),
            gate_capture: None,
            walk_memo: Arc::new(::degenbot_solvers::mobius_v3_int::WalkMemo::new(
                false, false,
            )),
            capture: None,
            capture_mixed: None,
            path_times: parking_lot::Mutex::new(PathTimesHeap::new()),
            gate_total: parking_lot::Mutex::new(
                ::degenbot_solvers::profit_envelope::GateStats::default(),
            ),
            solve_cpu_us: std::sync::atomic::AtomicU64::new(0),
            walk_pieces_total: std::sync::atomic::AtomicU64::new(0),
            walk_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_word_steps_total: std::sync::atomic::AtomicU64::new(0),
            walk_refine_sims_total: std::sync::atomic::AtomicU64::new(0),
            walk_ternary_total: std::sync::atomic::AtomicU64::new(0),
            walk_grid_total: std::sync::atomic::AtomicU64::new(0),
            sims_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            gate_recorder: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            core: Arc::new(crate::bot_core::state_lock::StateLock::new(
                crate::bot_core::BotState::new(),
            )),
            pool_refs,
            worker_clamp: true,
            inline_sim: Some(sim),
            sim_fleet_hosted,
            #[cfg(test)]
            test_solve_delay: None,
        })
    }

    fn admitted_for(idx: usize, hops: usize) -> SolvePathResult {
        SolvePathResult {
            optimal_input: U256::from(1_000_000_000u64 + u64::try_from(idx).unwrap_or(0) * 7),
            profit: U256::from(1_000u64 + u64::try_from(idx).unwrap_or(0)),
            hop_outputs: (0..hops)
                .map(|h| {
                    U256::from(
                        900_000_000u64
                            + u64::try_from(idx).unwrap_or(0) * 13
                            + u64::try_from(h).unwrap_or(u64::MAX),
                    )
                })
                .collect(),
            consumed_inputs: (0..hops)
                .map(|h| {
                    U256::from(
                        900_000_000u64
                            + u64::try_from(idx).unwrap_or(0) * 3
                            + u64::try_from(h).unwrap_or(u64::MAX),
                    )
                })
                .collect(),
            state_nonces: vec![0; hops],
            solver_pool_states: Vec::new(),
        }
    }

    /// Schedule ONE sim through the production scheduler (`PipelinedSims::
    /// schedule_one`, stance-routed) and join its receipt.
    fn schedule_and_join(
        ctx: &Arc<SolveCycleShared>,
        idx: usize,
        pid: u64,
        hops: usize,
    ) -> (bool, Option<SimulatedPathResult>) {
        let mut pending = PipelinedSims::default();
        let parent = tracing::Span::none();
        let result = admitted_for(idx, hops);
        let scheduled = pending.schedule_one(ctx, idx, pid, &result, &parent);
        if !scheduled {
            return (false, None);
        }
        pending
            .join_all()
            .next()
            .map_or((true, None), |(jpid, payload)| {
                assert_eq!(jpid, pid, "the receipt must carry its request's pid");
                (true, payload)
            })
    }

    /// PARITY FIXTURE (LTUE7I): fleet-stance inline sims return BYTE-EQUAL
    /// payloads to the legacy detached-sim threads over the committed
    /// capture corpus — same requests, same deterministic sim, only the
    /// dispatch machinery differs (the port).
    #[test]
    fn fleet_stance_inline_sims_are_parity_with_legacy_threads_on_capture_corpus() {
        let items = strided_corpus(PARITY_REQUESTS);
        let pool_refs = pool_refs_for(&items);
        let sim = CorpusSim::new();

        let run_arm = |sim_fleet_hosted: bool| {
            let ctx = make_ctx(Arc::clone(&sim), pool_refs.clone(), sim_fleet_hosted);
            // Deterministic reverse order so receipts interleave like a
            // real multi-bin fan-out (per-receipt channels, not the arm,
            // carry order).
            let mut joined: Vec<(u64, Option<SimulatedPathResult>)> = Vec::new();
            for idx in (0..items.len()).rev() {
                let pid = u64::try_from(idx).unwrap_or(u64::MAX);
                let hops = items[idx].hops.len().clamp(1, 4);
                let (scheduled, payload) = schedule_and_join(&ctx, idx, pid, hops);
                assert!(scheduled, "every fixtured request must schedule");
                joined.push((pid, payload));
            }
            joined.sort_unstable_by_key(|(pid, _)| *pid);
            joined
        };

        let legacy = run_arm(false);
        let fleet = run_arm(true);
        assert!(!legacy.is_empty(), "fixture must schedule sims");
        assert_eq!(
            fleet, legacy,
            "fleet-hosted inline sims must be result parity with the legacy detached-sim threads"
        );
        assert!(
            legacy.iter().any(|(pid, _)| pid % 11 == 5),
            "the fixture must exercise the failure-payload contract too"
        );
    }

    /// IDENTITY FIXTURE (LTUE7I): the stance flips the runtime identity —
    /// legacy keeps the `arb-sim-{pid}` detached threads byte-for-byte;
    /// the fleet stance runs the SAME requests on fleet `SimDriver` seats
    /// (`work-fleet-sim-{n}`) and the executor's census row is registered
    /// (the `fleet_merge_slots` pattern from the BCA77G work).
    #[test]
    fn fleet_stance_flips_the_sim_runtime_identity_legacy_threads_vs_fleet_seats() {
        let items = strided_corpus(4);
        let pool_refs = pool_refs_for(&items);
        let sim = CorpusSim::new();

        let run_arm = |sim_fleet_hosted: bool| {
            sim.thread_names.lock().clear();
            let ctx = make_ctx(Arc::clone(&sim), pool_refs.clone(), sim_fleet_hosted);
            for (idx, item) in items.iter().enumerate() {
                let pid = u64::try_from(idx).unwrap_or(u64::MAX);
                let hops = item.hops.len().clamp(1, 4);
                let (scheduled, _payload) = schedule_and_join(&ctx, idx, pid, hops);
                assert!(scheduled, "every fixtured request must schedule");
            }
            sim.thread_names.lock().clone()
        };

        let legacy_names = run_arm(false);
        assert!(
            !legacy_names.is_empty() && legacy_names.iter().all(|n| n.starts_with("arb-sim-")),
            "legacy sims must run on arb-sim detached threads, got {legacy_names:?}"
        );

        let fleet_names = run_arm(true);
        assert!(
            !fleet_names.is_empty() && fleet_names.iter().all(|n| n.starts_with("work-fleet-sim-")),
            "fleet sims must run on fleet SimDriver seats, got {fleet_names:?}"
        );
        let row = degenbot_core::worker_census::snapshot()
            .into_iter()
            .find(|e| e.resource == "fleet_simdriver_slots")
            .expect("fleet SimDriver slots must be census-registered");
        assert_eq!(row.thread_name, "work-fleet-sim-{n}");
    }
}

#[cfg(test)]
mod dispatch_binning_properties {
    //! JXCAR4 (epic 64ZQLA): solver dispatch binning properties over
    //! ARBITRARY item counts, cost shapes and seat shapes. The spawn-seam
    //! invariant (`FleetSolveExecutor::spawn` aborts on a bin >= the
    //! structural seat count, commit `ccc148275`) must never be the
    //! discovery mechanism again: a future binning regression shows up here
    //! as a shrunk counterexample, not a host-only SIGABRT.
    use super::executor_ab_probe::prod_lpt_bins;
    use super::lpt_partition;
    use degenbot_solvers::mixed::ResolvedMixedPath;
    use proptest::prelude::*;
    use std::sync::Arc;

    /// Synthesized fixture paths (empty hops = zero structural cost; the
    /// properties exercise the BINDER, not the solver).
    fn synth_items(n: usize) -> Vec<Arc<ResolvedMixedPath>> {
        (0..n)
            .map(|_| {
                Arc::new(ResolvedMixedPath {
                    hops: Vec::new(),
                    valid: true,
                    state_nonces: Vec::new(),
                    max_update_block: 0,
                })
            })
            .collect()
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(256))]
        #[test]
        fn lpt_partition_preserves_items_and_stays_enclosed(
            n_items in 0usize..=2048usize,
            n_bins in 1usize..=64usize,
            costs in proptest::collection::vec(0usize..=97usize, 0..=2048),
        ) {
            let cost = |i: usize| costs.get(i).copied().unwrap_or(0);
            let bins = lpt_partition(n_items, n_bins, cost);
            // Enclosure: exactly the requested bin shape, indices inside.
            prop_assert_eq!(bins.len(), n_bins);
            for bin in &bins {
                for &i in bin {
                    prop_assert!(i < n_items);
                }
            }
            // Preservation: every item index appears exactly once.
            let mut seen: Vec<usize> = bins.iter().flatten().copied().collect();
            seen.sort_unstable();
            prop_assert_eq!(seen.len(), n_items);
            for (want, got) in seen.iter().enumerate() {
                prop_assert_eq!(*got, want);
            }
        }

        #[test]
        #[expect(
            clippy::cast_precision_loss,
            reason = "quota units (1e6 scale, <= 6.4e7) are exact in f64"
        )]
        fn prod_bins_never_exceed_the_structural_seats(
            quota_units in 6_000_000u64..=64_000_000u64,
            n_items in 0usize..=1024usize,
        ) {
            // Hostable quotas only (floor >= the pinned-role floor of 6);
            // the seat count comes from the REAL budget table keyed to the
            // quota property, never from this machine's shape.
            let q = (quota_units as f64) / 1_000_000.0;
            let seats = match degenbot_workers::budget::FleetBudget::derive(
                q,
                &degenbot_workers::budget::BudgetOverrides::default(),
            ) {
                Ok(b) => b.solver_pin_count,
                Err(err) => {
                    return Err(TestCaseError::fail(format!(
                        "hostable quota refused: {err:?}"
                    )));
                }
            };
            let items = synth_items(n_items);
            let bins = prod_lpt_bins(&items, seats);
            // Binding at the fleet's own seat count: the spawn normalize
            // (validate_bin_index) can never fire.
            prop_assert_eq!(bins.len(), seats);
            for (bin_idx, bin) in bins.iter().enumerate() {
                prop_assert!(bin_idx < seats);
                for &i in bin {
                    prop_assert!(i < n_items);
                }
            }
            // Item preservation across the seats.
            let total: usize = bins.iter().map(Vec::len).sum();
            prop_assert_eq!(total, n_items);
        }
    }
}
