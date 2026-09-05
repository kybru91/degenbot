//! The production `InlineSimulator` implementation (SIMPIPE2 T4 prerequisite) —
//! the degenbot-python closure over the session sim config.
//!
//! The engine's `degenbot_bot` seam stays strategy-agnostic (ADR-019 D7):
//! this crate installs the concrete simulator. One per-path sim drives the
//! SAME core the FFI fan-out drives — `BlockSimHandle` (the layered DB:
//! AlloyDB → `WrapDatabaseAsync` → `BotStateDb` → `WarmCodeCache` →
//! `CacheDB`, the state overrides applied once) → `simulate_path_on_evm` —
//! so the two arms' verdicts are comparable by construction (the T4
//! soak's premise). The deliberate differences:
//!
//! - ZERO FFI: the sim runs entirely in the solve worker's thread context.
//! - The per-path handle build (the `M1` amortization target) still happens
//!   per candidate — the soak MEASURES that (a per-block handle reuse
//!   inside the worker is a follow-up if the tail demands it).
//! - `block_priority_fees` is `None` (the dispatcher's fee ring is
//!   Python-owned): the priority fee falls to the core's self-targeting
//!   formula without the percentile clamp. The soak compares LATENCY
//!   primarily; the formula (target/decay) dominates the clamp on mainnet.
//!
//! Runtime caveat (the T4 body note): `WrapDatabaseAsync::new` captures
//! `Handle::try_current()` at build time, and its DB calls escalate to
//! `block_in_place` on a multi-threaded worker. The solve workers are
//! rayon/`std` threads — NO ambient runtime — so the sim body spawns onto
//! this hook's DEDICATED multi-thread runtime (task-spawn, not
//! `block_in_place`): the spawned task runs on a runtime worker (where
//! `block_in_place` is legal AND the handle capture succeeds), and the
//! worker blocks on the `JoinHandle` from the plain thread.

use std::sync::Arc;

use alloy::primitives::{Bytes, U256};
use degenbot_arbitrage::{
    simulate_path_on_evm, FailBuckets, SimResult, SimulateContext, SimulatePath, SolveStep,
};
use degenbot_bot::bot_core::state_lock::StateLock;
use degenbot_bot::bot_core::{BotState, SimAnchorState};
use degenbot_bot::solvers::arb_engine::inline_sim::{
    AccessListRow, CapturedSwapRow, InlineSimFailure, InlineSimRequest, InlineSimulator,
    InlineSwapFamily, SimulatedPathResult,
};
use degenbot_executor::composers::EncodeOptions;
use degenbot_rpc::provider::AlloyProvider;
use degenbot_simulation::sim::evm::inspectors::SwapFamily;
use degenbot_simulation::WarmCodeCacheInner;
use parking_lot::RwLock;
use std::future::Future;

/// The session-static sim config + the shared state arc-set the closure
/// closes over. Built once (at `install_inline_simulator` time) and shared
/// by every per-path worker call for the engine's lifetime.
pub(crate) struct InlineSimHook {
    /// The `sim_bounded` provider (fail-fast cold-miss budget — the dispatch
    /// seam's incident-2026-08-20 discipline).
    provider: Arc<AlloyProvider>,
    executor_owner: alloy::primitives::Address,
    executor_address: alloy::primitives::Address,
    weth_address: alloy::primitives::Address,
    pool_manager_address: alloy::primitives::Address,
    multicall3_address: alloy::primitives::Address,
    inject_code: bool,
    injected_address: Option<alloy::primitives::Address>,
    runtime_bytecode: Bytes,
    warmup: degenbot_executor::WarmupSlots,
    erc6909_profit: bool,
    /// The shared core (the sim anchor's snapshot source) — the SAME short
    /// read discipline as the FFI path (ULUWNI: snapshot under a short read,
    /// drop the guard BEFORE any provider I/O).
    bot_state: Arc<StateLock<BotState>>,
    warm_cache: Arc<RwLock<WarmCodeCacheInner>>,
    /// The dedicated multi-thread sim runtime (the T4 body note — see the
    /// module doc). Built once, shared for the hook's lifetime.
    sim_runtime: Arc<tokio::runtime::Runtime>,
    /// SIMPIPE2 M2: per-block storage memo shared by every payload sim of
    /// the same sim height (recreated on block advance). Collapses the
    /// ~20-cold-storage-RPC-per-sim into ~per-pool-unique per cycle.
    storage_memo: std::sync::Mutex<(u64, Arc<degenbot_simulation::StorageMemo>)>,
}

fn outputs_vec(req: &InlineSimRequest) -> Vec<u128> {
    req.hop_outputs
        .iter()
        .map(|v| u128::try_from(*v).unwrap_or(u128::MAX))
        .collect()
}

impl InlineSimHook {
    /// Assemble the hook from the installed PyO3 context (see
    /// `PyArbitrageEngine::install_inline_simulator`).
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: Arc<AlloyProvider>,
        executor_owner: alloy::primitives::Address,
        executor_address: alloy::primitives::Address,
        weth_address: alloy::primitives::Address,
        pool_manager_address: alloy::primitives::Address,
        multicall3_address: alloy::primitives::Address,
        inject_code: bool,
        injected_address: Option<alloy::primitives::Address>,
        runtime_bytecode: Bytes,
        warmup: degenbot_executor::WarmupSlots,
        erc6909_profit: bool,
        bot_state: Arc<StateLock<BotState>>,
        warm_cache: Arc<RwLock<WarmCodeCacheInner>>,
    ) -> Self {
        Self {
            provider,
            executor_owner,
            executor_address,
            weth_address,
            pool_manager_address,
            multicall3_address,
            inject_code,
            injected_address,
            runtime_bytecode,
            warmup,
            erc6909_profit,
            bot_state,
            warm_cache,
            storage_memo: std::sync::Mutex::new((
                0,
                Arc::new(degenbot_simulation::StorageMemo::new()),
            )),
            sim_runtime: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    // M2 soak sizing (2026-09-05): with the hard-coded 2
                    // workers the per-cycle wall was 72ms + 4.74ms/path
                    // (R^2 0.89, 228 steady cycles) - the payload sims queued
                    // on the 2-thread runtime while ~50 bins/cycle arrived
                    // concurrently (sims p50 12ms). Sizing to the core count
                    // lets the bins' sims actually overlap. Env-tunable for
                    // constrained hosts; the sim bodies still block on the
                    // DB wrap's block_on, so workers also cover that wait.
                    .worker_threads(inline_sim_worker_count())
                    .enable_all()
                    .build()
                    .expect("inline-sim runtime build"),
            ),
        }
    }

    /// The EIP-1559 `next_base_fee` (the `calculations/evm_math.py` port —
    /// the worker-side twin of the driver's pre-sim computation).
    fn next_base_fee(&self, req: &InlineSimRequest) -> u128 {
        if req.parent_base_fee == 0 {
            return 0; // pre-EIP-1559 / unknown parent
        }
        let parent_base_fee = u128::from(req.parent_base_fee);
        let last_gas_target = (req.parent_gas_limit / 2).max(1);
        if req.parent_gas_used == last_gas_target {
            return parent_base_fee;
        }
        if req.parent_gas_used > last_gas_target {
            let gas_used_delta = req.parent_gas_used - last_gas_target;
            let base_fee_delta =
                (parent_base_fee * u128::from(gas_used_delta) / u128::from(last_gas_target) / 8)
                    .max(1);
            parent_base_fee + base_fee_delta
        } else {
            let gas_used_delta = last_gas_target - req.parent_gas_used;
            let base_fee_delta =
                parent_base_fee * u128::from(gas_used_delta) / u128::from(last_gas_target) / 8;
            parent_base_fee.saturating_sub(base_fee_delta)
        }
    }

    /// Convert a core `SimResult` into the primitive payload (the T1 field
    /// table).
    fn payload_from_sim(path_id: u64, sim: &SimResult) -> SimulatedPathResult {
        SimulatedPathResult {
            path_id,
            gross_profit: sim.gross_profit,
            net_profit: sim.net_profit,
            gas_used: sim.gas_used,
            priority_fee: sim.priority_fee,
            base_fee_next: sim.base_fee_next,
            execute_calldata: sim.execute_calldata.to_vec(),
            access_list: sim.access_list.as_ref().map(|lst| {
                lst.0
                    .iter()
                    .map(|row| AccessListRow {
                        address: row.address,
                        storage_keys: row
                            .storage_keys
                            .iter()
                            .map(|k| U256::from_be_bytes(k.0))
                            .collect(),
                    })
                    .collect()
            }),
            captured_swaps: sim
                .captured_swaps
                .iter()
                .map(|s| CapturedSwapRow {
                    emitter: s.emitter,
                    family: match s.family {
                        SwapFamily::V2 => InlineSwapFamily::V2,
                        SwapFamily::V3 => InlineSwapFamily::V3,
                        SwapFamily::V4 => InlineSwapFamily::V4,
                    },
                    amount0: s.amount0,
                    amount1: s.amount1,
                    sqrt_price_x96: s.sqrt_price_x96,
                    liquidity: s.liquidity,
                    tick: s.tick,
                })
                .collect(),
            hop_count: sim.hop_count,
            failure: None,
        }
    }
}

/// Join the spawned sim task from ANY thread context (the soak's runtime
/// matrix): the solve arms' workers may be plain threads (rayon/`std`), OR
/// tokio tasks on the solve-executor runtime (`DEGENBOT_SOLVE_EXECUTOR=tokio`
/// spawns the per-bin jobs as tasks). `Runtime::block_on` from inside a
/// runtime context panics ("Cannot start a runtime from within a runtime" —
/// the 2026-09-05 soak freeze #2), so:
/// - inside a multi-thread runtime: `block_in_place` + join on THAT runtime's
///   handle (legal on a worker; the handle drive keeps the task-local context
///   the DB wrap needs);
/// - otherwise (plain threads): block on the dedicated sim runtime.
fn join_sim_task<F>(sim_runtime: &tokio::runtime::Runtime, fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        _ => sim_runtime.block_on(fut),
    }
}

/// The inline-sim runtime's worker count (M2 soak sizing): core-count by
/// default (the payload sims are the per-path marginal cost of every solve
/// cycle - see the M1/M2 soak records), overridable with
/// `DEGENBOT_INLINE_SIM_WORKERS`. Clamped to 1..=32; unparsable/garbage
/// values fall back to the default rather than failing the engine build.
fn inline_sim_worker_count() -> usize {
    let default = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    match std::env::var("DEGENBOT_INLINE_SIM_WORKERS") {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .map(|n| n.clamp(1, 32))
            .unwrap_or(default),
        Err(_) => default,
    }
}

impl InlineSimulator for InlineSimHook {
    #[expect(clippy::too_many_lines)]
    fn simulate_path(&self, req: InlineSimRequest) -> Option<SimulatedPathResult> {
        // 1. PathInfo STRAIGHT OFF THE CORE — engine-Lock-FREE. The calling
        //    cycle holds the engine `Mutex` for its whole duration (the busy
        //    loop that owns this worker), so re-entering the engine lock from
        //    the worker would self-deadlock (cycle-waits-on-bin,
        //    bin-waits-on-cycle — the 2026-09-05 soak freeze). The projection
        //    (build_path_info) needs only a short core read, same class as
        //    the T2 clamp's read. Unknown/unresolvable hops degrade to `None`
        //    (the batch entry stays payload-less -> the legacy FFI path).
        let path_info = {
            let core = self.bot_state.read();
            match degenbot_bot::solvers::arb_engine::build_path_info(&core, &req.hops) {
                Ok(pi) => pi,
                Err(_) => return None,
            }
        };

        // 2. The sim anchor — SHORT core read, dropped before any provider
        //    I/O (ULUWNI discipline).
        let anchor = {
            let guard = self.bot_state.read();
            SimAnchorState::snapshot(&guard)
        };

        // 3. The sim path from the clamp-committed solve outputs (u128 steps;
        //    >u128 values are the int-overflow class anyway).
        let to_u128 = |v: &U256| u128::try_from(*v).ok();
        let optimal_input = to_u128(&req.optimal_input)?;
        let consumed: Vec<Option<u128>> = req.consumed_inputs.iter().map(to_u128).collect();
        let outputs: Vec<Option<u128>> = req.hop_outputs.iter().map(to_u128).collect();
        if consumed.iter().any(Option::is_none) || outputs.iter().any(Option::is_none) {
            return None;
        }
        let steps: Vec<SolveStep> = consumed
            .into_iter()
            .zip(outputs)
            .enumerate()
            .map(|(i, (consumed_input, output))| SolveStep {
                output: output.unwrap_or(0),
                consumed_input: consumed_input.unwrap_or(0),
                state_nonce: req.state_nonces.get(i).copied().unwrap_or(0),
            })
            .collect();
        let sim_path = SimulatePath {
            path_id: req.path_id,
            optimal_input,
            steps: steps.into_boxed_slice(),
            path_info,
            solve_block: req.sim_block,
            opts: EncodeOptions {
                erc6909_profit: self.erc6909_profit,
                ..EncodeOptions::default()
            },
        };

        let path_id = req.path_id;
        let hop_count = req.hop_outputs.len();
        let base_fee_next = self.next_base_fee(&req);
        let provider = Arc::clone(&self.provider);
        let warm_cache = Arc::clone(&self.warm_cache);
        // M2: the cycle-scoped storage memo - one per sim block; sims at a
        // new block recreate it (pre-state differs across heights).
        let storage_memo = {
            // A poisoned memo lock is recoverable: the memo is block-scoped
            // and purely advisory (the fallback path re-fetches on a miss).
            let mut guard = match self.storage_memo.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if guard.0 != req.sim_block {
                // M2 visibility: the retiring block's memo tally - the hit
                // share is the RPC-trip compression the memo buys. The
                // degenbot::diag target stays off the Python log cap (console
                // + OTel only).
                let (hits, misses) = guard.1.stats();
                if hits + misses > 0 {
                    tracing::info!(
                        target: "degenbot::diag",
                        block_number = guard.0,
                        memo.hits = hits,
                        memo.misses = misses,
                        "[inline-sim] storage memo stats (block retired)"
                    );
                }
                *guard = (
                    req.sim_block,
                    Arc::new(degenbot_simulation::StorageMemo::new()),
                );
            }
            Arc::clone(&guard.1)
        };
        let executor_owner = self.executor_owner;
        let executor_address = self.executor_address;
        let weth_address = self.weth_address;
        let pool_manager_address = self.pool_manager_address;
        let multicall3_address = self.multicall3_address;
        let inject_code = self.inject_code;
        let injected_address = self.injected_address;
        let runtime_bytecode = self.runtime_bytecode.clone();
        let warmup = self.warmup;

        // 4. The sim body runs as a task on the dedicated runtime (see the
        //    module doc for the thread/runtime matrix). The request is cloned
        //    into the task (the outer conversions read the original after).
        let (result, buckets): (Result<Option<SimResult>, String>, FailBuckets) = {
            let req_task = req.clone();
            let sim_future = async move {
                tokio::spawn(async move {
                    // SIMPIPE2 M1 span parity: the engine-side sim gets the
                    // same Jaeger visibility the retired FFI fan-out had -
                    // named `degenbot.simulate.inline`, nested under the
                    // spawning solve worker's span context (tokio::spawn
                    // clones the current task context). One span per sim:
                    // closes the Jaeger-invisibility gap the T4 soak found.
                    let req = req_task;
                    let _sim_span = tracing::info_span!(
                        target: "degenbot::solver",
                        "degenbot.simulate.inline",
                        path_id = req.path_id,
                        sim_block = req.sim_block,
                        hops = req.hops.len(),
                        sim_ok = false,
                    )
                    .entered();
                    let ctx = SimulateContext {
                        provider: &provider,
                        executor_owner,
                        executor_address,
                        weth_address,
                        pool_manager_address,
                        multicall3_address,
                        inject_code,
                        injected_address,
                        runtime_bytecode,
                        warmup,
                        base_fee_next,
                        current_block: req.sim_block,
                        block_timestamp: req.block_timestamp,
                        block_priority_fees: None,
                    };
                    match degenbot_simulation::BlockSimHandle::build(
                        &provider,
                        base_fee_next,
                        req.sim_block,
                        req.block_timestamp,
                        &ctx.override_params(),
                        &anchor,
                        &warm_cache,
                        Some(&storage_memo),
                    ) {
                        Some(mut handle) => {
                            let mut buckets = FailBuckets::new();
                            let result = simulate_path_on_evm(
                                handle.evm_mut(),
                                &ctx,
                                &sim_path,
                                &mut buckets,
                            )
                            .map_err(|e| format!("{e}"));
                            tracing::Span::current().record(
                                "sim_ok",
                                result.as_ref().ok().and_then(|o| o.as_ref()).is_some(),
                            );
                            (result, buckets)
                        }
                        None => {
                            // No ambient runtime at build / an override error:
                            // tally `rpc-failed` (mirrors the FFI build-failure arm).
                            let mut buckets = FailBuckets::new();
                            buckets.record(
                                req.path_id,
                                "rpc-failed",
                                None,
                                Bytes::new(),
                                optimal_input,
                                outputs_vec(&req),
                            );
                            (Ok(None), buckets)
                        }
                    }
                })
                .await
                .unwrap_or_else(|e| {
                    // The sim task panicked — the FFI fan-out's exception class.
                    let mut buckets = FailBuckets::new();
                    buckets.record(
                        req.path_id,
                        "exception",
                        None,
                        Bytes::new(),
                        optimal_input,
                        outputs_vec(&req),
                    );
                    (Err(format!("{e}")), buckets)
                })
            };
            join_sim_task(&self.sim_runtime, sim_future)
        };

        // 5. Convert: Ok(Some) → success payload; Ok(None)/Err → failure
        //    payload through the buckets (one record — single path).
        match result {
            Ok(Some(sim)) => Some(Self::payload_from_sim(path_id, &sim)),
            Ok(None) | Err(_) => {
                let mut failures = buckets.into_failures();
                let f = failures.pop()?;
                Some(SimulatedPathResult {
                    path_id,
                    gross_profit: U256::ZERO,
                    net_profit: U256::ZERO,
                    gas_used: 0,
                    priority_fee: 0,
                    base_fee_next: 0,
                    execute_calldata: Vec::new(),
                    access_list: None,
                    captured_swaps: Vec::new(),
                    hop_count,
                    failure: Some(InlineSimFailure {
                        fail_index: f.fail_index,
                        revert_data: f.revert_data.to_vec(),
                        bucket: f.bucket,
                    }),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // The env var is process-global, so the cases share ONE serial test to
    // avoid the parallel-test env race (each case asserts a distinct tail).
    #[test]
    fn worker_count_env_matrix() {
        let default = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        std::env::set_var("DEGENBOT_INLINE_SIM_WORKERS", "not-a-number");
        assert_eq!(super::inline_sim_worker_count(), default);

        std::env::set_var("DEGENBOT_INLINE_SIM_WORKERS", "9999");
        assert_eq!(super::inline_sim_worker_count(), 32);

        std::env::set_var("DEGENBOT_INLINE_SIM_WORKERS", "0");
        assert_eq!(super::inline_sim_worker_count(), 1);

        std::env::set_var("DEGENBOT_INLINE_SIM_WORKERS", "6");
        assert_eq!(super::inline_sim_worker_count(), 6);

        std::env::remove_var("DEGENBOT_INLINE_SIM_WORKERS");
        assert_eq!(super::inline_sim_worker_count(), default);
    }
}
