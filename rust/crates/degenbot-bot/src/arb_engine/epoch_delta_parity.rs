//! CAPTURE-REPLAY PARITY GATE (epic MROOY7, task LXDY4C — the task's primary
//! validation gate).
//!
//! Replays the committed heavy mixed solve-capture corpus
//! (`degenbot-solvers/tests/fixtures/heavy_mixed_solve_captures.jsonl`; 369
//! production rows over 63 mainnet blocks, 76 paths, 369 V2 + 738 CL hops)
//! through `Bot::dispatch_log`, and — per corpus BLOCK — asserts that the
//! affected-path set derived from the block's `EpochDelta` (the new
//! `dispatch_log` byproduct) equals the affected-path set derived from
//! [`DirtySetsOracle`] (the frozen incumbent: classify-by-BotState-bucket
//! per notify, three family sets, atomic take — written exactly as the deleted
//! `EngineSubscriber` + `insert_dirty` behaved). Also asserts per-event
//! key-growth agreement and the drain seam's take-once consumption parity.
//!
//! The oracle never runs outside `cargo test` — it is the retirement gate
//! mandated by the task guardrail, not a parallel implementation.

#![expect(clippy::unwrap_used, clippy::expect_used)] // a fixture parse failure must stop the gate loudly

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use alloy::primitives::aliases::U112;
use alloy::primitives::{Address, Bytes, I256, U256};
use parking_lot::Mutex;
use serde_json::Value;

use super::test_oracle::DirtySetsOracle;
use super::ArbitrageEngine;
use crate::bot_core::drain_sink::DrainSink;
use crate::bot_core::engine::Engine;
use crate::bot_core::log_dispatcher::PoolStateSubscriber;
use crate::bot_core::solve_coordinator::SolveCoordinator;
use crate::bot_core::state_lock::StateLock;
use crate::bot_core::{BlockContext, BlockMetadata, Bot, BotState, RegisterV3PoolParams};
use degenbot_solvers::affected_keys::AffectedKey;
use degenbot_solvers::mixed::{HopType, PoolHop};

// ---------------------------------------------------------------------------
// Incumbent replay machinery (the retired path, frozen as a test oracle)
// ---------------------------------------------------------------------------

/// The retired `EngineSubscriber` classifier as a `PoolStateSubscriber`:
/// dirties the pool's BotState-bucket family per notify — exactly what the
/// deleted adapter + `insert_dirty` did.
struct OracleSubscriber {
    core: Arc<StateLock<BotState>>,
    oracle: Arc<Mutex<DirtySetsOracle>>,
}

impl PoolStateSubscriber for OracleSubscriber {
    fn on_pool_state_updated(&self, pool_id: u64) {
        self.oracle
            .lock()
            .classify_insert(&self.core.read(), pool_id);
    }
}

/// The affected-path set for delta-shaped keys via the engine's
/// `pool_to_paths` reverse index — the SAME derivation
/// `rebuild_and_solve_affected` performs (minus the R522XA per-path-status
/// re-check gate, which is path LIFECYCLE state, not dirty intake: both
/// sides here derive identically from the same key set).
fn affected_paths(engine: &ArbitrageEngine, keys: &[AffectedKey]) -> BTreeSet<u64> {
    keys.iter()
        .filter_map(|key| engine.pool_to_paths.get(&key.path_index_key()))
        .flatten()
        .copied()
        .collect()
}

// ---------------------------------------------------------------------------
// Synthetic log builders (shapes mirror the real decoders + the
// reorg-coordinator test builders, kept local so the gate is self-contained)
// ---------------------------------------------------------------------------

/// Deterministic per-(path, hop, family) pool address (2=V2, 3=V3, 4=V4).
fn pool_addr(path_id: u64, hop: usize, family: u8) -> Address {
    let mut a = [0u8; 20];
    a[0] = 0xC0;
    a[1] = family;
    a[2..10].copy_from_slice(&path_id.to_be_bytes());
    a[10..14].copy_from_slice(&u32::try_from(hop).unwrap_or(u32::MAX).to_be_bytes());
    Address::from(a)
}

fn rpc_log(
    address: Address,
    topics: Vec<alloy::primitives::B256>,
    data: Vec<u8>,
    block: u64,
) -> alloy::rpc::types::Log {
    alloy::rpc::types::Log {
        inner: alloy::primitives::Log::new_unchecked(address, topics, Bytes::from(data)),
        block_hash: None,
        block_number: Some(block),
        block_timestamp: None,
        transaction_hash: None,
        transaction_index: None,
        log_index: None,
        removed: false,
    }
}

/// The V2 `Sync(uint112,uint112)` topic (duplicated from the decoder).
const V2_SYNC_TOPIC: alloy::primitives::B256 =
    alloy::primitives::b256!("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1");

fn u112_word(v: u128) -> Vec<u8> {
    let mut word = [0u8; 32];
    word[16..32].copy_from_slice(&v.to_be_bytes());
    word.to_vec()
}

fn make_sync_log(
    pool_address: Address,
    reserve0: U112,
    reserve1: U112,
    block: u64,
) -> alloy::rpc::types::Log {
    let mut data = Vec::with_capacity(64);
    data.extend(u112_word(reserve0.to::<u128>()));
    data.extend(u112_word(reserve1.to::<u128>()));
    rpc_log(pool_address, vec![V2_SYNC_TOPIC], data, block)
}

fn make_v3_swap_log(
    pool_address: Address,
    sqrt_price_x96: U256,
    liquidity: u128,
    tick: i32,
    block: u64,
) -> alloy::rpc::types::Log {
    use degenbot_decoders::v3_swap_decoder::V3_SWAP_TOPIC;
    let sender = Address::from([0xbbu8; 20]);
    let recipient = Address::from([0xccu8; 20]);
    let amount0 = I256::try_from(-1_000_i128).unwrap();
    let amount1 = I256::try_from(4_000_i128).unwrap();
    let mut data = Vec::with_capacity(160);
    data.extend_from_slice(&amount0.to_be_bytes::<32>());
    data.extend_from_slice(&amount1.to_be_bytes::<32>());
    data.extend_from_slice(&sqrt_price_x96.to_be_bytes::<32>());
    data.extend(u112_word(liquidity));
    data.extend_from_slice(
        &I256::try_from(i128::from(tick))
            .unwrap()
            .to_be_bytes::<32>(),
    );
    rpc_log(
        pool_address,
        vec![V3_SWAP_TOPIC, sender.into_word(), recipient.into_word()],
        data,
        block,
    )
}

fn make_v4_swap_log(
    pool_manager: Address,
    pool_id: [u8; 32],
    sqrt_price_x96: U256,
    liquidity: u128,
    tick: i32,
    block: u64,
) -> alloy::rpc::types::Log {
    use alloy::primitives::B256;
    use degenbot_decoders::v4_swap_decoder::V4_SWAP_TOPIC;
    let sender = Address::from([0xbbu8; 20]);
    let amount0 = I256::try_from(-1_000_i128).unwrap();
    let amount1 = I256::try_from(4_000_i128).unwrap();
    let fee: u32 = 3_000;
    let mut data = Vec::with_capacity(192);
    data.extend_from_slice(&amount0.to_be_bytes::<32>());
    data.extend_from_slice(&amount1.to_be_bytes::<32>());
    data.extend_from_slice(&sqrt_price_x96.to_be_bytes::<32>());
    data.extend(u112_word(liquidity));
    data.extend_from_slice(
        &I256::try_from(i128::from(tick))
            .unwrap()
            .to_be_bytes::<32>(),
    );
    let mut fee_word = [0u8; 32];
    fee_word[28..32].copy_from_slice(&fee.to_be_bytes());
    data.extend_from_slice(&fee_word);
    rpc_log(
        pool_manager,
        vec![V4_SWAP_TOPIC, B256::from(pool_id), sender.into_word()],
        data,
        block,
    )
}

#[expect(clippy::cast_sign_loss, clippy::cast_possible_truncation)] // lossy is fine — identity parity only
fn parse_u128(v: &Value, default: u128) -> u128 {
    match v {
        Value::Number(n) => {
            let t = n.to_string();
            t.parse::<u128>()
                .ok()
                .or_else(|| t.parse::<f64>().ok().map(|f| f as u128))
                .unwrap_or(default)
        }
        Value::String(s) => s.parse::<u128>().unwrap_or(default),
        _ => default,
    }
}

fn parse_u256(v: &Value, default: U256) -> U256 {
    match v {
        Value::Number(n) => {
            let t = n.to_string();
            t.parse::<u128>().map_or(default, U256::from)
        }
        Value::String(s) => U256::from_str_radix(s, 10).unwrap_or(default),
        _ => default,
    }
}

fn parse_u112(v: &Value, default: u128) -> U112 {
    let w = parse_u128(v, default).clamp(1, u128::MAX >> 16);
    U112::try_from(w).unwrap_or(U112::from(1u64))
}

// ---------------------------------------------------------------------------
// Drain-seam fake: records every solve fan-out for the consumption check
// ---------------------------------------------------------------------------

struct RecordingEngine {
    solves: Mutex<Vec<(Vec<AffectedKey>, u64)>>,
}

impl Engine for RecordingEngine {
    fn solve_dirty(&self, affected: &[AffectedKey], block: u64, _metadata: &BlockMetadata) {
        self.solves.lock().push((affected.to_vec(), block));
    }
    fn send_result_batch(&self, _metadata: &BlockMetadata) {}
    fn finalize_block(&self, _block: u64, _metadata: &BlockMetadata) {}
    fn set_last_solved_block(&self, _block: u64) {}
    fn on_pump_ended(&self) {}
    fn set_solve_anchor(&self, _block: u64) {}
    fn record_logs_this_block(&self) {}
    fn last_processed_block(&self) -> Option<u64> {
        Some(0)
    }
}

// ---------------------------------------------------------------------------
// THE GATE
// ---------------------------------------------------------------------------

#[test]
#[expect(clippy::too_many_lines)] // the replay is one narrative
fn epoch_delta_affected_path_set_matches_dirty_sets_on_capture_corpus() {
    // --- corpus load (the committed heavy mixed capture; DBENCH_CAPTURES
    // points at a fresh capture for diagnostic runs, mirroring
    // degenbot-solvers/tests/offline_heavy_replay.rs).
    let path = std::env::var("DBENCH_CAPTURES").unwrap_or_else(|_| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../degenbot-solvers/tests/fixtures/heavy_mixed_solve_captures.jsonl")
            .to_string_lossy()
            .into_owned()
    });
    let content = degenbot_solvers::capture_fixture::read_fixture(&path);
    let rows: Vec<Value> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("capture JSON"))
        .collect();
    assert!(rows.len() >= 300, "corpus drifted: {} rows", rows.len());

    // --- shared core + engine over it (production topology, ADR-006 D1+D4).
    let bot = Arc::new(Bot::new(1));
    let mut engine = ArbitrageEngine::with_core(bot.state_arc());

    // The oracle subscriber replays the RETIRED EngineSubscriber behavior on
    // the notify stream; the production EpochDelta byproduct runs alongside.
    let oracle = Arc::new(Mutex::new(DirtySetsOracle::new()));
    let oracle_sub: Arc<dyn PoolStateSubscriber> = Arc::new(OracleSubscriber {
        core: bot.state_arc(),
        oracle: Arc::clone(&oracle),
    });
    let oracle_weak = Arc::downgrade(&oracle_sub);

    // Register every corpus path (76 unique) with per-corpus-family pools.
    let mut registered_pool_ids: Vec<u64> = Vec::new();
    let mut seen_paths: HashMap<u64, Vec<PoolHop>> = HashMap::new();
    let mut first_hops_of_path: HashMap<u64, Vec<(String, usize)>> = HashMap::new();
    for row in &rows {
        let path_id = row.get("path_id").and_then(Value::as_u64).expect("path_id");
        if seen_paths.contains_key(&path_id) {
            continue;
        }
        let hops = row.get("hops").and_then(Value::as_array).expect("hops");
        let order = row
            .get("hop_order")
            .and_then(Value::as_array)
            .expect("hop_order");
        let mut pool_hops = Vec::with_capacity(hops.len());
        let mut hop_kinds = Vec::with_capacity(hops.len());
        for (i, hop) in hops.iter().enumerate() {
            let kind = hop.get("kind").and_then(Value::as_str).expect("kind");
            let zero_for_one = order.get(i).and_then(Value::as_bool).unwrap_or(true);
            if kind == "V2" {
                let pid = engine.register_v2_pool(
                    pool_addr(path_id, i, 2),
                    U112::from(10_000u64),
                    U112::from(20_000u64),
                    997,
                    1000,
                );
                registered_pool_ids.push(pid);
                pool_hops.push(PoolHop {
                    pool_id: pid,
                    zero_for_one,
                });
            } else {
                let pid = engine.register_v3_pool(&RegisterV3PoolParams {
                    address: pool_addr(path_id, i, 3),
                    token0: Address::from([0x01u8; 20]),
                    token1: Address::from([0x02u8; 20]),
                    fee: 3000,
                    tick_spacing: 60,
                    factory: Address::from([0xf0u8; 20]),
                    sqrt_price_x96: U256::from(1u128) << 96,
                    liquidity: 1_000_000u128,
                    tick: 0,
                    tick_data: hashbrown::HashMap::new(),
                    update_block: 0,
                    tick_data_block: None,
                    coverage: crate::bot_core::PoolTickCoverage::Tracked,
                    fetcher: None,
                    ..Default::default()
                });

                registered_pool_ids.push(pid);
                pool_hops.push(PoolHop {
                    pool_id: pid,
                    zero_for_one,
                });
            }
            hop_kinds.push((kind.to_owned(), i));
        }
        let eid = engine.register_path(pool_hops).ok();
        if let Some(_eid) = eid {
            seen_paths.insert(path_id, Vec::new());
            first_hops_of_path.insert(path_id, hop_kinds);
        }
        // (A path whose resolution fails would drop out of the reverse index
        // on BOTH sides identically — parity is unaffected.)
    }
    let unique_paths = seen_paths.len();
    assert!(unique_paths >= 50, "corpus paths drifted: {unique_paths}");

    // Attach the oracle subscriber EXACTLY like the production wiring
    // (register_path's attach loop): one Weak per registered pool.
    for pid in &registered_pool_ids {
        bot.subscribe_pool_state_change(*pid, oracle_weak.clone());
    }

    // Deterministic synthetic V4 tail cycle: family coverage beyond the
    // V2+CL corpus (one registered pool, capture-derived scalars).
    let v4_manager = Address::from([0x44u8; 20]);
    let v4_pool_slot = std::cell::RefCell::new(0u64);
    let v4_id: [u8; 32] = {
        let mut a = [0xeeu8; 32];
        a[0] = 0x10;
        a
    };
    {
        let pid = bot
            .state_arc()
            .write()
            .register_v4_pool(&crate::bot_core::RegisterV4PoolParams {
                pool_manager: v4_manager,
                pool_id: v4_id,
                pool_key: crate::bot_core::V4PoolKey {
                    currency0: Address::from([0xa0u8; 20]),
                    currency1: Address::from([0xa1u8; 20]),
                    fee: 3_000,
                    tick_spacing: 60,
                    hooks: Address::from([0xf0u8; 20]),
                },
                hook_flags: 0,
                protocol_fee: 0,
                sqrt_price_x96: U256::from(1u128) << 96,
                liquidity: 1_000_000,
                tick: 0,
                tick_data: hashbrown::HashMap::new(),
                update_block: 0,
                tick_data_block: None,
                coverage: crate::bot_core::PoolTickCoverage::Sparse,
                fetcher: None,
            })
            .expect("synthetic V4 registers");
        *v4_pool_slot.borrow_mut() = pid;
        bot.subscribe_pool_state_change(pid, oracle_weak.clone());
    }

    // --- replay: rows grouped by block, ascending; per-block parity. One
    // synthetic V4 tail cycle (block = last+1) extends V-family coverage.
    let mut by_block: BTreeMap<u64, Vec<(u64, &Value)>> = BTreeMap::new();
    let mut last_block = 0u64;
    for row in &rows {
        let block = row.get("block").and_then(Value::as_u64).expect("block");
        let path_id = row.get("path_id").and_then(Value::as_u64).expect("path_id");
        last_block = last_block.max(block);
        by_block.entry(block).or_default().push((path_id, row));
    }
    let v4_block = last_block + 1;
    // (deterministic capture-derived scalars for the synthetic V4 event)
    let cl0 = rows.iter().find_map(|r| {
        r.get("hops").and_then(Value::as_array).and_then(|h| {
            h.iter()
                .find(|h| h.get("kind").and_then(Value::as_str) == Some("CL"))
                .cloned()
        })
    });
    let (v4_sqrt, v4_liq) = cl0
        .as_ref()
        .and_then(|h| {
            h.get("ranges")
                .and_then(Value::as_array)
                .map(|r| r[0].clone())
        })
        .map_or((U256::from(1u128) << 96, 1_000_000), |rng| {
            (
                parse_u256(
                    rng.get("sqrt_price_x96").unwrap_or(&Value::Null),
                    U256::from(1u128) << 96,
                ),
                parse_u128(rng.get("liquidity").unwrap_or(&Value::Null), 1_000_000),
            )
        });
    let v4_pool_id: u64 = *v4_pool_slot.borrow();
    assert!(
        by_block.len() >= 50,
        "corpus blocks drifted: {}",
        by_block.len()
    );

    let last_report = Mutex::new((0u64, 0usize, 0usize)); // (block, keys, paths) evidence
    for (block, entries) in &by_block {
        for (row_idx, (path_id, row)) in entries.iter().enumerate() {
            let hop_kinds = first_hops_of_path
                .get(path_id)
                .expect("path was seen during registration");
            let hops = row.get("hops").and_then(Value::as_array).expect("hops");
            for (i, (kind, hop_idx)) in hop_kinds.iter().enumerate() {
                let hop = &hops[*hop_idx];
                let ranges = hop.get("ranges").and_then(Value::as_array);
                let cl_range = ranges.and_then(|r| r.first());
                let sqrt = cl_range.map_or(U256::from(1u128) << 96, |r| {
                    parse_u256(
                        r.get("sqrt_price_x96").unwrap_or(&Value::Null),
                        U256::from(1u128) << 96,
                    )
                });
                let liq = cl_range.map_or(1_000_000, |r| {
                    parse_u128(r.get("liquidity").unwrap_or(&Value::Null), 1_000_000)
                });
                let before = oracle.lock().total_len();
                let before_delta = bot.active_delta().snapshot_keys().len();
                match kind.as_str() {
                    "V2" => {
                        let r0 = parse_u112(hop.get("reserve_in").unwrap_or(&Value::Null), 10_000);
                        let r1 = parse_u112(hop.get("reserve_out").unwrap_or(&Value::Null), 20_000);
                        bot.dispatch_log(&make_sync_log(
                            pool_addr(*path_id, *hop_idx, 2),
                            r0,
                            r1,
                            *block,
                        ));
                    }
                    _ => {
                        bot.dispatch_log(&make_v3_swap_log(
                            pool_addr(*path_id, *hop_idx, 3),
                            sqrt,
                            liq,
                            0,
                            *block,
                        ));
                    }
                }
                let after = oracle.lock().total_len();
                let after_delta = bot.active_delta().snapshot_keys().len();
                // PER-EVENT parity: an applied refresh grows BOTH ledgers by
                // the same pools; an apply-miss grows neither.
                assert_eq!(
                    after - before,
                    after_delta - before_delta,
                    "per-event key growth diverged at block {block} row {row_idx} hop {i} ({kind})"
                );
            }
        }
        // PER-BLOCK PARITY: affected-path set from the EpochDelta byproduct
        // == from the DirtySets oracle, on the SAME replayed stream.
        let delta_keys = bot.active_delta().snapshot_keys();
        let oracle_keys = oracle.lock().to_affected_keys();
        let delta_paths = affected_paths(&engine, &delta_keys);
        let oracle_paths = affected_paths(&engine, &oracle_keys);
        assert_eq!(
            delta_paths, oracle_paths,
            "affected-path parity broke at corpus block {block}"
        );
        // Drain ONCE per block: the take semantics are byte-identical too.
        let taken_delta = bot.active_delta().take_keys();
        let taken_oracle: Vec<AffectedKey> = oracle.lock().take_keys();
        assert_eq!(
            taken_delta, taken_oracle,
            "take parity broke at block {block}"
        );
        assert!(
            bot.active_delta().is_empty(),
            "drain clears the ledger at {block}"
        );
        *last_report.lock() = (*block, delta_keys.len(), delta_paths.len());
    }

    // --- synthetic V4 tail cycle (family coverage; same parity contract).
    bot.dispatch_log(&make_v4_swap_log(
        v4_manager, v4_id, v4_sqrt, v4_liq, 0, v4_block,
    ));
    {
        let delta_keys = bot.active_delta().snapshot_keys();
        let oracle_keys = oracle.lock().to_affected_keys();
        assert_eq!(
            affected_paths(&engine, &delta_keys),
            affected_paths(&engine, &oracle_keys),
            "V4 parity broke at the synthetic tail block {v4_block}"
        );
        // Per-event: BOTH ledgers saw the same single V4 key.
        assert_eq!(
            delta_keys, oracle_keys,
            "V4 key parity broke at the synthetic tail block {v4_block}"
        );
        assert!(delta_keys
            .iter()
            .any(|k| k.hop() == HopType::V4 && k.pool_id() == v4_pool_id));
        // The tail cycle also drains like a corpus block.
        let taken = bot.active_delta().take_keys();
        let taken_oracle: Vec<AffectedKey> = oracle.lock().take_keys();
        assert_eq!(taken, taken_oracle, "V4 take parity broke");
    }

    // --- drain-seam consumption (the coordinator fan-out): the delta's
    // taken keys ARE what every engine's solve receives, and has_dirty
    // gates as the ledger's emptiness — replayed end-to-end once.
    let recording = Arc::new(RecordingEngine {
        solves: Mutex::new(Vec::new()),
    });
    let coordinator = SolveCoordinator::new(vec![Arc::clone(&recording) as Arc<dyn Engine>]);
    coordinator.set_delta(bot.active_delta());
    assert!(!coordinator.has_dirty_paths());
    oracle.lock().insert(0x0BAD_F00D, HopType::V2); // simulate one further applied event
    bot.active_delta().record_affected(HopType::V2, 0x0BAD_F00D);
    assert!(coordinator.has_dirty_paths());
    let ctx = BlockContext::new(7u64, BlockMetadata::default());
    coordinator.on_drain(&ctx);
    let solved = recording.solves.lock().clone();
    assert_eq!(solved.len(), 1, "one drain -> one engine fan-out");
    let (solved, solved_block) = solved.into_iter().next().expect("drain fanned out");
    assert_eq!(solved_block, 7);
    assert_eq!(solved, vec![AffectedKey::new(HopType::V2, 0x0BAD_F00D)]);
    assert!(!coordinator.has_dirty_paths());

    let (final_block, final_keys, final_paths) = *last_report.lock();
    // Gate evidence (recorded in the ergo task result): 63 corpus blocks +
    // the synthetic V4 tail cycle; final corpus cycle 1 key -> 1 affected
    // path; {unique_paths} corpus paths; V2+V3+V4 mixed families; per-block
    // affected-path parity green throughout, take parity byte-identical.
    let _ = (
        final_block,
        final_keys,
        final_paths,
        unique_paths,
        by_block.len(),
    );
}
