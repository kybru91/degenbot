//! `PoolStateSubscriber` adapter wrapping a shared `ArbitrageEngine` (ADR-006 D4).
//!
//! The engine is shared as `Arc<Mutex<ArbitrageEngine>>` (the pump + Python both
//! hold clones). `EngineSubscriber` upgrades that to a subscriber: when
//! `Bot`'s `LogDispatcher` notifies `on_pool_state_updated(pool_id)`, this
//! adapter UPGRADES ITS WEAK (liveness) and nothing else.
//!
//! Epic MROOY7 task LXDY4C cut the adapter down to this role. Previously it
//! classified the pool's hop family via `BotState` bucket lookups and wrote
//! the shared `DirtySets` — both retired: pool classification now comes from
//! the DECODE (event family is known at log-application time) and touched
//! pools are recorded into the block's `EpochDelta` as a byproduct of
//! `Bot::dispatch_log`. The adapter survives ONLY as the dispatcher's
//! liveness probe — the seam-retirement task (SZJUKL) deletes it entirely
//! and parks a thin Published-edge adapter here instead.
//!
//! RAYPAR engine-shard T3 (C42WKO, superseded): the dirty-write lock-order
//! notes belong to that retired machinery.

use std::sync::Weak;

use parking_lot::Mutex;

use crate::arb_engine::ArbitrageEngine;
use crate::bot_core::log_dispatcher::PoolStateSubscriber;

/// A `PoolStateSubscriber` backed by a shared `ArbitrageEngine`.
///
/// Constructed from a `Weak<Mutex<ArbitrageEngine>>` so a de-registered engine
/// (all strong handles dropped) is silently skipped by the dispatcher's
/// `Weak::upgrade` — no leak, no panic. `Bot.attach_engine` receives this as a
/// `Weak<dyn PoolStateSubscriber>`.
///
/// Liveness only: no pool classification, no dirty writes. The delta records
/// in `LogDispatcher::dispatch` / `Bot::notify_pool_state_changed`.
pub struct EngineSubscriber {
    engine: Weak<Mutex<ArbitrageEngine>>,
}

impl EngineSubscriber {
    /// Construct from a weak reference to the shared engine.
    #[must_use]
    pub(crate) fn new(engine: Weak<Mutex<ArbitrageEngine>>) -> Self {
        Self { engine }
    }
}

impl PoolStateSubscriber for EngineSubscriber {
    fn on_pool_state_updated(&self, _pool_id: u64) {
        // Liveness check only: if the engine is gone, upgrade returns None
        // and the dispatcher's Weak fan-out skips this subscriber. This
        // upgrade does NOT lock the engine Mutex — it just checks the Arc
        // strong count. No classification, no dirty writes (LXDY4C).
        if self.engine.upgrade().is_none() {
            // liveness-only: nothing to do; the dispatcher's Weak fan-out
            // already skipped the dead engine.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arb_engine::ArbitrageEngine;
    use std::sync::Arc;

    /// A live engine → the adapter upgrades (liveness) and does not panic.
    /// Classification and dirty-writing are GONE — the delta byproduct owns
    /// touched-pool tracking now, so the subscriber idles.
    #[test]
    fn adapter_upgrades_the_live_engine_and_does_nothing_else() {
        let engine = Arc::new(Mutex::new(ArbitrageEngine::new()));
        let subscriber = EngineSubscriber::new(Arc::downgrade(&engine));

        // Must not panic; no dirty/delta side effects exist on this type.
        subscriber.on_pool_state_updated(42);
    }

    /// A dead weak (engine dropped) → `on_pool_state_updated` silently no-ops.
    #[test]
    fn adapter_silently_skips_dropped_engine() {
        let engine = Arc::new(Mutex::new(ArbitrageEngine::new()));
        let subscriber = EngineSubscriber::new(Arc::downgrade(&engine));
        // Intentionally drop the engine AFTER constructing the subscriber.
        drop(engine);
        // Must not panic.
        subscriber.on_pool_state_updated(42);
    }
}
