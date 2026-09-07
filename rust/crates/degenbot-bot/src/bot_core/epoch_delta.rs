//! `EpochDelta` — the per-block touched-pool ledger (epic MROOY7, task LXDY4C).
//!
//! ADR-041 seam-retirement lineage: `DirtySets` + the `EngineSubscriber`
//! pool classification are retired in favor of this type. Log application
//! (`LogDispatcher::dispatch`) records the touched `(HopType, pool_id)`
//! key — the `pool_to_paths` reverse-index key, i.e. an
//! [`AffectedKey`](degenbot_solvers::affected_keys::AffectedKey) — as a
//! BYPRODUCT of `Bot::dispatch_log`, one ledger per block epoch.
//! Affected-path derivation reads the ledger directly at the drain; no
//! subscriber-side classification and no dirty-set machinery survive the
//! cutover.
//!
//! The ledger shares `DirtySets`' consumption semantics so the solve-cycle
//! behavior is unchanged: `take_keys` swaps the touched keys out atomically
//! (a drain consumes exactly what accumulated so far; later dispatches
//! accumulate into the same ledger for the next drain), and a rewind
//! RELABELS the epoch without voiding keys — matching the retired dirty
//! sets, which were never cleared on reorg (a restored pool re-notifies and
//! re-records; its pre-rewind dirt stays valid because post-rewind re-solves
//! compute against the restored state).

use hashbrown::HashSet;
use parking_lot::Mutex;

use super::epoch::Epoch;
use degenbot_solvers::affected_keys::AffectedKey;

/// The epoch's touched-pool ledger: keys recorded by log application,
/// consumed by the drain's affected-path derivation.
pub struct EpochDelta {
    epoch: parking_lot::RwLock<Epoch>,
    keys: Mutex<HashSet<AffectedKey>>,
}

impl EpochDelta {
    /// A ledger minted for `epoch`. Constructed by `Bot` and shared with
    /// the drain seam ([`super::solve_coordinator`]).
    #[must_use]
    pub fn new(epoch: impl Into<Epoch>) -> Self {
        Self {
            epoch: parking_lot::RwLock::new(epoch.into()),
            keys: Mutex::new(HashSet::new()),
        }
    }

    /// The ledger's labeled epoch (bookkeeping metadata only — keys
    /// accumulate across drains regardless of the label).
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        *self.epoch.read()
    }

    /// Relabel the ledger (a rewind or block advance). Keys are RETAINED:
    /// the touched-set is solve-cursor state, not block-window state —
    /// the retired dirty sets were never cleared on reorg either, and the
    /// reorg coordinator's per-pool restore re-records what it restored.
    pub fn set_epoch(&self, epoch: Epoch) {
        *self.epoch.write() = epoch;
    }

    /// Record one touched key (idempotent — a `HashSet` insert).
    pub fn record(&self, key: AffectedKey) {
        self.keys.lock().insert(key);
    }

    /// Convenience: record a (hop family, pool id) pair.
    pub fn record_affected(&self, hop: degenbot_solvers::mixed::HopType, pool_id: u64) {
        self.record(AffectedKey::new(hop, pool_id));
    }

    /// Atomically take every touched key (drain consumption), leaving the
    /// ledger empty for the next solve cycle — the retired
    /// `DirtySets::take_all` semantics, keys now driving the derivation.
    #[must_use]
    pub fn take_keys(&self) -> Vec<AffectedKey> {
        let taken: HashSet<AffectedKey> = std::mem::take(&mut *self.keys.lock());
        // Deterministic order (sorted) so replayed corpora compare byte-wise.
        let mut keys: Vec<AffectedKey> = taken.into_iter().collect();
        keys.sort_unstable();
        keys
    }

    /// Read WITHOUT consuming — the parity gate's non-destructive read side.
    #[must_use]
    pub fn snapshot_keys(&self) -> Vec<AffectedKey> {
        let mut keys: Vec<AffectedKey> = self.keys.lock().iter().copied().collect();
        keys.sort_unstable();
        keys
    }

    /// Returns `true` if no touched keys are pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.lock().is_empty()
    }
}

impl std::fmt::Debug for EpochDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochDelta")
            .field("epoch", &self.epoch())
            .field("pending_keys", &self.snapshot_keys().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_takes_atomically() {
        let delta = EpochDelta::new(42u64);
        delta.record_affected(degenbot_solvers::mixed::HopType::V2, 7);
        delta.record_affected(degenbot_solvers::mixed::HopType::V2, 7); // dedup
        delta.record_affected(degenbot_solvers::mixed::HopType::V3, 9);
        assert_eq!(delta.snapshot_keys().len(), 2);
        assert!(!delta.is_empty());
        let taken = delta.take_keys();
        assert_eq!(taken.len(), 2);
        assert!(delta.is_empty(), "take clears the ledger");
        // A later drain after new dispatches sees only the new keys.
        delta.record_affected(degenbot_solvers::mixed::HopType::V4, 3);
        assert_eq!(delta.take_keys().len(), 1);
    }

    #[test]
    fn rewind_relables_and_keeps_keys() {
        let delta = EpochDelta::new(10u64);
        delta.record_affected(degenbot_solvers::mixed::HopType::V2, 1);
        delta.set_epoch(Epoch::at(10).rewind_to(9));
        assert_eq!(delta.epoch(), Epoch::with_generation(9, 1));
        // Keys survive the relabel — solve-cursor state, not block-window state.
        assert_eq!(delta.take_keys().len(), 1);
    }
}
