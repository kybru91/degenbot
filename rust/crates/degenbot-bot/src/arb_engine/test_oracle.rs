//! TEST-ONLY oracle: the retired dirty-set machinery (epic MROOY7 task LXDY4C).
//!
//! Production deleted `DirtySets` + the `EngineSubscriber` classifier in the
//! same change that introduced the `EpochDelta` byproduct. This module keeps
//! the INCUMBENT ALGORITHM frozen verbatim (insert per hop family, atomic
//! take, BotState-bucket classification) as the derivation oracle the
//! capture-replay parity test (`records the corpus affected-path set from
//! EpochDelta == from DirtySets`) compares against. It never runs outside
//! `cargo test` and has no production callers — not a parallel
//! implementation, the retirement gate itself.

use hashbrown::HashSet;

use super::HopType;
use crate::bot_core::BotState;
use degenbot_solvers::affected_keys::AffectedKey;

/// The retired `DirtySets` three-family set, single-threaded test form.
pub(crate) struct DirtySetsOracle {
    v2: HashSet<u64>,
    v3: HashSet<u64>,
    v4: HashSet<u64>,
}

impl DirtySetsOracle {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            v2: HashSet::new(),
            v3: HashSet::new(),
            v4: HashSet::new(),
        }
    }

    /// Insert `pool_id` into the set for `hop_type` (verbatim old semantics:
    /// non-pool hop types are never dirtied).
    pub(crate) fn insert(&mut self, pool_id: u64, hop_type: HopType) {
        match hop_type {
            HopType::V2 => {
                self.v2.insert(pool_id);
            }
            HopType::V3 => {
                self.v3.insert(pool_id);
            }
            HopType::V4 => {
                self.v4.insert(pool_id);
            }
            _ => {} // Non-pool hop types are never dirtied.
        }
    }

    /// The retired `EngineSubscriber` classification: name the pool's family
    /// by consulting the shared `BotState`'s bucket (verbatim old `insert_dirty`).
    pub(crate) fn classify_insert(&mut self, core: &BotState, pool_id: u64) {
        if core.get_v2_pool_state(pool_id).is_some() {
            self.insert(pool_id, HopType::V2);
        } else if core.get_v3_pool(pool_id).is_some() {
            self.insert(pool_id, HopType::V3);
        } else if core.get_v4_pool(pool_id).is_some() {
            self.insert(pool_id, HopType::V4);
        }
        // Unregistered pool_id → no-op (no path references it).
    }

    /// Atomically take all dirty sets (verbatim old `take_all`).
    pub(crate) fn take_all(&mut self) -> (HashSet<u64>, HashSet<u64>, HashSet<u64>) {
        (
            std::mem::take(&mut self.v2),
            std::mem::take(&mut self.v3),
            std::mem::take(&mut self.v4),
        )
    }

    /// Returns `true` if all three sets are empty.
    #[must_use]
    #[expect(dead_code)] // retired-machinery parity surface (take_keys covers it)
    pub(crate) fn is_empty(&self) -> bool {
        self.v2.is_empty() && self.v3.is_empty() && self.v4.is_empty()
    }

    /// Total pending key count across the three sets (parity probe).
    #[must_use]
    pub(crate) fn total_len(&self) -> usize {
        self.v2.len() + self.v3.len() + self.v4.len()
    }

    /// Atomically take all sets, delta-shaped (sorted) — drain parity form.
    #[must_use]
    pub(crate) fn take_keys(&mut self) -> Vec<AffectedKey> {
        let (v2, v3, v4) = self.take_all();
        let mut keys: Vec<AffectedKey> = v2
            .iter()
            .map(|&p| AffectedKey::new(HopType::V2, p))
            .chain(v3.iter().map(|&p| AffectedKey::new(HopType::V3, p)))
            .chain(v4.iter().map(|&p| AffectedKey::new(HopType::V4, p)))
            .collect();
        keys.sort_unstable();
        keys
    }

    #[must_use]
    pub(crate) fn to_affected_keys(&self) -> Vec<AffectedKey> {
        let mut keys: Vec<AffectedKey> = self
            .v2
            .iter()
            .map(|&p| AffectedKey::new(HopType::V2, p))
            .chain(self.v3.iter().map(|&p| AffectedKey::new(HopType::V3, p)))
            .chain(self.v4.iter().map(|&p| AffectedKey::new(HopType::V4, p)))
            .collect();
        keys.sort_unstable();
        keys
    }
}

impl Default for DirtySetsOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert the retired three-family take into delta-shaped affected keys
/// (sorted). Lets migrated tests keep their `rebuild_and_solve_affected`
/// call shapes while the delta path is the production intake.
#[must_use]
pub(crate) fn affected_keys(
    v2: &HashSet<u64>,
    v3: &HashSet<u64>,
    v4: &HashSet<u64>,
) -> Vec<AffectedKey> {
    let mut keys: Vec<AffectedKey> = v2
        .iter()
        .map(|&p| AffectedKey::new(HopType::V2, p))
        .chain(v3.iter().map(|&p| AffectedKey::new(HopType::V3, p)))
        .chain(v4.iter().map(|&p| AffectedKey::new(HopType::V4, p)))
        .collect();
    keys.sort_unstable();
    keys
}
