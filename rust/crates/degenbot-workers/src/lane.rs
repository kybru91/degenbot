//! Seat context (LW-T2, Seam B): a lane unit takes ALL external capability
//! through a passed `LaneCtx` — never through ambient state, and seats run
//! with NO ambient tokio runtime (the `Handle::try_current() == Err` wedge
//! test pins this structurally). Pyo3-free by construction (design doc §8).

use crate::dispatcher::ArenaToken;
use crate::slot::PinKey;

/// Escalation port for a lane (LW-T3, task LWSYUF, fills the real
/// injection/bounds/determinism implementation; empty stub here).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EscalationPort;

/// Cooperative quit signal for a lane (stub; later seams refine).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuitSig;

/// The per-seat unit context, handed to the unit body by the seat — never
/// constructed ambiently. The `arena` identity is warm across cycles while
/// the pin lives and freshly minted after a T9 role switch (an arena is
/// never live across a role switch, design doc §3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneCtx {
    /// The lane's pinned bin key: the pin IS the key, so the ctx names the
    /// bin the unit is running for.
    pub pin: PinKey,
    /// The seat's warm arena token (`DETACHED` for pooled seats, which
    /// carry no warm arena).
    pub arena: ArenaToken,
    /// Escalation port (LW-T3 fills the real port).
    pub escalation: EscalationPort,
    /// Cooperative quit signal (stub).
    pub quit: QuitSig,
}

impl LaneCtx {
    /// The stub context for pooled seats (no warm arena, no pin): the
    /// pinned solve lanes hand the real pin-bound ctx at the T2 grant seam
    /// (LW-T8 unifies the pooled paths onto it).
    #[must_use]
    pub fn detached() -> Self {
        Self {
            pin: 0,
            arena: ArenaToken::DETACHED,
            escalation: EscalationPort,
            quit: QuitSig,
        }
    }
}
