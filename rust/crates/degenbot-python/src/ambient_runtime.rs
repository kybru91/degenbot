//! Python-driver seam for the shared degenbot-core ambient runtime (VJGZJ2).
//!
//! The verify seams (`aave_updater::verify_touched_positions_on_chain`,
//! `pool::verify_v3/v4_liquidity_map`) removed their per-call multi-thread
//! runtime build (the dead tokio-rt-worker churn source) and now require the
//! CALLER's ambient runtime — a missing one returns the typed VJGZJ2 error
//! ("run under the shared degenbot-core ambient runtime"). Rust consumers
//! enter the runtime naturally (they run on it); a Python driver shell has
//! no way to do that, so this module exposes the minimal primitive: call a
//! Python callable with the shared runtime entered on the calling thread.
//! The orchestration engine stays Rust-owned (AGENTS.md) — this is the thin
//! driver affordance the typed error's message invites.

use pyo3::prelude::*;

/// Call `fn_work()` with the shared degenbot-core runtime entered on the
/// calling thread.
///
/// Python drivers + tests that call an ambient-runtime-only verify seam
/// (the VJGZJ2 policy) wrap the call in this helper:
///
/// ```python
/// divergences = call_on_ambient_runtime(
///     partial(verify_touched_positions_on_chain, database_path=..., ...)
/// )
/// ```
///
/// The shared runtime singleton is created on first use (one runtime per
/// process, created deterministically); the enter guard is held for the
/// duration of the call, then the thread-local Handle is restored.
/// Re-entrant — a caller already inside a runtime just re-enters.
///
/// # Args
///
/// - `fn_work` — a zero-argument Python callable. Its return value (or
///   exception) is passed through unchanged.
///
/// # Errors
///
/// Propagates whatever error `fn_work()` raises, unchanged; `add_function`'s
/// registration of this symbol surfaces any binding failure at module init.
///
/// # Returns
///
/// Whatever `fn_work()` returns.
#[pyfunction]
pub fn call_on_ambient_runtime(
    #[expect(unused_variables)] py: Python<'_>,
    fn_work: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let _guard = degenbot_core::runtime::get_runtime().enter();
    fn_work.call0().map(Bound::unbind)
}
