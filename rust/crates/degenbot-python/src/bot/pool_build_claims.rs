//! Claim-based single-flight pool builds (CXKACI): Rust-side coordination.
//!
//! The Python registration pipeline (`REG_WORKERS` asyncio consumers) meets the
//! same not-yet-built pool in many candidate paths processed concurrently.
//! `Bot.build_pool`/`build_managed_pool` check the Python registry and
//! return existing pools, but two consumers can race between that pre-check
//! and the Rust registration — the loser's registration raises
//! `PoolAlreadyRegisteredError` although the pool becomes registry-reachable
//! milliseconds later, when the first builder finishes its Python-side
//! registry insertion.
//!
//! Historically the loser lost its path AND had the pool fatally memoized,
//! so every later candidate through that pool was short-circuited and the
//! registered-path total became a function of race timing (observed
//! 545,511-645,329 paths — then the full 1,000,000 cap once the race was
//! mitigated — against a byte-identical static DB; 2026-09-09: 7,973,943
//! `PoolAlreadyRegistered` skips vs 566,125 registered).
//!
//! The claim lives HERE, in the Rust core, and the Python driver stays a
//! shell around it: the first consumer of a `(family, key)` claims it and
//! leads the build; concurrent consumers await the in-flight claim and
//! share the built pool. The leader publishes the pool (or its failure) with
//! `complete`/`fail` strictly AFTER its registry insertion, so:
//! - waiters parked before publication wake with the result (a `watch`
//!   channel holds the published value — no lost-wakeup window between the
//!   `Building` check and the park);
//! - waiters whose claim window closed before they subscribed find no claim,
//!   re-run their build, and hit the registry pre-check — still correct, one
//!   redundant lookup;
//! - the claim slot is dropped on publication, so bookkeeping is bounded by
//!   concurrently-building pools, not the pool universe.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::watch;

type ClaimKey = (String, String);

/// One claim's fixed identity — installed at `try_claim`, published once.
enum ClaimState {
    /// Claimed; the leader is building.
    Building,
    /// The leader published the built pool.
    Done(Py<PyAny>),
    /// The leader published its failure (exception object).
    Failed(Py<PyAny>),
}

/// The watch payload: an `Arc` keeps publication cheap (no deep clone of
/// the enum) while parking waiters borrow the latest state.
type ClaimValue = Arc<ClaimState>;

/// One in-flight or freshly-published claim.
struct ClaimSlot {
    /// Latest claim state. `watch` guarantees any subscriber, whenever it
    /// subscribes, immediately sees the published value.
    tx: watch::Sender<ClaimValue>,
}

/// `degenbot._ffi.bot.PoolBuildClaims` — claim-based single-flight builds.
///
/// One instance per registration pipeline (per process, in practice): the
/// map is keyed by the same identity the pool registries use — V2/V3 pool
/// address, V4 pool id hash.
#[pyclass(name = "PoolBuildClaims", module = "degenbot._ffi.bot")]
pub(crate) struct PyPoolBuildClaims {
    claims: Arc<Mutex<HashMap<ClaimKey, Arc<ClaimSlot>>>>,
}

/// Python `None` as a plain fn: the method-path form fails the
/// higher-ranked `FnOnce` bound inside async blocks, and the closure form
/// trips `redundant_closure_for_method_calls` there.
#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "attached outside the async block; the method path does not satisfy the bound there"
)]
fn none_py() -> Py<PyAny> {
    Python::attach(|py| py.None())
}

/// The parked-waiter core: resolve an in-flight claim through publication.
///
/// Shared by the `wait` FFI wrapper (which hands this to `future_into_py`)
/// and the Rust unit tests (which drive it on a bare tokio runtime).
async fn resolve_claim(mut rx: watch::Receiver<ClaimValue>) -> PyResult<Py<PyAny>> {
    loop {
        // Hold a reference-but-clone it out of the borrow guard so no watch
        // borrow lives across the await below.
        let state: ClaimValue = rx.borrow_and_update().clone();
        match &*state {
            ClaimState::Building => {
                // A dropped sender (post-publication cleanup) still leaves
                // the last value readable — `changed` errs and the loop
                // falls through to re-check the retained value.
                let _ = rx.changed().await;
            }
            ClaimState::Done(pool) => {
                return Ok(Python::attach(|py| pool.clone_ref(py)));
            }
            ClaimState::Failed(exc) => {
                return Python::attach(|py| {
                    let bound = exc.clone_ref(py).into_bound(py);
                    Err(PyErr::from_value(bound))
                });
            }
        }
    }
}

#[pymethods]
impl PyPoolBuildClaims {
    #[new]
    fn new() -> Self {
        Self {
            claims: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Try to claim the build for `(family, key)` (CXKACI).
    ///
    /// `True` = caller is the leader and owns the build; `False` = a build
    /// is in flight (or just finished) and the caller should `wait`.
    #[pyo3(signature = (family, key))]
    fn try_claim(&self, family: String, key: String) -> bool {
        let mut map = self.claims.lock();
        if map.contains_key(&(family.clone(), key.clone())) {
            return false;
        }
        let (tx, _rx) = watch::channel(Arc::new(ClaimState::Building));
        map.insert((family, key), Arc::new(ClaimSlot { tx }));
        true
    }

    /// Publish the built pool for `(family, key)` (leader only, after its
    /// registry insertion) and release the claim. Parked waiters wake with
    /// the pool.
    #[pyo3(signature = (family, key, pool))]
    fn complete(&self, family: String, key: String, pool: Py<PyAny>) {
        let mut map = self.claims.lock();
        let slot = map.get(&(family.clone(), key.clone())).cloned();
        if let Some(slot) = slot {
            slot.tx.send_replace(Arc::new(ClaimState::Done(pool)));
            // Drop the slot: parked waiters keep their receivers (watch
            // retains the final value); late waiters find no claim and
            // re-run their build against the registry pre-check.
            map.remove(&(family, key));
        }
    }

    /// Publish a build failure for `(family, key)` (leader only) and release
    /// the claim. Parked waiters see the exception, and a later candidate
    /// builds fresh (failures are never fatal — the `SkipGate` notes).
    #[pyo3(signature = (family, key, error))]
    fn fail(&self, family: String, key: String, error: Py<PyAny>) {
        let mut map = self.claims.lock();
        let slot = map.get(&(family.clone(), key.clone())).cloned();
        if let Some(slot) = slot {
            slot.tx.send_replace(Arc::new(ClaimState::Failed(error)));
            map.remove(&(family, key));
        }
    }

    /// Await an in-flight claim's pool. Returns the published pool, raises
    /// the leader's failure, or returns `None` when no claim is in flight
    /// (the leader published before this waiter subscribed — re-run the
    /// build; the registry pre-check answers).
    #[pyo3(signature = (family, key))]
    fn wait<'py>(
        &self,
        py: Python<'py>,
        family: String,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let slot = self.claims.lock().get(&(family, key)).cloned();
        let Some(slot) = slot else {
            // No claim in flight: the leader published (and dropped the
            // slot) before this waiter subscribed. Still a coroutine, so
            // the caller always awaits: it resolves to None and re-runs
            // its build against the registry pre-check.
            return future_into_py(py, async move {
                // resolves to Python None: re-run the build against the
                // registry pre-check.
                Ok::<Py<PyAny>, PyErr>(none_py())
            });
        };
        let rx = slot.tx.subscribe();
        future_into_py(py, resolve_claim(rx))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used, reason = "test assertions fail loudly")]

    use std::time::Duration;

    use super::*;

    #[test]
    fn first_claimant_leads_the_second_waiter_exists() {
        let claims = PyPoolBuildClaims::new();
        assert!(claims.try_claim("v2".into(), "pool-a".into()));
        assert!(!claims.try_claim("v2".into(), "pool-a".into()));
        // Distinct keys are independent claims.
        assert!(claims.try_claim("v2".into(), "pool-b".into()));
    }

    #[test]
    fn publication_releases_the_claim() {
        let claims = PyPoolBuildClaims::new();
        assert!(claims.try_claim("v4".into(), "hash-x".into()));
        Python::attach(|py| {
            let pool: Py<PyAny> = py.None();
            claims.complete("v4".into(), "hash-x".into(), pool);
        });
        // The slot is dropped on publication: a fresh claim can start (the
        // registry pre-check makes it benign).
        assert!(claims.try_claim("v4".into(), "hash-x".into()));
    }

    #[tokio::test]
    async fn parked_waiter_resolves_with_the_published_pool() {
        let claims = PyPoolBuildClaims::new();
        assert!(claims.try_claim("v4".into(), "hash-y".into()));
        let rx = claims
            .claims
            .lock()
            .get(&("v4".into(), "hash-y".into()))
            .cloned()
            .expect("claim installed")
            .tx
            .subscribe();

        let publisher = tokio::spawn(async move {
            // Park the waiter first, then publish.
            tokio::time::sleep(Duration::from_millis(20)).await;
            Python::attach(|py| {
                let marker: Py<PyAny> = py
                    .eval(c"'claim-published-marker'", None, None)
                    .expect("marker expr")
                    .unbind();
                claims.complete("v4".into(), "hash-y".into(), marker);
            });
        });

        let resolved = resolve_claim(rx).await.expect("waiter resolves");
        let got = Python::attach(|py| resolved.extract::<String>(py).expect("the shared marker"));
        assert_eq!(got, "claim-published-marker");
        publisher.await.expect("publisher task");
    }

    #[tokio::test]
    async fn parked_waiter_sees_the_leaders_failure() {
        let claims = PyPoolBuildClaims::new();
        assert!(claims.try_claim("v2".into(), "pool-z".into()));
        let rx = claims
            .claims
            .lock()
            .get(&("v2".into(), "pool-z".into()))
            .cloned()
            .expect("claim installed")
            .tx
            .subscribe();

        Python::attach(|py| {
            let exc: Py<PyAny> = py
                .eval(c"ValueError('boom')", None, None)
                .expect("exception expr")
                .unbind();
            claims.fail("v2".into(), "pool-z".into(), exc);
        });

        let err = resolve_claim(rx).await.expect_err("failure propagates");
        Python::attach(|py| {
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }
}
