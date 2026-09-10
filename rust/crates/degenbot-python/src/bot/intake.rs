//! The fleet registration intake's `PyO3` surface (PRG-3): the Python
//! driver submits its pool-build callables as fleet units to the
//! `PoolStateUpdater` intake executor and joins each unit's receipt. The
//! legacy stance never sees this module — the `c_api` register site gates
//! it on the installed fleet boot (construction-time stance like the
//! executor field, never read per call).
//!
//! GIL cadence (mirrors the incumbent worker threads): the seat attaches
//! once to invoke the callable; the callable's Rust-builder sections
//! release the GIL through the existing `py.detach` seams, so pooled
//! seats run concurrently through the Rust core — the identical runtime
//! the legacy `ThreadPoolExecutor` provided (parity gate).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;

/// The seat→waiter outcome (`Send` because `Py<T>` and `PyErr` are).
type IntakeOutcome = Result<Py<PyAny>, PyErr>;

/// One submitted intake unit's completion handle. `done()` probes
/// completion cheaply, `result()` delivers the callable's return value
/// or re-raises its exception (repeatable — the outcome is stored, not
/// consumed), `wait()` is the blocking GIL-detached join, and
/// `wait_async()` is the asyncio-native awaitable on the shared runtime.
#[pyclass(name = "IntakeReceipt", module = "degenbot._ffi")]
pub struct PyIntakeReceipt {
    /// The stored outcome (one unit, one delivery — repeatable reads).
    outcome: Arc<Mutex<Option<IntakeOutcome>>>,
    /// The completion signal: one `()` delivery after the outcome lands
    /// (`!Sync` receiver gated behind a mutex; `Arc` so `wait_async` can
    /// move a clone into the blocking task).
    signal_rx: Arc<Mutex<Receiver<()>>>,
    /// Set by the seat right after the outcome lands — a cheap probe for
    /// the driver (no parked waiter thread per unit).
    done: Arc<AtomicBool>,
}

impl PyIntakeReceipt {
    fn lock_outcome(&self) -> std::sync::MutexGuard<'_, Option<IntakeOutcome>> {
        self.outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[pymethods]
impl PyIntakeReceipt {
    /// Non-blocking probe: has the unit completed? The driver's asyncio
    /// join polls this (one relaxed atomic load) instead of parking a
    /// waiter thread per in-flight build.
    #[must_use]
    pub fn done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    /// The unit's result (call once [`Self::done`] turns true). Raises the
    /// callable's exception if the build failed.
    ///
    /// # Errors
    /// `RuntimeError` when the unit has not completed yet; the callable's
    /// own exception on build failure.
    pub fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.lock_outcome();
        match &*guard {
            Some(Ok(value)) => Ok(value.clone_ref(py)),
            Some(Err(err)) => Err(err.clone_ref(py)),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "intake unit has not completed yet (poll done() first)",
            )),
        }
    }

    /// Blocking join (legacy-future parity). The parked recv runs
    /// GIL-DETACHED — a waiter holding the GIL would deadlock its own
    /// unit (the seat's `Python::attach` could never acquire it).
    ///
    /// # Errors
    /// `TimeoutError` when the unit did not complete in time; the
    /// callable's own exception on build failure.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let outcome = py.detach(|| {
            let signal_rx = self
                .signal_rx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match timeout {
                Some(secs) => {
                    let dur = Duration::try_from_secs_f64(secs.max(0.0))
                        .unwrap_or(Duration::from_secs(1));
                    signal_rx.recv_timeout(dur).map_err(|recv_err| {
                        pyo3::exceptions::PyTimeoutError::new_err(format!(
                            "intake unit did not complete in {secs}s: {recv_err}"
                        ))
                    })
                }
                None => signal_rx.recv().map_err(|recv_err| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "intake executor dropped the receipt channel: {recv_err}"
                    ))
                }),
            }
        });
        outcome?;
        self.result(py)
    }

    /// The asyncio-native join: an awaitable that resolves on the shared
    /// tokio runtime when the unit completes (no parked waiter thread, no
    /// poll loop — the driver awaits it directly).
    ///
    /// # Errors
    /// `RuntimeError` when the signal channel dropped without a delivery
    /// (executor died); the callable's own exception on build failure.
    fn wait_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let signal_rx = Arc::clone(&self.signal_rx);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let delivery = tokio::task::spawn_blocking(move || {
                let guard = signal_rx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.recv()
            })
            .await
            .map_err(|join_err| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "intake receipt join failed: {join_err}"
                ))
            })?;
            delivery.map_err(|recv_err| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "intake executor dropped the receipt channel: {recv_err}"
                ))
            })
        })
    }

    #[expect(
        clippy::unused_self,
        reason = "__repr__ is a pyo3 protocol method — the receiver is part of the protocol shape"
    )]
    fn __repr__(&self) -> &'static str {
        "IntakeReceipt()"
    }
}

/// Submit one pool-build callable to the fleet intake executor (PRG-3).
/// The callable runs on a pooled `work-fleet-poolupd-{n}` seat; its slot
/// grant is bounded by the budget's `pool_state_updater_slots` and its
/// admission rides the Deferrable cordon class. Never drops: a full
/// per-role queue spills to the executor's FIFO backlog.
#[must_use]
pub fn submit(fn_work: Py<PyAny>) -> PyIntakeReceipt {
    let (sig_tx, sig_rx) = std::sync::mpsc::channel::<()>();
    let receipt = PyIntakeReceipt {
        outcome: Arc::new(Mutex::new(None)),
        signal_rx: Arc::new(Mutex::new(sig_rx)),
        done: Arc::new(AtomicBool::new(false)),
    };
    let done = Arc::clone(&receipt.done);
    let outcome_slot = Arc::clone(&receipt.outcome);
    degenbot_bot::fleet_intake::registration_intake().spawn(Box::new(move || {
        let outcome = Python::attach(|py| {
            // GOQWCL: propagate the seat's Rust thread name into Python —
            // an anonymous C thread registers as `Dummy-N`, hiding which
            // fleet seat executed the unit (py-spy/operator
            // greppability). The per-call rename is idempotent.
            let seat = std::thread::current();
            if let Some(name) = seat.name() {
                let _ = py
                    .import("threading")
                    .and_then(|m| m.call_method0("current_thread"))
                    .and_then(|t| t.call_method1("setName", (name,)));
            }
            fn_work.call0(py)
        });
        *outcome_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
        done.store(true, Ordering::Relaxed);
        // A waiting driver may be gone (cancelled task): log it, never
        // crash a warm seat.
        if sig_tx.send(()).is_err() {
            tracing::warn!(
                target: "degenbot::fleet",
                "[fleet-reg] intake unit completed with no waiting consumer"
            );
        }
    }));
    receipt
}
