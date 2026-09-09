//! Shared Tokio runtime management.
//!
//! This module provides a singleton multi-threaded Tokio runtime instance
//! that can be shared across multiple Python-bound objects, avoiding the
//! overhead of creating a separate runtime for each contract or provider instance.
//!
//! # Why Multi-Threaded?
//!
//! The runtime uses `Builder::new_multi_thread()` rather than
//! `new_current_thread()` to support concurrent RPC calls from multiple Python
//! threads. With Python 3.13+ free-threading (no GIL), multiple threads can
//! call into Rust provider/contract methods simultaneously. A multi-threaded
//! Tokio runtime enables true parallelism for these I/O-bound operations,
//! while a current-thread runtime would serialize them into a bottleneck.
//!
//! Two-runtime sizing: the worker count comes from the cgroup-aware CPU
//! budget (`crate::cpu_budget`) — the leftover after the solve bins take
//! theirs — and NOT from `available_parallelism`, which reads 24 host cores
//! inside an 8-core cgroup quota in this devcontainer. Operators pin it
//! with the typed `runtime.io_workers` key (env `DEGENBOT_IO_WORKERS`); the
//! legacy tokio-conventional `TOKIO_WORKER_THREADS` env name is rejected at
//! config load.
//!
//! # Lazy Initialization
//!
//! The runtime is only created on first call to `get_runtime()`. Pure Rust
//! functions (`tick_math`, `decoder`, `address_utils`) never initialize
//! it, so scripts that don't use provider/contract code pay no runtime cost.
//!
//! # Usage
//!
//! ```no_run
//! use degenbot_core::runtime::get_runtime;
//!
//! let runtime = get_runtime();
//! let result = runtime.block_on(async {
//!     // async code here
//!     42
//! });
//! ```

use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn build_runtime() -> Result<Runtime, std::io::Error> {
    // SMTH6M: the cgroup-aware budget is the single sizing authority for the
    // ambient runtime (see `crate::cpu_budget::ambient_io_worker_count`).
    let workers = crate::cpu_budget::ambient_io_worker_count();
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
}

/// Get the shared Tokio runtime instance.
///
/// This function lazily initializes a multi-threaded Tokio runtime
/// on first call. Subsequent calls return the same runtime instance.
///
/// Worker thread count is sized by the cgroup-aware CPU budget
/// ([`crate::cpu_budget`]) and can be pinned explicitly with the typed
/// `runtime.io_workers` key (env `DEGENBOT_IO_WORKERS`).
///
/// # Panics
///
/// Panics if the runtime fails to create (e.g., if the system cannot
/// spawn the required threads).
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        // Targeted expect (fulfilled): a global `&'static Runtime` initializer
        // has no error channel, so a failed spawn is panic loudly, as the
        // `# Panics` doc above documents.
        #[expect(clippy::panic)]
        build_runtime().unwrap_or_else(|e| panic!("Failed to create Tokio runtime: {e}"))
    })
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_build_runtime_sizes_from_cgroup_budget_policy() {
        // SMTH6M: the ambient runtime is sized by the relocated cpu_budget
        // policy (cgroup+affinity budget, minus the solve bins, floored at
        // 1) — never from tokio's available_parallelism default, which reads
        // 24 host cores inside an 8-core cgroup quota here.
        let expected = crate::cpu_budget::ambient_io_worker_count_from(
            ::degenbot_config::holder::config().runtime.io_workers,
            crate::cpu_budget::solve_worker_count(),
            crate::cpu_budget::effective_cpu_budget(),
        );
        let rt = build_runtime().unwrap();
        assert_eq!(
            rt.metrics().num_workers(),
            expected,
            "ambient runtime workers must follow the CPU-budget policy"
        );
    }

    #[test]
    fn test_runtime_singleton() {
        let rt1 = get_runtime();
        let rt2 = get_runtime();

        assert!(std::ptr::eq(rt1, rt2));
    }

    #[test]
    fn test_runtime_can_spawn_tasks() {
        let runtime = get_runtime();

        let result = runtime.block_on(async {
            let handle = tokio::spawn(async { 42 });
            handle
                .await
                .expect("spawned task should complete successfully")
        });

        assert_eq!(result, 42);
    }
}
