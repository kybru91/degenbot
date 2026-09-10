//! The dedicated solve executor (epic BXUSGL T1): a private, CPU-bound tokio
//! runtime running on named threads sized from the cgroup CPU budget
//! (`cpu_budget::solve_worker_count`).
//!
//! ## No self-nicing (VPD5ZH follow-up)
//!
//! Workers run at DEFAULT OS priority. The IOx-style nice(10) was removed:
//! nice only arbitrates *within* a CPU budget, and under cgroup CFS
//! throttling - the actual root cause of heavy-cycle stalls - the freeze
//! hits every thread equally, then requeues niced workers BEHIND the I/O
//! threads after each unthrottle, stretching steal further. The quota is
//! respected structurally now (budget minus headroom), so priority games
//! only cost.
//!
//! ## Why a dedicated runtime (not a shared-pool job)
//!
//! The I/O side of the bot (block clock, RPC, the Python result bridge) runs
//! on the process's main tokio runtime. The solve fan-out is pure CPU
//! (per-path solves, seconds at the heavy end). Running CPU jobs on the
//! I/O runtime — or letting an unprioritized CPU pool contend with it for
//! cores — is exactly the "same runtime for I/O and CPU" hazard the Tokio
//! docs warn about. The `InfluxDB IOx` `DedicatedExecutor` pattern (alamb's
//! thenewstack.io article + gist; the same technique behind tpchgen PR
//! `#34`'s bounded-parallelism choice over Rayon: "I couldn't find any way
//! [with Rayon] to limit the number of things that were buffered at once")
//! hosts CPU tasks on a SEPARATE multi-thread runtime so latency-critical
//! I/O tasks never queue behind a CPU burst.
//!
//! ## Bounded in-flight parallelism
//!
//! The caller spawns exactly `n_threads` jobs — one per LPT bin, each
//! pinning one persistent worker for the whole bin (no splitting, no
//! stealing: RAYPAR T3 semantics, warm L1/L2 and allocator arenas). Results
//! stream back over a channel as each PATH completes (per-path sends, not
//! per-bin), so the drain merges fast paths while slow bins still run.

use std::sync::mpsc;

/// A pre-composed per-bin solve closure. The sync CPU work inside the async
/// block is BY DESIGN: the "never block a runtime worker without .await"
/// rule applies to the shared I/O runtime, and this is deliberately not it
/// (see module docs). Workers never yield mid-bin.
type Job = Box<dyn FnOnce() + Send + 'static>;

/// Loud, unrecoverable executor failure (mirror of the ADR-021 tripwire's
/// abort discipline): a dead executor would deadlock the first solve — its
/// per-path sends would land in a pipe nobody drains — so swallowing the
/// error is never an option.
#[expect(
    clippy::print_stderr,
    reason = "the abort path must stay legible with no tracing subscriber installed (test harnesses drop the tracing event); stderr is the process's last message"
)]
fn abort_executor(context: &str, err: &str) -> ! {
    tracing::error!(context = %context, error = %err, "[solve-executor] unrecoverable - aborting");
    eprintln!("[solve-executor] UNRECOVERABLE, aborting: {context}: {err}");
    std::process::abort();
}

pub(crate) struct SolveExecutor {
    tx: mpsc::Sender<Job>,
}

impl SolveExecutor {
    /// Build the executor: one host thread installs a private multi-thread
    /// runtime and pulls jobs off the request channel onto it.
    pub(crate) fn new(thread_name: &'static str, worker_threads: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Job>();
        let thread_name = thread_name.to_string();
        let spawned = std::thread::Builder::new()
            .name(format!("{thread_name}-host"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name(thread_name)
                    .worker_threads(worker_threads.max(1))
                    .build();
                let runtime = match runtime {
                    Ok(rt) => rt,
                    Err(err) => abort_executor("runtime build", &err.to_string()),
                };
                runtime.block_on(async move {
                    while let Ok(job) = rx.recv() {
                        tokio::spawn(async move { job() });
                    }
                    // Channel closed (host sends dropped): shut down. Jobs in
                    // flight finish; spawned tasks are awaited by the runtime
                    // on its normal shutdown path.
                });
            });
        if let Err(err) = spawned {
            abort_executor("host thread spawn", &format!("{err:?}"));
        }
        Self { tx }
    }

    /// Submit one job (one LPT bin). Never blocks: the channel is unbounded
    /// and bounded-ness comes from the caller spawning exactly n bins.
    pub(crate) fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        // Send errors only when the host is gone (process exiting mid-solve):
        // nothing can drain results then anyway.
        let _ = self.tx.send(Box::new(job));
    }
}

/// LW-T6 (Seam G2): the LEGACY tokio-stance fleet exists while the cutover
/// runs (LW-T9 deletes it). Its census row must be HONEST: same-name-for-all
/// workers is stated in the row itself, never hidden behind an `{n}` pattern
/// the runtime does not honor (GOQWCL: honesty > elegance, dumps stay
/// explainable).
#[cfg(test)]
#[expect(clippy::expect_used)]
mod census_honesty_tests {
    #[test]
    fn legacy_tokio_census_row_names_its_same_name_multiplicity_honestly() {
        // First use registers the legacy row (the row exists even if the
        // runtime build aborts loudly — see global_solve_executor).
        let _executor = super::global_solve_executor();
        let snap = degenbot_core::worker_census::snapshot();
        let row = snap
            .iter()
            .find(|e| e.resource == "solve_executor_fleet")
            .expect("the legacy tokio census row exists");
        // HONESTY PIN: the row says the workers share one name, and does NOT
        // claim an `{n}` per-index pattern the legacy runtime never honors.
        assert!(
            row.thread_name.contains("SAME name"),
            "the legacy row must state its same-name-for-all multiplicity: {}",
            row.thread_name
        );
        assert!(
            !row.thread_name.contains("{n}"),
            "a non-per-index runtime must not claim the {{n}} pattern: {}",
            row.thread_name
        );
    }
}

static SOLVE_EXECUTOR: std::sync::OnceLock<SolveExecutor> = std::sync::OnceLock::new();

/// The process-wide solve executor, built lazily on the first tokio-stance
/// solve and persisting for the process lifetime (mirroring the retired
/// pool's construction-once contract: persistent workers keep warm L1/L2 +
/// allocator arenas across drains).
pub(crate) fn global_solve_executor() -> &'static SolveExecutor {
    // PE4FPM: self-register the fleet (upsert-idempotent; the OnceLock below
    // guards re-entry, the census call is deliberately outside it so the row
    // exists even if build aborts loudly).
    degenbot_core::worker_census::register(degenbot_core::worker_census::WorkerCensusEntry {
        resource: "solve_executor_fleet",
        kind: "tokio multi-thread runtime (solve bins — the LPT-pin fleet host)",
        count: degenbot_core::cpu_budget::solve_worker_count(),
        thread_name: "degenbot-solve-tokio (SAME name for all workers + the -host dispatcher; per-index naming lands at the fleet cutover — LW-T9)",
        sizing: "one persistent worker per cpu_budget::solve_worker_count (cgroup budget minus headroom); DEGENBOT_SOLVE_CPUS override (VPD5ZH)",
    });
    SOLVE_EXECUTOR.get_or_init(|| {
        // Match the LPT bin count the engine computes at dispatch time
        // (cpu_budget::solve_worker_count, quota-derived): one runtime can
        // never host fewer workers than there are bins. The retired rayon
        // global pool was the only wider pool; no dispatch arm outgrows these.
        // VPD5ZH: budget from the cgroup quota (not a pool width) - an 8-bin
        // fleet on a quota-capped container froze the whole process under CFS
        // throttling whenever solve bursts overlapped I/O. Headroom (default
        // 2 CPUs) is left for the main runtime, Python, pump, and exporter;
        // DEGENBOT_SOLVE_CPUS overrides.
        SolveExecutor::new(
            "degenbot-solve-tokio",
            degenbot_core::cpu_budget::solve_worker_count(),
        )
    })
}
