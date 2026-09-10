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

use std::sync::atomic::Ordering;
use std::sync::mpsc;

use degenbot_workers::lane::{LaneCtx, QuitSig};

/// A pre-composed per-bin solve closure (LW-T8 ruling: the legacy adapter
/// is a MINI-SEAT array — ONE persistent named std thread per bin; NO
/// ambient runtime anywhere, so `Handle::try_current() == Err` on every
/// unit; the only out-of-band capability is the seat's `LaneCtx` port).
type Job = Box<dyn FnOnce(&LaneCtx) + Send + 'static>;

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
    /// Round-robin seat selector (the callers spawn exactly `seats` jobs in
    /// bin order, so seat seq % lanes preserves one-thread-per-bin).
    next: std::sync::atomic::AtomicU64,
    /// One mailbox per mini-seat (LW-T8 ruling: N persistent named std
    /// threads, one per bin, each with an mpsc mailbox).
    lanes: Vec<mpsc::Sender<Job>>,
}

impl crate::arb_engine::executor::Executor for SolveExecutor {
    fn bin_count(&self) -> usize {
        degenbot_core::cpu_budget::solve_worker_count()
    }

    fn submit(
        &self,
        _bin: usize,
        work: crate::arb_engine::executor::SubmitWork,
    ) -> Result<
        degenbot_workers::dispatcher::SubmitReceipt,
        degenbot_workers::dispatcher::SubmitError,
    > {
        // The legacy mini-seat adapter: seats run PLAIN (no ambient runtime)
        // with their per-seat ctx port as the ONLY out-of-band capability.
        self.spawn(move |ctx| work(ctx));
        Ok(degenbot_workers::dispatcher::SubmitReceipt {
            accepted_with_backlog: false,
        })
    }
}

impl SolveExecutor {
    /// Build the executor (LW-T8 ruling): a mini-seat array — one persistent
    /// named std thread per bin (round-robin dispatch preserves
    /// one-thread-per-bin as the callers spawn exactly `worker_threads`
    /// jobs in bin order). Plain threads: NO ambient tokio runtime — the
    /// seat's `LaneCtx` port is the only out-of-band capability, and
    /// escalations consume the injected port, never the ambient Handle.
    pub(crate) fn new(thread_name: &'static str, worker_threads: usize) -> Self {
        let thread_name = thread_name.to_string();
        let seats = worker_threads.max(1);
        let mut lanes = Vec::with_capacity(seats);
        for slot in 0..seats {
            let (stx, srx) = mpsc::channel::<Job>();
            let spawned = std::thread::Builder::new()
                .name(format!("{thread_name}-{slot}"))
                .spawn(move || {
                    // One-seat LaneCtx (ctx-mint at the executor surface,
                    // never inside a unit body): no pin, no warm arena,
                    // escalation through the injected default port.
                    let ctx = LaneCtx {
                        pin: 0,
                        arena: degenbot_workers::dispatcher::ArenaToken::DETACHED,
                        escalation: degenbot_workers::lane::default_escalation_port()
                            .unwrap_or_else(degenbot_workers::lane::no_escalation_port),
                        quit: QuitSig,
                    };
                    while let Ok(job) = srx.recv() {
                        job(&ctx);
                    }
                });
            if let Err(err) = spawned {
                abort_executor("legacy seat spawn", &format!("{err:?}"));
            }
            lanes.push(stx);
        }
        Self {
            next: std::sync::atomic::AtomicU64::new(0),
            lanes,
        }
    }

    /// Submit one job (one LPT bin). Never blocks: the seat lane is
    /// unbounded and bounded-ness comes from the caller spawning exactly n
    /// bins in order (round-robin across the mini-seat array pins each bin
    /// to its own persistent plain thread).
    pub(crate) fn spawn(&self, job: impl FnOnce(&LaneCtx) + Send + 'static) {
        let seq = self.next.fetch_add(1, Ordering::Relaxed);
        let lane =
            usize::try_from(seq % u64::try_from(self.lanes.len()).unwrap_or(u64::MAX)).unwrap_or(0);
        let _ = self.lanes[lane].send(Box::new(job));
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
        // HONESTY PIN (flipped by the LW-T8 mini-seat ruling): the legacy
        // seats are now PLAIN THREADS with per-index names — the row claims
        // the {n} pattern it actually honors.
        assert!(
            row.thread_name.contains("{n}"),
            "the per-index legacy seats must claim the {{n}} pattern: {}",
            row.thread_name
        );
        assert!(
            !row.thread_name.contains("SAME name"),
            "per-index seats must not claim same-name-for-all anymore: {}",
            row.thread_name
        );
        // And the RUNTIME side honors it: one running seat's thread name
        // matches the per-index pattern (the wedge: off-runtime + per-index).
        assert!(
            snap.iter()
                .any(|e| e.thread_name.starts_with("degenbot-solve-tokio-")),
            "the census row documents the per-index seat naming"
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
        thread_name: "degenbot-solve-tokio-{n} (legacy PLAIN-THREAD seats; escalation via the LW-T3 port)",
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
