# Test-touched process-global state — audit

Epic `64ZQLA` (host-shape independence) task `I4EJ4N`. Systematic pass over
every piece of process-global state that the Rust test suites touch, after two
2026-09 failures that only fired on a raw 24-core host while staying green in
the devcontainer:

- `ccc148275` — parity test binned at `solve_worker_count()` (host cores)
  against hermetically booted fleet seats → 22 bins on 6 seats → fail-loud
  abort at the T2 grant;
- `50b76dd24` — the state-lock dump test asserted the global `ACTIVE_READS`
  table renders `"(none)"` while parallel strangers registered foreign holds
  inside its diagnostic window.

The class of bug: **a test's observable outcome depends on (a) another test's
window into a shared global, or (b) machine-derived sizing**. Container
parallelism happened to keep both latent.

## Method and coverage

`rg` over `rust/crates/**` for `static`/`LazyLock`/`OnceLock` declarations
(219 total sites scanned; ~90 are mutable-relevant), plus env reads
(`degenbot-config/tests/no_stray_env_reads.rs` already enforces the env-read
inventory), plus `available_parallelism`/cgroup/`Instant::now` reachability.
Each mutable, test-touchable site is classified:

- `safe-serialized` — every concurrent consumer sits behind one gate that
  actually covers all walkers of the value.
- `racy-by-parallelism` — parallel test threads may observe/mutate within a
  test's window; outcome depends on scheduling.
- `host-shape-coupled` — the value or a threshold derives from
  `available_parallelism`, cgroup files, core counts, or clocks.

Dispositions: `leave` (safe or already mitigated; reason recorded), `gate`
(add serialization), `inject` (parameterize the seam), or `property`
(covered by a suite under this epic).

## Findings

### degenbot-bot

| Site | Kind | Classification | Disposition |
|---|---|---|---|
| `bot_core/state_lock.rs` `DIAG`/`WARN_THRESHOLD_MS`/`TRACE_BACKTRACES` — global flag flips inside the state-lock diag tests | shared register-enable window | racy-by-parallelism (mitigated) | **leave** — `50b76dd24` made the only stranger-sensitive assertion own-row-tolerant; residual foreign effects are warning logs only. The `test_serial` gate covers all `state_lock` diag tests against each other. |
| `bot_core/state_lock.rs` `ACTIVE_READS`/`ACTIVE_WRITES`/`SLOW_READ_DROPS` | shared hold registry | racy-by-parallelism (mitigated) | **leave** — same ownership pattern (own-key assertions; tolerant fn is the accepted depth). |
| `failure_policy.rs:166` `OVERRIDES` — `installed_override_governs_action` installs `verify_mismatch -> Observe` into a process-global `OnceLock` | shared decision-table override | racy-by-parallelism (latent, benign today) | **leave** — the only overridden bucket (`verify_mismatch`) is asserted by no other parallel reader (`matrix_rows_match_adr_040_table` asserts `severity`, not `action`, for it), and the `FIRST` OnceLock keeps the install single-shot. **Standing rule** recorded here: any future test asserting `action()`/`bucket()` on a bucket another test may override must assert its own bucket only, or the override must ride a typed config. |
| `failure_policy.rs:300` `COOLDOWNS` | keyed cooldown registry | safe-serialized | **leave** — tests use independent keys (`cooldown_keys_are_independent`); registry keyed, no cross-key reads. |
| `allocator_ctrl.rs` `AUTO_ENABLED`/`INIT_DONE`/`VERSION_OK` | global stance + one-shot logging | safe-serialized (by ignore) | **leave** — the two env/singleton-touching tests are `#[ignore]` with a "run alone" contract; the default-track tests never toggle the flag. |
| `arb_engine/solver_dispatch.rs:203` `SOLVE_FLEET_HOSTED` + the three `fleet_*_executor.rs` `FLEET_*_BOOT`/`*_EXECUTOR` `OnceLock`s | global construction stance + first-writer-wins boot descriptor | **racy-by-parallelism** | **inject — fix task filed** (ergo under epic `64ZQLA`). `install_engine_stances` mutates the global before a construction reads it (TOCTOU): between the fleet-stance test's `store(true)` and its restore of `false` (`engine_stages.rs:507`), any parallel test constructing an `ArbEngine` binds fleet-hosted solving and the once-installed fleet boot. Engine construction should derive its stance from the **caller's own cfg** (inject), not from a process-global. |
| `arb_engine/sim_slots.rs` `CAP`/`SLOTS` | first-init defaults | safe-serialized | **leave** — no test installs; all consumers accept the first-initialized default, which is the prod default. |
| `metrics.rs:165` `GLOBAL`, `otel.rs:85` `HANDLE`, `instruments.rs:961` `PIPELINE` | init-once telemetry singletons | safe-serialized | **leave** — no parallel-asserting tests found; init is idempotent; extra instrumentation from strangers is unobservable in assertions today. |
| `arb_engine/solver_dispatch.rs:1814`, `solve_executor.rs:118,134` — `solve_worker_count()` as prod sizing input under the legacy (non-fleet) stance | host-shape-coupled prod sizing | host-shape-coupled | **property** — covered by `CVURM7`/`TTANQJ` (the quota→slot authorities) and structurally by `ccc148275` (bins bind at the executor's own seat count). |

### degenbot-core

| Site | Kind | Classification | Disposition |
|---|---|---|---|
| `cpu_budget.rs:219,277` `SOLVE_WORKERS`/`AMBIENT_WORKERS` `OnceLock`s — cached host-derived counts | host-derived cache | host-shape-coupled | **property + inject** — the pure `*_from_with_roots`/`solve_worker_count_from` seams exist and are the sanctioned test surface (`CVURM7`); no test may assert a host-derived count. |
| `worker_census.rs` `CENSUS`/`BOOT_DUMPED`/`EXPORT_HOOK` | append registry + one-shot dump | safe-serialized | **leave** — registry is mutex-guarded; tests assert *contains/sorted* (strangers benign); the boot dump is logging only. |
| `runtime.rs:46` `RUNTIME` (`OnceLock<Runtime>`), `IO_RT_SEQ` | init-once runtime singleton | host-shape-coupled (io worker count) | **leave** — sized from the ambient budget once; no test asserts its width. |

### degenbot-workers, degenbot-simulation, other crates

| Site | Kind | Classification | Disposition |
|---|---|---|---|
| `degenbot-workers/gauges.rs` `DASHBOARD_HOOK` | init-once hook | safe-serialized | **leave** — hook install is idempotent, no test asserts a specific hook. |
| `degenbot-simulation/sim/evm/serving.rs` `SERVE_ENABLED` + `SERVE_TEST_GUARD`; `divergence_probe.rs` `PROBE_ENABLED`/`TALLY` + `TALLY_TEST_GUARD` | global stance + shared tally | safe-serialized | **leave** — the test-guard mutex is exactly the pattern this audit prescribes for stance-shaped globals. |
| `degenbot-simulation/sim/evm/sim_metrics.rs` counters | metric counters | safe-serialized | **leave** — write-only in the suite; no test re-reads absolute values. |
| `degenbot-pools`/`degenbot-math` `tests/alloc_tracking.rs` `#[global_allocator]` shim + `ACTIVE`/`BYTES`/`ALLOCS` | allocator-wide counters | safe-serialized | **leave** — opt-in `DEGENBOT_ALLOC_TRACK=1`; the measuring harness is a single test binary with a single measuring `#[test]` driving all phases on one thread (the pool file's second test is the gate-off pass-through). |
| `degenbot-abi` `abi_types/cached.rs` `CACHE_TEST_MUTEX` | cache test gate | safe-serialized | **leave** — the canonical test-guard-mutex pattern. |
| `degenbot-uniswap` `deployments.rs` `TABLE`; `degenbot-python` `dex_identity` `PRESETS`, `conversion/rpc_types.rs` field sets, `diagnostics/thread_registry.rs`/`gil_probe.rs` | read-only tables / thread-scoped diagnostics | safe-serialized | **leave** — `LazyLock`-initialized read-only data; diagnostics are thread-scoped or monotone. |
| `degenbot-config` `holder.rs` `CFG`/`DEFAULT` `OnceLock`s | init-once typed config | host-shape-coupled (config source) | **leave** — no test installs a config (verified across `degenbot-config/tests`); config enters tests as values, not via the holder. `cpu_budget` reads it once (cached) — see the core rows above. |

## Systemic guards already in place

- `degenbot-config/tests/no_stray_env_reads.rs` — inventoried env-read sweep.
- `just check-no-pyo3-in-cores` / `just check-no-inner-allow` — contain the
  surface property tests and fixes must respect.
- Property tasks `CVURM7`/`TTANQJ`/`JXCAR4` under this epic pin all
  host-derived sizing formulas to their pure seams.

## Items requiring action (summary)

1. `SOLVE_FLEET_HOSTED` + fleet-boot `OnceLock` TOCTOU — **filed as an ergo
   task under epic `64ZQLA`** (inject: per-construction stance from the
   caller's cfg).
2. Standing rule for the failure-policy override store — recorded in the
   table above; no code change.
