# WFF6MM — Soak + hard cutover: retire the in-cycle fallback arm

## Soak evidence (phase A)

Phase A scope only: flag-ON live soak evidence for `solve.admission_shed` (the
capacity-modulated admission shipped in `eec70d10c` + `7f0efc990`). The CUTOVER
(file deletions, stance retirement) is Phase B and was not performed here. No repo
file other than this evidence file was changed; WFF6MM stays in `doing`.

### Environment and build

- Repo `/workspaces/degenbot`, HEAD `eec70d10c`; `solve.admission_shed` default OFF,
  forced ON for this soak via `DEGENBOT_SOLVE_ADMISSION=1`.
- Rebuild command (unconditional): `uv sync --reinstall-package degenbot`. The build
  receipt does not fingerprint `degenbot-bot` sources, so `just verify-build-fresh`
  cannot gate this change and was treated as informational only.
- Freshness verified independently: the installed `src/degenbot/_ffi.abi3.so` mtime
  (1789173253) is newer than the newest `degenbot-bot` source (1789173093), and the
  binary contains the `[admission] SHED: draw-time zero budget` string and the
  `degenbot.detached.shed` instrument.
- Launch: `/tmp/launch_admission.sh` (a copy of `/tmp/launch_bot.sh` with
  `export DEGENBOT_SOLVE_ADMISSION=1` added), operator socket
  `/tmp/degenbot-operator.sock`, `DEGENBOT_OTEL=1`, metrics at
  `http://127.0.0.1:9464/metrics`.
- Flood: `uv run --no-sync degenbot path discover --socket /tmp/degenbot-operator.sock --bound 200000`.

### Acceptance table

| Check | Expected | Observed | Verdict |
| --- | --- | --- | --- |
| Healthy `in_flight` max | 0 over all samples | 0 over 95/95 samples (95 s @ 1 Hz) | PASS |
| Healthy `shed_total` delta | 0 | 0 (flat at 0) | PASS |
| Healthy `degraded_cycles` | 0 | 0 | PASS |
| Stall `in_flight` climb | >= 8 during the probe | 269 at the shed draw (log), 0 sampleable (metrics dark during freeze, see note) | PASS (with artifact caveat) |
| Stall `shed_total` delta | prompt text says 0; GOAL/step 5 say "INCREASED" | +1 (0 -> 1) | PASS against the GOAL; prompt acceptance line self-contradicts |
| Stall `degraded_cycles` delta | <= 2 (transition edge only) | +1 (0 -> 1) | PASS |
| `leads_expired_total` | bounded or zero | 0 | PASS |
| Recovery `applied_total` | resumed climbing | 1285 -> 2415 (+1130 over 65 s) | PASS |
| Bot alive at end | alive, clean stop | alive; final scrape OK; `./run_bot.sh stop` clean | PASS |

Note on the stall `in_flight` row: the injected SIGSTOP was the documented injection
artifact. When the stop lands while the merge thread holds the engine Mutex, the whole
pipeline blocks: `/metrics` is unresponsive for the freeze window and the pump logs
`HEADER STALL`, so the `in_flight` gauge cannot be scraped while it is climbing. The
climb is instead read directly at the shed decision:
`in_flight=269, target_depth=8` in the `[admission] SHED` line (269 >> 8).

Note on the stall `shed_total` row: the phase prompt's acceptance bullet says
"shed_total delta == 0" under stall, which contradicts the GOAL ("prove shed+carry"),
protocol step 5 ("shed_total INCREASED during the stall window") and step 7 ("expect SHED
lines > 0"). The observed flag-ON behavior is shed delta +1; the == 0 reading is treated
as a copy/paste error in the acceptance list, not as a requirement, and is flagged here
rather than silently resolved.

### Healthy window (95 s @ 1 Hz, before flood)

| Series | first | last | min | max |
| --- | --- | --- | --- | --- |
| `degenbot_detached_in_flight` | 0 | 0 | 0 | 0 |
| `degenbot_detached_shed_total` | 0 | 0 | 0 | 0 |
| `degenbot_detached_degraded_cycles_total` | 0 | 0 | 0 | 0 |
| `degenbot_detached_applied_total` | 23 | 34 | 23 | 34 |
| `degenbot_detached_leads_expired_total` | 0 | 0 | 0 | 0 |
| `degenbot_solves_executed_total` | 4 | 12 | 4 | 12 |
| `degenbot_cgroup_throttled_total` | 15875 | 15876 | 15875 | 15876 |
| `degenbot_pump_seconds_since_header` | 0.0043 | 0.0076 | 0.0023 | 0.0076 |

Expectation (in_flight pinned 0, shed flat 0, degraded 0, applied climbing) held. Arm
histograms in this window were `arm="detached"` only (no `in_cycle`, no shed).

### Flood and stall injections

- Flood started 00:37:30; applied rate `degenbot_detached_applied_total` 388 -> 489 over
  20 s (~5/s), well above the ~1-2/s target; `solves_executed_total` 18 -> 20; `in_flight`
  stayed 0 and `shed_total` stayed 0 in the sustained flood regime before injection.
- Injection #1 (unintended 66 s): the original `/tmp/stall_probe.sh` self-timed its
  SIGCONT on a loop of `curl -m 1`, so when the freeze made metrics unresponsive the loop
  stretched the stop from the intended ~6 s to 66 s (STOP 00:38:55 -> CONT 00:40:01).
  During it: `[admission] SHED` fired at block 25957804 with
  `in_flight=269, target_depth=8, paths.affected=0`; `shed_total` 0 -> 1;
  `degraded_cycles` 0 -> 1; `leads_expired` 0; `stale_dropped` 129;
  `HEADER STALL: headers were silent {silent_secs=73.9}`. Applied 489 -> 718 across it.
- Injection #2 (corrected 7.0 s): a hard wall-clock timer (`sleep 7; kill -CONT`) bounded
  the stop (STOP 00:40:23.727 -> CONT 00:40:30.729 = 7.00 s). `/metrics` was dark for the
  stop window (first post-stall scrape at 00:40:30.697); after resume `in_flight` was 0 on
  every one of 127 samples and the queued results drained faster than the 150 ms scrape
  interval. No new shed and no new degrade (both flat at 1). Applied 804 -> 1140.

### Stall window series (compact, corrected 7.0 s run)

Timestamps are epoch seconds; `/metrics` returned nothing from STOP (…623.7) until
CONT (…630.7), so the first row is the last pre-stop read.

| epoch | in_flight | applied | shed | degraded |
| --- | --- | --- | --- | --- |
| 1789173630 | (dark) | 804 | (dark) | (dark) |
| 1789173631 | 0 | 939 | 1 | 1 |
| 1789173632 | 0 | 956 | 1 | 1 |
| 1789173633 | 0 | 956 | 1 | 1 |
| 1789173634 | 0 | 956 | 1 | 1 |
| 1789173635 | 0 | 956 | 1 | 1 |
| 1789173636 | 0 | 956 | 1 | 1 |
| 1789173637 | 0 | 1005 | 1 | 1 |
| 1789173638 | 0 | 1140 | 1 | 1 |

Recovery window (65 s @ 1 Hz, immediately after the corrected stall):

| Series | first | last | min | max |
| --- | --- | --- | --- | --- |
| `degenbot_detached_in_flight` | 0 | 0 | 0 | 0 |
| `degenbot_detached_applied_total` | 1285 | 2415 | 1285 | 2415 |
| `degenbot_detached_shed_total` | 1 | 1 | 1 | 1 |
| `degenbot_detached_degraded_cycles_total` | 1 | 1 | 1 | 1 |
| `degenbot_detached_leads_expired_total` | 0 | 0 | 0 | 0 |
| `degenbot_solves_executed_total` | 30 | 35 | 30 | 35 |
| `degenbot_cgroup_throttled_total` | 15879 | 15883 | 15879 | 15883 |

`in_flight` returned to 0 and applied resumed at ~17/s with no further shed or degrade.

### Log evidence

- `[admission] SHED` lines: **1**.
  `[admission] SHED: draw-time zero budget — nothing submitted; keys retained for carry {block_number=25957804, paths.affected=0, in_flight=269, target_depth=8}`
- `HEADER STALL` DIAG lines: **1** (`silent_secs=73.9`, the injection artifact).
- `[fleet-posture]` cordon events: 2 ENTER / 2 EXIT (clean-window hysteresis), reasons
  `EventBurst { events: 15875 }` and `EventBurst { events: 2 }`.
- Solve-cycle arm distribution: final histograms are `degenbot_solve_duration_seconds_count{arm="detached"}=34`
  and `{arm="in_cycle"}=1` (34 + 1 = 35 = `solves_executed_total`). The single `in_cycle`
  cycle is the transition-edge degrade during recovery from the 66 s freeze:
  `[solve-phase] cycle complete (clamp done) {..., total_us=534768}`; the other 34 cycles
  log as `[solve-phase] … (merge runs on the sidecar)`. No `arm="shed"` series exists on the
  duration/mutex histograms because a shed cycle short-circuits before a solve is timed; the
  shed evidence is the counter plus the log line above.

### Jaeger

One query was run:
`curl -s 'http://host.docker.internal:16686/api/traces?service=degenbot-bot&operation=degenbot.arb.solve&limit=80&lookback=1h'`.
It returned HTTP 200 with 4 traces / 2928 spans; the only `cycle.arm` values present are
`detached` (4 spans). **No span carried `cycle.arm="shed"`** and no `"shed"` string appears
anywhere in the response. The trace export was sparse (4 traces for a multi-minute run), so
this is a telemetry-coverage observation, not proof the shed path emits no span; the
authoritative shed signal in this soak is the `[admission] SHED` log line and the
`degenbot_detached_shed_total` counter.

### A/B versus the 2026-09-11 flag-OFF baseline

The 2026-09-11 flag-OFF baseline pinned `in_flight` at 0 across a 600/600 @ 10 Hz healthy
window and, under a 6 s SIGSTOP, produced exactly 2 degraded in-cycle cycles (126 ms + 62 ms)
via the cap-8 fallback. With the flag ON, the sustained regime is identical where it must be —
95/95 healthy samples and 65/65 recovery samples hold `in_flight` at 0, `shed_total` flat
until the injected stall, and `degraded_cycles` at 0 — but the failure mode changes: a drain
stall now sheds the zero-budget draw (0 -> 1, drawn at `in_flight=269`) and carries the keys
rather than re-topologizing the cycle, and the only degrade observed for a stall an order of
magnitude longer than the baseline's was a single 534 ms `in_cycle` transition-edge cycle
(delta +1, within the <= 2 allowance). In other words the flag-ON contract held: shed under
stall, zero shed and zero degrade in the sustained healthy/flood regime.

### Artifacts

All under `/tmp/wff6mm-artifacts/`:

- `healthy_samples.txt` — 95 x 1 Hz healthy window (all series).
- `recovery_samples.txt` — 65 x 1 Hz recovery window.
- `stall2_samples.txt`, `stall2_timeline.txt`, `stall2_pre.txt`, `stall2_post.txt` — corrected 7.0 s injection.
- `stall_samples_full.txt`, `stall_samples_inflight.txt` (empty), `stall_probe_out.txt` — 66 s injection attempt.
- `metrics_boot.txt`, `metrics_prestall.txt`, `metrics_poststall.txt`, `metrics_final.txt` — metric snapshots.
- `bot_run_clean.log` — ANSI-stripped `logs/bot_run.log` (13 209 lines).
- `total_us.txt`, `jaeger_traces.json`, `flood.pids`, `sample.sh`, `sample10.sh`, `stall_probe2.sh`, `parse_jaeger.py`.
- `/tmp/launch_admission.sh` — flag-ON launch script; `/tmp/flood-wff6mm.log` — flood log.


## Cutover (phase B)

Hard cutover executed against HEAD `eec70d10c`; scope: delete the in-cycle
fallback arm and every construct that existed only to switch to it. No
back-compat shim, no feature flag (AGENTS.md no-back-compat rule).

### What was deleted

- **Dispatch arm** (`arb_engine/solver_dispatch.rs`): the whole in-cycle
  dispatch body after the detached block (`Arm::InCycle`, the `tokio_solve`
  measure block, the in-cycle drain + clamp/twin/solve-complete telemetry).
  The detached enqueue is now unconditional: `begin_cycle()` takes no stance
  argument and the driver always returns at enqueue end.
- **Machine** (`arb_engine/detached_cycle.rs`): `Saturated` state and its
  transition rows (the machine is now `Unopened → Open`), `tick_in_cycle`
  (the in-cycle ledger-seq issuance), `FanInTally` + its tripwire, and the
  `Arm::InCycle` verdict. `begin_cycle` takes no argument; the ledger seq is
  machine-issued for the one arm.
- **Policy/contract surface** (`solver_dispatch.rs`): `LaneArmPolicy::lock`
  + `claim_all_lanes` and the `LaneDrainLockContext` enum (contract 3's name
  plate and the in-cycle Suppressed/Failed claim divergence). Suppressed/
  Failed now NEVER claim (the pid-only keyless witness rule, contract 4).
- **Stance**: `solve.detached_solves` retired from `degenbot-config`
  `SCHEMA`; `DEGENBOT_DETACHED_SOLVES` is an unknown key (env ignored,
  CLI/TOML fail closed as unknown). The engine's `detached_solving` field,
  config read, and test setter are gone.
- **Dead tests** for the retired behavior:
  `detached_off_merges_synchronously_inside_the_call` (flag-OFF sync merge),
  `solve_dirty_hold_does_not_starve_runtime_tasks` (the in-cycle multi-second
  Mutex hold — the hazard it guarded is retired; its T2 premise was a 600 ms
  hold that no longer exists). Tests that pinned retired counters/arms were
  reworked to the one-arm reality (cap no longer degrades, `cycle.arm` spans
  stamp only `detached`, replay tests use the machine-issued seq).
- **Docs**: `docs/detached-solve-cycle-decision.md` (theses 2/3/5, policy
  matrix, new cutover section), `CONTEXT.md` (machine vocabulary:
  `Unopened → Open`, no stance argument), `docs/architecture/
  block-epoch-pipeline.md` + `stateview-feasibility.md` (stance references),
  `docs/rust-config-keys.md` (regenerated).

### What was kept

- The ONE drain (`drain_lane_outcomes`); its only caller is now the detached
  sidecar. The variant-gated gauge pairing (Solved bumps at send, decrements at
  merge) survives untouched — it is what the admission draw reads.
- `DETACHED_INFLIGHT_CAP = 8` as the clamp bound for the admission target
  depth (no longer a dispatch gate).
- `degenbot.detached.degraded_cycles` still exported at 0 (no producer left;
  the series stays for dashboard continuity).
- The admission draw (shed+carry) and its counters are unchanged.

### Test-harness accommodation (test-only)

The production path returns at enqueue end, which would make every direct
`solve_dirty`/`rebuild_and_solve_affected` unit test read an empty result
map. A `#[cfg(test)]` `test_sync_merge` engine field (default true) makes
such direct calls drain their just-enqueued pipe INLINE through the sidecar's
own `merge_detached_item` (`drain_merge_inline`). `EngineStages::solve_dirty`
turns it OFF before driving the engine (the sidecar owns the pipe there — the
spawn happens after the engine call returns, and the machine's merge Receiver
is take-once). Production compiles none of this.

### Validation

- `just fmt-check` — clean.
- `just lint-rust-check` (clippy, `-D warnings`, all targets/features) — clean.
- `cargo test -p degenbot-bot --lib` — 728 passed, 0 failed.
- `cargo test -p degenbot-bot --lib --features otel` — 763 passed, 0 failed
  (3 ignored, env-dependent).
- `cargo test -p degenbot-config` — precedence + schema + lib green;
  `schema_completeness` green after regenerating
  `degenbot_env_inventory.txt` (it was stale from eec70d10c: missing the three
  `DEGENBOT_SOLVE_ADMISSION*` keys).
  Pre-existing environmental failure, unrelated to this change (reproduced on
  stashed HEAD): `no_stray_env_reads_outside_the_config_loader` fails on the
  untracked `autoresearch/` scratch tree at the repo root.
- `uv sync --reinstall-package degenbot` then `just verify-build-fresh` —
  installed `degenbot._ffi` build 390 == repo receipt (fingerprint
  `859bc1dc6b025083`); `DEGENBOT_DETACHED_SOLVES` is absent from the rebuilt
  `.so` while `DEGENBOT_SOLVE_ADMISSION` is present.
- `uv run --no-sync pytest tests/arbitrage tests/registry tests/test_config_modern_layout.py`
  — 1206 passed, 1 skipped. `tests/rust tests/rust-seam` — 528 passed, 1 skipped.

### Note on the retired T2 starvation test

While the suite was green on HEAD, keeping a rewritten T2 starvation test
(EngineStages + a one-worker tokio runtime + the detached sidecar) made
`bot_core::block_pump::tests::fsm_lifecycle_recovers_and_does_not_reassert_stale`
fail reproducibly under `--features otel`: a stale-parent span clone in the
suite's single global-subscriber test. The interaction is a pre-existing
parallel-test tracing hazard (the repo already documents the thread-local
`set_default` unsafety), triggered by running detached sidecar work inside a
tokio test concurrently with the global-subscriber test. Since the hazard the
T2 test guards — a multi-second in-cycle engine-Mutex hold pinning a runtime
worker — was deleted by this cutover, the test was deleted rather than
reshaped around the flaky interaction. The enqueue-end return it also pinned is
covered by `detached_cycle_returns_at_enqueue_end_and_sidecar_merges` and
`detached_solve_returns_at_enqueue_end_when_sync_drain_is_off`.

## Review remediation

Supervisor review caught a Prometheus series-name bug in the QTZGFL
instruments: the OTel exporter appends `_total` to monotonic counters, so a
declared name already ending in `_total` rendered doubled. The declarations
were renamed to drop the suffix; the rendered series names now match the house
sibling style (`degenbot.detached.applied` → `degenbot_detached_applied_total`).

| Declared (before) | Rendered (before) | Declared (after) | Rendered (after) |
| --- | --- | --- | --- |
| `degenbot.detached.shed_total` | `degenbot_detached_shed_total_total` | `degenbot.detached.shed` | `degenbot_detached_shed_total` |
| `degenbot.detached.leads_expired_total` | `degenbot_detached_leads_expired_total_total` | `degenbot.detached.leads_expired` | `degenbot_detached_leads_expired_total` |

Rust method names (`count_detached_shed`, `count_detached_leads_expired`) are
unchanged, as is the declared-name-independent zero-init contract. The
`instruments.rs` render test was tightened from `starts_with(name)` to an
exact sample-name match — the looser predicate had masked the doubled suffix —
so the corrected `degenbot_detached_shed_total` /
`degenbot_detached_leads_expired_total` series are now asserted exactly.
References to the declared names in `detached_cycle.rs`,
`degenbot-config/src/schema.rs`, `docs/rust-config-keys.md`, and
`docs/tasks/QTZGFL-admission-draw.md` were updated to match.

`degenbot.detached.degraded_cycles` was reviewed and deliberately kept: WFF6MM
retired its producer (the in-cycle arm), and the supervisor accepted the
retained `0`-pinned series for dashboard continuity. It is unchanged by this
remediation.
