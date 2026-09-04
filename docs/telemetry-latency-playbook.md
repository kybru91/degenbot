# Latency telemetry playbook

Operational reference for the tracing/metrics the bot emits: for each latency
critical phase — what it measures, what "bad" looks like, the hypotheses ranked
by prior evidence, and where in the code to look next. Companion to
[`docs/grafana/ALERTS.md`](grafana/ALERTS.md) (Prometheus rules) — this doc is
the Jaeger-side investigation guide.

Sources of truth:

- Spans/events: `rust/crates/degenbot-bot/src/solvers/arb_engine/solver_dispatch.rs`
  (the `[solve-phase]` family), `bot_core/block_pump.rs` (`degenbot.pump.block`),
  `solvers/arb_engine/engine_handle.rs` (`degenbot.arb.solve`).
- Metrics: `rust/crates/degenbot-bot/src/instruments.rs` (`degenbot_*` families).
- OTel setup (event cap, exporter): `rust/crates/degenbot-bot/src/otel.rs`.

Incident shorthand used below: `745f21` = trace `745f2122...` (4.6s solve,
pre-viability-gate), `d3d15a` = `d3d15a5b` (2.2s, first instrumented),
`fb7f7a` = `fb7f7ab1` (441ms, post-gate). All 2026-08-22 session.

---

## Solve cycle anatomy

One `degenbot.arb.solve` span per dirty-block cycle; four `[solve-phase]`
events partition it:

```
span start
  │  (activation scan — no event until fan-out; its cost = fanout.phase_us)
  ├─[solve-phase] fanned out to affected paths   {paths.affected, dirty.v2/v3/v4}
  ├─[solve-phase] resolved hop snapshots         {paths.resolved, invalid.reasons}
  │  (resolve→LPT staging sits in its own child span — MQUKB6-T2 follow)
  ├─ degenbot.arb.stage                          {paths.staged}
  ├─[solve-phase] rayon solve complete           {paths.solved, paths.invalid,
  │                                               solve.cpu_us, profitable,
  │                                               slowest.paths}
  ├─[solve-phase] cycle complete (clamp done)    {clamp.twins, clamp.phase_us,
  └─ span end                                     total_us}
```

Span children of `degenbot.arb.solve` (phase spans, MQUKB6-T2 pattern):
`degenbot.arb.fanout`, `degenbot.arb.resolve`, `degenbot.arb.stage`,
`degenbot.arb.lpt`, `degenbot.arb.merge`. Time between two adjacent child
spans is a regression signal — on trace `f701ccd3` (block 25906841,
2026-09-04) 647 ms sat unattributed between resolve and lpt (per-path deep
clones of the resolved CL tick-range sequences during staging). Fixed by
Arc-sharing the resolved snapshots into the dispatch (staging is refcount
bumps; measured 0.3–0.5 µs/path vs 25-30 µs/path steady-state before, and the
cold-ramp 150-420 µs/path pathology eliminated). If a resolve→lpt gap reappears
larger than `degenbot.arb.stage`, suspect new per-path work added between the
stage span close and `compute_bins`.

Incident shorthand: `f701ccd3` = trace `f701ccd36f4ecf80d671e798df218fa4`
(block 25906841, 2026-09-04 session).

Derived quantities (compute these on every investigation):

| Quantity | Formula | Healthy |
|---|---|---|
| Activation-scan wall | `fanout.phase_us` | < 100ms; scales with affected set |
| Resolve wall | next-event − fanout timestamp | ~10µs/path |
| Rayon wall | rayon-event − resolve-event | ≈ `solve.cpu_us / 7.8` (8 cores) |
| **Achieved parallelism** | **`solve.cpu_us / rayon_wall`** | **≈ 7.8 (8 cores)** |
| Per-path solve CPU | `solve.cpu_us / paths.solved` | ~1.4–2.6ms |
| Invalidity rate | `paths.invalid / paths.resolved` | trending down |

---

## Symptom → hypothesis → action

### S1. Solve cycle > 1s (p95 regression)

Evidence so far: pre-gate traces hit 4.2–4.6s; post-gate worst seen 441ms.

Diagnose by reading the phase split before touching code:

1. **Rayon wall dominates + parallelism ≈ 7.8** → work-bound; the fix is fewer
   paths or cheaper paths. Check `slowest.paths` for outliers (>20ms) vs a flat
   profile (everything ~2ms). Outliers ⇒ investigate those path_ids' pool
   depth/tick counts. Flat ⇒ volume problem; look at activation fan-out width.
2. **Rayon wall dominates + parallelism well under 7** → contention or pool
   starvation. Hypotheses: (a) worker threads blocked on a mutex inside
   `solve_path` (none known today — workers touch no engine state), (b) rayon
   pool shared with another subsystem, (c) CPU steal from co-located processes.
3. **Activation scan dominates** (fanout.phase_us > several hundred ms at
   modest affected counts) → registry walk regressed; suspect `pool_to_paths`
   reverse-index growth or lock contention with registration.
4. **Resolve wall grows superlinearly** with resolved count → re-projection of
   structurally-invalid paths (see S3).

Tracing locations: `solver_dispatch.rs::rebuild_and_solve_affected` (all four
phase events); `engine_handle.rs:129` (span open); per-path timing already in
the closure (`t0`/`micros`, feeds `slowest.paths`).

### S2. Achieved parallelism low (< 6×)

`solve.cpu_us` high relative to rayon wall means workers idle or blocked.
Hypotheses ranked:

1. Affected set too small to saturate 8 workers — benign; ignore unless cycles
   are also slow.
2. False sharing / allocation pressure in the par_iter closure (clones of
   `ResolvedMixedPath` per work item). Profile allocations if cpu_us itself is
   fine but wall balloons only at large sets.
3. Something inside `solve_path` taking a lock — audit any new code for
   `Mutex`/`RwLock` under the par_iter in `solver_dispatch.rs`.

### S3. Invalidity rate > ~50% of resolved

The invalid population is rejected during resolve (under the core read lock).
Current reason histogram (from `invalid.reasons`):

- `pool not viable in the swap direction` — O(1) gate (commit 0194af47d);
  cheap, acceptable at any rate.
- `integer tick-range sequence unavailable` — **the remaining cliff**: pools
  that pass directional viability but fail range building (sparse ticks).
  Each rejection pays an O(tick_data) bitmap walk, and failures are NOT cached
  (`get_cached_tick_ranges` stores successes only) — every cycle re-walks.

Next fix if this dominates again: memoize the failure per pool keyed on
`state_nonce` (+ current tick), or hoist a per-pool "range-buildable" flag
maintained at state-update time (same pattern as the viability gate).

Also consider: skip non-viable paths at ACTIVATION time (dirty-mark walk) via
per-pool viability flags maintained in BotState — removes them from
`paths.affected` entirely rather than rejecting at resolve.

### S4. Clamp phase growing (clamp.phase_us)

Cost model: one twin simulation per CL hop across clamped paths
(`clamp.twins`). Observed 10–69ms for 700–2900 twins — linear, cheap.
Investigate only if twins-per-path jumps (path shape shift toward CL-heavy) or
phase exceeds ~100ms. Location: `clamp_cl_hop_capacity` loop after the merge;
twin simulators are `v3_simulate_swap`/`v4_simulate_swap` in degenbot-pools.

### S5. Pump block span ≫ solve span total

`degenbot.pump.block` should wrap the drain tightly. If block duration exceeds
the sum of child solves by a lot, time is going to: header decode, log decode,
state apply (each has a Prometheus histogram: `degenbot_log_decode`,
`degenbot_state_apply`), or queue wait (`degenbot_drain_queue_wait`). Cross-
check the matching metric histogram percentiles in Prometheus before adding
new spans.

### S7. header_to_solved dominated by a flat settle wait

Decompose the pump-side gap first — `degenbot.pump.block` fields:
`header_to_first_log_us` (header → first relevant log), `log_burst_us`
(first → last relevant log, i.e. the apply work), `settle_wait_us` (last log →
settle decision). The settle wait is the publish debounce — it costs roughly
`DEBOUNCE_MS` (the full window) on every block; it buys coalescing of
same-block log bursts at the price of that fixed tail.

2026-09-04 baseline (block-25906841 session): `settle_wait_us` pinned at
~51 ms on 24 of 28 blocks while `log_burst_us` spanned 1.3–27.5 ms — the 50 ms
debounce fired long after each burst was fully applied, making it a fixed
~50 ms per-block tax on header_to_solved. The window is operator-tunable via
`DEGENBOT_PUMP_DEBOUNCE_MS` (parse contract `BlockPump::debounce_ms_cfg`:
unset/zero/invalid falls back to 50 ms, so a bad value can never collapse the
window to zero). Live A/B at 15 ms settled the same bursts at ~16 ms with no
change in bursts and still exactly one solve cycle per block.

Trade-off when lowering: a same-block log straggling past the window starts a
second solve cycle for that block (extra solve cost + an extra publish). The
truth condition for publication (ADR-008 D2: all dispatched logs applied) is
unchanged — check solve-cycles-per-block in Jaeger if raising throughput
suspiciously.

### S6. Missing events / silent spans (exporter-side data loss)

Known incident class, not a bot bug:

1. **Span-event cap**: opentelemetry_sdk defaults `max_events_per_span = 128`;
   busy solves emit thousands. Fixed at u32::MAX in otel.rs (04637eacf,
   c9dfeb4f1). Symptom if regressed: exactly ≤128 logs per solve span and no
   phase tail.
2. **Stale binary**: `uv sync` does NOT reinstall an editable package whose
   version metadata is unchanged — use
   `uv sync --reinstall-package degenbot` after Rust edits, then restart.
   Verify which build a trace came from via `code.line.number` tags vs current
   source lines.
3. Batch-exporter lag: last spans of a killed process are lost. Don't trust a
   trace count drop right after a restart as a regression.

---

## Telemetry inventory (quick reference)

Jaeger spans: `degenbot.pump.block` (root, per drained block),
`degenbot.arb.solve` (child, per solve cycle), `degenbot.path.register`
(registration worker). Span events carry `code.line.number` — use it to
confirm binary freshness against current source.

Key Prometheus families (`instruments.rs`): `degenbot_solve_duration_seconds`,
`degenbot_header_to_solved_seconds`, `degenbot_drain_queue_wait_seconds` /
`_depth`, `degenbot_log_decode_seconds`, `degenbot_state_apply_seconds`,
`degenbot_state_head_lag_blocks`, `degenbot_candidates_found_total`,
`degenbot_submit_latency_seconds`. Alert rules + thresholds:
[`ALERTS.md`](grafana/ALERTS.md).

## Open follow-ups (priority order)

1. Cache SequenceUnavailable per pool+nonce (S3) — biggest remaining
   structural waste when invalidity spikes recur.
2. Per-pool viability flags at activation time (S3 alt) — shrinks the
   affected set itself.
3. Watch clamp growth if path mix shifts CL-heavy (S4).
4. If parallelism ever drops below ~7× on quiet boxes, check CPU quota in the
   devcontainer before blaming the solver (S2).
