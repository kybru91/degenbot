# QTZGFL — Draw site: capacity-modulated admission behind a feature flag

Implemented the admission-draw experiment as a parallel, flag-gated path: the
construction-stamped stance `DEGENBOT_SOLVE_ADMISSION` (default OFF) replaces
the in-flight cap degrade with a capacity-modulated draw
(`budget = max(0, target_depth − in_flight)`) and responds to a zero budget
with a SHED — no submission, counted, span-stamped, cursor advanced, keys
retained for carry. With the flag OFF every path is byte-identical to the
pre-change behavior, including the degrade.

## Flag parse matrix

One install site, parsed once at construction (the KAHU5W construction-stance
pattern), never re-read from env per cycle:

| `DEGENBOT_SOLVE_ADMISSION` | effect |
| --- | --- |
| unset | OFF (current degrade, byte-identical) |
| `0` / `false` | OFF |
| `1` / `true` / `on` | ON (modulated draw + shed) |
| any other word | config load fails loudly (the loader owns the words) |

The conservative default is deliberate: unlike the sibling detached stance
(default ON), the experiment must be opt-in so the shipped posture keeps the
existing safety valve untouched.

## Typed config keys

- `solve.admission_target_depth` (`[usize]`, default `8`) —
  `DEGENBOT_SOLVE_ADMISSION_TARGET_DEPTH`. The un-merged-result pipe depth
  target in KEYS. **Clamped at construction to `1..=DETACHED_INFLIGHT_CAP`**
  (a target above the design-locked safety valve is meaningless; a target of 0
  would never submit). The clamp is documented on the field and carries a
  judgment call: it makes a zero-budget verdict arise only from gauge
  saturation, never from config.
- `solve.admission_retention_blocks` (`[u64]`, default `50`) —
  `DEGENBOT_SOLVE_ADMISSION_RETENTION`. The carried-key retention window W.

Both are typed schema keys (not ad-hoc env reads); the generated
`docs/rust-config-keys.md` was regenerated (`REGEN_CONFIG_DOCS=1`).

## Where it lives

- Draw + retention site: `EngineStages::on_resolve`
  (`rust/crates/degenbot-bot/src/arb_engine/engine_stages.rs`). Flag OFF →
  `take_keys()` exactly as before; flag ON → `expire_older_than(head − W)`
  then `draw_freshest(budget)`, overflow retained. Lock order: engine mutex
  outer, ledger mutex inner — no new order was introduced.
- Shed decision: `rebuild_and_solve_affected`
  (`rust/crates/degenbot-bot/src/arb_engine/solver_dispatch.rs`), at the top
  of the cycle (after the solve-anchor resolution, before the dirty fan-out's
  pending-new-path merge, so a shed never clears work it did not do). It
  consumes + clears the draw-time verdict stashed by `on_resolve` — it does
  **not** re-read the gauge — latches `cycle_arm = "shed"` through the one
  `record_cycle_arm_telemetry` wiring site, and advances the solved-block
  cursor exactly like the `skipped_empty` branch.
- Budget helper: `ArbitrageEngine::admission_budget_keys() -> Option<usize>`
  (`None` = stance OFF; `Some(0)` = shed). **DRAW-SITE ONLY** (F3): it reads
  the same `detached_cycle.outstanding` atomic `begin_cycle` uses, and its
  output is consumed at the draw. The dispatch never re-reads the gauge; it
  consumes the verdict `on_resolve` stashes.

## Machine-safety analysis (shed cycles)

The DRAW is the single consumption decision. `on_resolve` computes the
budget, stashes the zero verdict on the engine (`admission_draw_zero`, set
under the engine mutex), and draws exactly that many keys. The dispatch
consumes and clears the verdict in the same engine-lock scope (`solve_dirty`)
and **never re-reads the live gauge**. The stage machine drives Resolved →
Solved sequentially on the driver thread, so the stash cannot interleave with
another cycle's draw.

A shed cycle **does not consult the solve-arm machine at all**: the
draw-zero check sits before `begin_cycle`, so no B-row fires, no seq tick is
drawn, no merge pipe is opened, and no submission happens (a shed cycle
SUBMITS NOTHING and CLAIMS NOTHING). Per the transition table in
`detached_cycle.rs`, that is trivially legal because no transition verb is
invoked; no typed `RejectionReason` can possibly trip. The cursor advance and
the arm latch are cycle-level bookkeeping identical to `skipped_empty`, which
the table does not model.

A cycle that drew a **positive** budget ALWAYS submits its drawn keys. If
`begin_cycle` then returns the in-cycle (cap-saturated) verdict at the
transition edge — a bin thread bumped in-flight after the draw — the EXISTING
in-cycle response serves it: one legal machine row (the `InCycle` begin is a
B-row), no claim invented, and no drawn key discarded. The former post-begin
race-shed was deleted precisely because it raced the merge pipe and discarded
already-drawn keys. Flag-ON contract: shed at a draw-time zero; otherwise the
arms flow unchanged. Flag OFF is byte-identical. The in-cycle arm, the cap
constant, and the detached stance parsing are untouched (WFF6MM owns the
cutover).

## Telemetry

- `degenbot.detached.shed_total` and `degenbot.detached.leads_expired_total`,
  both zero-initialized like `detached_send_failed` (the 9395c481b lesson).
- Arm vocabulary grew one closed-set value: `cycle.arm="shed"`, riding the same
  `record_cycle_arm_telemetry` helper and the engine's `cycle_arm` latch; the
  closed-set comments on `ArbitrageEngine::cycle_arm`, the helper, and the
  histogram docs were updated to `detached | in_cycle | skipped_empty | shed`
  (`unset` sentinel retained).
- The machine owns testable disposition counters
  (`DetachedCycle::shed_cycles`, `DetachedCycle::leads_expired`) plus
  `DetachedCycle::shed()` / `note_leads_expired(n)`, which also feed the
  pipeline meters — mirroring the existing `applied` / `dropped_stale`
  split. The counters are the end-to-end test witness because the global OTel
  pipeline is not installable in the unit-test process; the pipeline meters
  themselves are covered by the `instruments.rs` render tests.

## Red → green evidence

Red was obtained by a temporary one-line probe forcing
`admission_budget_keys()` to `None` (the draw absent), producing genuine
behavioral failures for the right reason rather than a missing-API compile
error (the surface already compiled):

- `admission_budget_arithmetic_and_target_clamp`: left `None`, right `Some(3)`.
- `admission_zero_budget_sheds_the_whole_cycle`: left `"in_cycle"`, right `"shed"`.
- `admission_carries_retained_keys_to_a_later_cycle`: zero-budget draw unexpectedly drew keys (retention absent).
- `admission_retention_window_expires_carried_leads`: the stale lead was drawn, not expired.

The probe was removed and the suite went green (7 filtered tests passed,
including the two instrument render tests). The five admission tests:

- `admission_budget_arithmetic_and_target_clamp` — budget arithmetic + saturating
  overshoot + `None` when OFF + target clamp at both ends.
- `admission_zero_budget_sheds_the_whole_cycle` — the full path through
  `solve_dirty` with the gauge preloaded at target: no path submitted, cursor
  advanced to the anchor, `cycle_arm() == "shed"`, shed counter fired once.
- `admission_carries_retained_keys_to_a_later_cycle` — a zero-budget
  `on_resolve` retains all keys; a later cycle with headroom draws them
  freshest-first, then the final carry drains. This is the acceptance the
  design turns on.
- `admission_retention_window_expires_carried_leads` — `head − W` prunes the
  stale lead, counts it once, and draws only the in-window lead.
- `admission_off_keeps_take_all_and_the_cap_degrade` — flag OFF keeps the
  in-cycle degrade and the full `take_keys` draw regardless of the gauge.

## Acceptance notes and judgment calls

- `target_depth` is clamped to the cap, so under the flag ON a positive budget
  implies `in_flight < target <= cap`: `begin_cycle` then always returns the
  detached arm, and the only way a saturated cycle reaches the arm site is the
  defensive race branch.
- The draw/shed gate keys on the admission stance alone (not on
  `detached_solving`). With detached OFF the gauge never moves and the stance
  ON would limit in-cycle draws to `target` keys; that is flag-ON-only behavior
  and the default posture is untouched.
- "Healthy drain draws behave as take-all" is read as: when the pending key
  count fits the budget the draw equals a full `take_keys` (parity is already
  pinned in `epoch_delta.rs`); with more pending keys than the budget the
  overflow is carried, which is the point of the experiment.
- The retention prune runs at `on_resolve`, which is the per-settled-block
  drain beginning — the effective block-advance site (production never calls
  `EpochDelta::set_epoch`). The ledger mutex is taken inside that call, under
  the engine mutex, and no reverse nesting exists.

## Deviations

- **Checkpoint transfer (per the supervisor note).** No live bot session was
  run and no approval question was asked; the checkpoint decision transfers to
  the supervisor and the LIVE soak capture belongs to WFF6MM. This document is
  the evidence record.
- No `.so` rebuild was performed: the gates are cargo-only, no Python-facing
  behavior was exercised, and the build-receipt fingerprint covers only the
  `degenbot-python` crate's own sources, which were not edited.
- The pipeline counters are asserted end-to-end through the machine-owned
  atomic witnesses (the unit-test process cannot install the global OTel
  pipeline); the Prometheus zero-init/fire/render contract is covered by
  `detached_admission_counters_render_before_any_event`.

## Gates

- `cargo fmt -p degenbot-bot -p degenbot-config -- --check` — clean.
- `cargo clippy -p degenbot-bot --all-targets --features otel` — clean.
- `cargo test -p degenbot-bot --features otel --lib` — 767 passed, 0 failed,
  3 ignored.
- `cargo check --workspace` — clean.
- `cargo test -p degenbot-config` — schema/doc-generation suites green; the
  `no_stray_env_reads_outside_the_config_loader` integration test fails on a
  **pre-existing, gitignored `autoresearch/` repo snapshot** (every reported
  violation lives under `autoresearch/01a09292-.../rust/...`, none in this
  change), not on this work.

## Review remediation

The supervisor review of the implementation diff found three coupled defects;
all three are fixed here with a red-first regression test.

### Findings

- **F1 — race carry-loss.** `on_resolve` drew `k > 0` keys (REMOVING them
  from the ledger), then an earlier cycle's bin thread bumped in-flight to/over
  the target before the dispatch. The dispatch's fresh `admission_budget_keys()`
  re-read returned `Some(0)`, the early shed branch fired, and the drawn keys
  were discarded — never submitted, never re-recorded. The log line and comments
  claimed "keys retained for carry", which was false in that window.
- **F2 — pending-path loss.** The post-begin race-shed (inside the
  `match &arm` block) fired AFTER `affected_path_ids.extend(&self.pending_new_paths);
  self.pending_new_paths.clear();`. A race shed therefore discarded the
  eager-registration merge protection, so the next normal cycle's results
  replacement could drop the eagerly-solved results — the exact bug class that
  merge exists to prevent.
- **F3 — root cause.** The budget was decided TWICE (once at the draw in
  `on_resolve`, once fresh at the dispatch). The two reads can disagree, and
  every disagreement path loses work.

### Design (as implemented)

1. **The DRAW is the single consumption decision.** `on_resolve` (under the
   engine mutex) stashes the zero verdict on the new engine field
   `admission_draw_zero` and draws exactly the budget it computed. The
   dispatch consumes AND clears the verdict in the same engine-lock scope of
   `solve_dirty` and never re-reads the gauge. `admission_budget_keys` stays
   as the draw-site helper; its doc now states its `Some(0)` output is
   consumed only at the draw.
2. **The early shed branch condition is now** `self.solve_admission && draw_zero`
   (the draw-time verdict), not a fresh gauge read. Its position is unchanged —
   before the `pending_new_paths` merge — so a shed consumes no pending work
   (F2) and no drawn keys (F1). The shed log now reads
   "draw-time zero budget — nothing submitted; keys retained for carry", which
   is true by construction.
3. **The post-begin race-shed block was DELETED.** With the draw-time decision,
   a cycle that drew a positive budget ALWAYS submits its drawn keys; if
   `begin_cycle` returns a saturated verdict at the transition edge, the
   EXISTING in-cycle response serves it — one legal machine row, no claim
   invented, no new transition, no `RejectionReason` reachable. Flag-ON
   contract: **"shed at a draw-time zero; otherwise the arms flow unchanged"**.
   Flag OFF remains byte-identical.

### Red evidence (red-first where behavior changed)

The race regression test `admission_race_positive_draw_never_sheds` drives both
stages explicitly so it can interleave the in-flight bump exactly in the race
window (between `on_resolve` and `on_solve`) — the staged-path shape was
chosen because the test drives both stages, making the interleave deterministic;
the direct-`solve_dirty` shape was not needed.

Before the fix (current pre-remediation code):

```text
running 1 test
thread 'arb_engine::tests::tests::admission_race_positive_draw_never_sheds'
panicked at .../tests.rs:7852:9:
assertion `left == right` failed: the transition-edge cap verdict takes the
EXISTING in-cycle response, not a shed
  left: "shed"
 right: "in_cycle"
test arb_engine::tests::tests::admission_race_positive_draw_never_sheds ... FAILED
```

After the fix: `admission_race_positive_draw_never_sheds ... ok` (part of the
9-test `admission` filter run, 9 passed / 0 failed).

### Test rework

- `admission_zero_budget_sheds_the_whole_cycle` now drives the staged path
  (`EngineStages::on_resolve` + `on_solve`) instead of `solve_dirty` with a
  preloaded gauge — the dispatch no longer re-reads the gauge, so the old seam
  tested nothing.
- `admission_draw_zero_shed_preserves_pending_new_paths` (new): a draw-zero
  staged shed leaves the eager-registration pipe intact, and the NEXT normal
  cycle merges + clears it with the eager result surviving (F2).
- `admission_race_positive_draw_never_sheds` (new, red-first): a positive draw
  followed by a gauge bump to the cap must NOT shed; the drawn keys are
  submitted, the arm is `in_cycle`, the shed counter is unchanged, and the
  pending pipe still merges (F1/F3 + F2).
- Budget arithmetic, carry, retention, and flag-OFF tests are unchanged and
  green.

### Note for WFF6MM

The transition-edge in-cycle case survives under flag ON by design (no shed, no
tick, no claim). The WFF6MM soak AC should expect `degenbot.detached.degraded_cycles`
= 0 in the sustained shed regime (every draw is zero-budget, so every cycle
sheds before `begin_cycle`), with at most a couple of transition-edge cycles
under an injected stall.
