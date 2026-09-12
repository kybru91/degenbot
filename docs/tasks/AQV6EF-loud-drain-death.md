# AQV6EF — Loud drain death: send-failure signal + sidecar panic guard

## What shipped

Closed the only genuinely silent loss in the shipped posture: a dead merge
seat that drops every later detached result with no trace and (because a
failed send never bumps the in-flight gauge) cannot even fire the Saturated
fallback.

- **Send-failure signal (hole 1).** `SolveLane` in
  `rust/crates/degenbot-bot/src/arb_engine/executor.rs` gained an optional
  `on_send_failed` hook. `solved()`, `suppressed()` and `failed()` now
  route a **failed** `mpsc` send through `note_send_failed` instead of
  swallowing it. The detached arm installs the hook at the lane-construction
  site (`solver_dispatch.rs`, next to `set_on_solved_send`); the in-cycle
  arm leaves it `None` (its pipe is drained under the cycle hold).
  Strictly additive: on success nothing changed, no pipe delivery is
  fabricated, and the in-flight gauge is untouched.
- **Typed drain-death record + sticky cordon.** New
  `DrainFailure { SendFailed { pid, unit, seat }, MergePanic { message }, Stranded { pid } }`
  and the ONE response function `drain_death_response(&DrainFailure, Option<&PostureOwner>)`.
  It increments the dedicated counters, fires the existence-proven FF-T4
  sticky cause `PostureCause::LaneDeath` (only a fresh process lifts it),
  and emits a rate-limited-but-continuous error line
  (first occurrence, then every 256th — never a once-only line an operator
  can miss).
- **Sidecar panic guard (hole 2).** `detached_merge_sidecar`
  (`solver_dispatch.rs`) now wraps each `engine.lock().merge_detached_item(item)`
  in `catch_unwind` (`panic = "abort"` is NOT set anywhere in
  `rust/Cargo.toml`, so unwinding is live). A caught panic becomes
  `DrainFailure::MergePanic` + the same sticky cordon, the still-queued
  tail is drained and counted as `DrainFailure::Stranded`, and the sidecar
  returns. Every later send then hits the dead pipe and fires the send
  failure path — the death is a **terminal state**, not a silent strand.
  The function gained an `owner: Option<&PostureOwner>` parameter for
  hermetic tests; production (`spawn_merge_sidecar`) passes `None` (the
  process owner).
- **Telemetry.** `degenbot.detached.send_failed_total`
  (`degenbot.detached.send_failed`) and `degenbot.detached.merge_panic_total`
  were added to `PipelineInstruments`, both **zero-initialized** in
  `PipelineInstruments::new` (mirroring the 9395c481b pattern — a missing
  series must never read as "no lost outcomes"), with no-otel no-op twins in
  `lib.rs`.
- **Test seam.** `#[cfg(test)] test_merge_panic` hook on
  `ArbitrageEngine` + `set_merge_panic_hook`, invoked at the top of
  `merge_detached_item` (the existing `test_solve_panic` precedent).

## Classification decision (and why)

**A dead merge seat is the FF-T4 LANE-DEATH terminal class — a sticky
posture cordon with a LIVE process — NOT a `failure_policy` Fatal/Exit
bucket.**

Justification:

1. The pipe's only consumer is the sidecar thread, and
   `spawn_merge_sidecar` is spawned exactly once per engine lifetime, so a
   dead merge seat **can never recover in-process**. The task offers two
   defensible terminal states (fatal abort or loud sticky cordon); I chose
   the one the house already built for exactly this shape.
2. The FF-T4 vocabulary in `degenbot-workers::posture` and CONTEXT.md
   already decides the shape: "lane-death terminal receipt + posture cause
   (sticky cordon; only a fresh process lifts it) ... the posture cordons
   (sticky), the process lives." `lane_death_response` (solve-lane death)
   does the same and does not abort. Reusing it is the house instruction
   ("do NOT invent a new recovery mechanism").
3. Acceptance criterion 2 requires a panic in `merge_detached_item` to
   produce "the same posture trip" and the cordon to be observable. An
   abort has no posture to observe, so the cordon is the AC-mandated path.
4. The failure-taxonomy row is deliberately NOT added: `drain_dead` /
   `drain_stall` are Fatal/Process (default action `Exit`), and entering
   that bucket while *not* exiting would make the taxonomy contradict the
   reaction. This mirrors `lane_death_response`, which also sits outside
   `failure_policy`. The loudness comes from the typed counter, the sticky
   feed transition (warn-level), and the recurring error line.

## Red → green evidence

Red-first (API-missing compile errors, the permitted red for a young API):

```
error[E0425]: cannot find type `DrainFailure` in this scope
error[E0599]: no method named `set_on_send_failed` found for struct `executor::SolveLane`
error[E0425]: cannot find function `drain_death_response` in this scope
error[E0599]: no method named `set_merge_panic_hook` found for struct `MutexGuard<...ArbitrageEngine>`
error[E0061]: this function takes 2 arguments but 3 arguments were supplied
error[E0599]: no method named `count_detached_send_failed` found for struct `PipelineInstruments`
error[E0433]: cannot find type `DrainFailure` in this scope   (x2)
error: could not compile `degenbot-bot` (lib test) due to 8 previous errors
```

Green (the four new contract tests, run by exact name):

```
arb_engine::executor::tests::a_send_against_a_dropped_receiver_fires_the_drain_death_hook ... ok
arb_engine::executor::tests::a_dead_merge_drain_cordons_the_fleet_stickily ... ok
arb_engine::tests::tests::a_panicking_merge_becomes_a_typed_record_and_a_sticky_cordon ... ok
instruments::kind_tests::detached_send_failed_counter_renders_before_any_loss ... ok
```

The tests prove: a send against a dropped Receiver fires the typed hook with
the exact `SendFailed { pid, unit, seat }` and does NOT bump the in-flight
gauge; the response cordons the fleet and the cordon survives 30 clean
throttle windows (sticky); an injected panic in `merge_detached_item` is
caught, trips the same cordon, and the sidecar returns so later sends are
`is_err`; the counter renders at 0 before it fires and at 1 after.

## Gates run

- `cd rust && cargo fmt -p degenbot-bot` — clean.
- `cargo clippy -p degenbot-bot --all-targets --features otel` — clean.
- `cargo test -p degenbot-bot --features otel --lib` — 761 passed / 0
  failed / 3 ignored.
- `cargo test -p degenbot-bot --features otel` (task gate, incl.
  integration + doc-tests) — all targets green.
- `cargo check --workspace` — clean.

## Judgment calls / deviations

- **Cordon, not abort** — see the classification section.
- **Second counter** (`degenbot.detached.merge_panic_total`) added beyond
  the task's named `send_failed_total`, because AC2 asks the panic path for
  its own typed failure record. Additive and zero-initialized.
- **`Stranded` variant** drains and counts the still-queued tail at panic
  shutdown, so that class is not silently lost either.
- **`recv` instead of `for item in merge_rx`** in the sidecar: keeps
  ownership of the Receiver for the post-panic `try_iter` drain (clippy
  `needless_pass_by_value` is explicitly expected, with the ownership
  contract in the reason).
- **No `.so` rebuild** performed: the task's gates are Rust-only
  (`cargo test` / `cargo check`), and the supervisor asked for a dirty
  tree. `just verify-build-fresh` will therefore report staleness until the
  wheel is rebuilt — expected, not a regression.

## Accepted gaps

- The panicked item's in-flight gauge bump is not decremented (the item
  never reached a merge receipt; the same was true of the pre-fix
  thread-kill). Decrementing would fabricate a receipt and violate the
  gauge-pairing constraint, so it is left as an honest over-count on a dead
  drain.
- The sticky cordon does not stop the solver arm (`WorkerRole::Solver` is
  `CordonClass::Never`), which is intentional per the FF-T4 design; the
  terminal state is made loud by the counter and recurring error line rather
  than by halting the process.
