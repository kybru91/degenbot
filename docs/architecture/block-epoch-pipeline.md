# The block-epoch pipeline

Ergo epic `MROOY7` · decision record: [ADR-041](../adr/ADR-041-block-epoch-pipeline.md) · data-plane spike outcome: the stateview feasibility doc (spike `KWKEVV`; canonical copy on branch `pi-fabric/stateview-spike`)

This is the working design reference for the block-epoch pipeline: how one block
flows from logs to settled results through a single stage machine, over a single
seam between the pump and the engine. Every implementation task of the epic
builds against the stage table and seam retirement list below, which the
**user checkpoint has signed off**; code lands against them as the epic's
tasks execute.

There are two orthogonal axes:

1. **Runtime lifecycle** — process-level phases: Boot → Subscribed → SnapshotLoaded
   → Resumed. It never interleaves with per-epoch work; see its table below.
2. **The block-epoch stage machine** — the per-epoch stages of one block, below.

The existing `EnginePhase` enum (`Created`/`Subscribed`/`SnapshotLoaded`/
`Backfilled`/`Resumed`) is remapped in full onto axis 1: its role —
registration/snapshot/backfill ordering — is runtime-level, not per-epoch, so
it contributes no sub-state to the `Solved`/`Simulated` stage rows.

## Epoch invariants

- **I1.** `Epoch { block, seq }` is the *only* block coordinate. `BlockContext`
  carries it; every stage transition, decision, span, and metric records it.
  The anchor soup (solve/sim/verifier/backfill anchors, `last_solved_block`,
  `last_drained_block`, `BlockMetadata`) is deleted.
- **I2.** `seq` is monotone non-decreasing; it bumps exactly once per `Rewind`.
  `block` may regress on reorg; `seq` never does. `(block, seq)` pairs compare
  by `seq` first, then `block`.
- **I3.** A `BlockContext` whose `(block, seq)` does not match the active epoch
  fails fast — mirroring the repository's fail-loud posture (ADR-021): stale
  contexts are never silently applied.
- **I4.** Writers to `StateLock<RwLock<BotState>>` exist **only** in the
  Streaming stage of the active epoch. Quiesced..Simulated take read guards
  (cheap-read, per the spike). Gated..Finalized commit only delivery/results
  surfaces, never pool state.
- **I5.** Exactly one `Publish` per quiesce cycle; Published implies the
  block's completeness verdict (tombstone-by-successor or settle) was obtained.
- **I6.** `Rewind{to_epoch}` may originate from **any** stage; at most one
  rewind is in flight, and a second fails fast.
- **I7.** The delivery cutoff (glossary: *Last complete block*, on `BotState`)
  is monotone and never reset by a resume (ADR-028 addendum (a)); it is
  mirrored from the Finalized stage's tombstone verdict.

## Stage table

| Stage | What happens | Data-plane posture (`StateLock<RwLock<BotState>>`) | Emits / decides | Sub-state absorbed (from the six machines) | Retires |
|---|---|---|---|---|---|
| **Streaming** | Live WS log events + gap `eth_getLogs` backfill applied in log order | **The only stage permitted to write** | Dirty pools into the epoch's `EpochDelta`; recovery single-writer discard; `Backfill` request | `BlockClock` log arrival; `PumpFSM` recovery rules (ADR-028) | `DispatchOwner`'s dispatch-log path |
| **Quiesced** | All dispatched logs for the open block applied; completeness classified (tombstone vs settle; early-slice/debounce gates) | Read guard | `Notify` (header tick), `Verify` (WS completeness), `Backfill`, watchdog `Recover`/`LogSilence` — data, not timers | `BlockClock` tick detection + quiesce classification (ADR-008); `PumpFSM` watchdogs + backfill trigger (ADR-028) | inline quiesce/`block-window` reasoning |
| **Resolved** | The block's `EpochDelta` → affected-path derivation | Read guard (path index) | The epoch's affected-path set | `PathLifecycle` registration bookkeeping | `DirtySets` + `EngineSubscriber` classification |
| **Solved** | Solver runs the affected paths | **Cheap-read:** read guard through the solve — `solve_path_duration` p99 **5 ms**, `solve_gate_duration` p99 **1 ms** (budget 100 ms) | Solved candidates | Solver dispatch | `drain_lock` + engine `Mutex` on the solve path (ADR-037 shard retired here) |
| **Simulated** | In-process revm simulation — the sole executor (ADR-019) | Read guard | Derivation/profit facts per candidate (tri-state per ADR-030) | simulation bookkeeping only (`EnginePhase` carries none of this — it is runtime-lifecycle, see below) | the sim anchor of `sim_anchor.rs` (expressed as `Epoch`) |
| **Gated** | Risk/size/profit rechecks on solved+simulated candidates | None (pure evaluation) | Deliver/reject verdicts per bucket (ADR-040) | `DeliveryLifecycle` gating policy | `DrainerHealth` no-progress checks (→ machine watchdogs) |
| **Published** | Winning bundle to execution; sinks subscribe here (delivery channel, submission, Python) | None (results written to delivery/FFI surface, not pool state) | `Publish` — exactly one per quiesce cycle; RPC-disagreement `Verify` (ADR-021 upstream check retained) | `PumpFSM` settle publish gate | `DrainWork::Publish` + `DispatchOwner` + `DrainSink` routing |
| **Finalized** | Epoch closed: delivery cutoff stamped, results final | Cutoff monotone on `BotState`; never reset by resume | Epoch close | `DeliveryLifecycle` terminal stamps; registration verify-lifecycle terminal (ADR-022) | cursor stamps in `solve_coordinator.rs` (`last_solved_block`) |
| **Rewind{to_epoch}** | Reorg unwind from **any** stage to a fresh epoch at an earlier block | `ReorgJournal` restore-before-block ≤ **23 µs p99** (V3, depth 32), 20–30 ns (V2); no view machinery needed | `Epoch.seq` bump; stale contexts fail fast; epoch views above the fork invalidated | The pump's reorg-episode and resume tracking (`block_pump.rs`) | the `ReorgCoordinator` becomes the rewind executor |

The pump's (`block_pump.rs`) reorg-episode and resume tracking folds into
`Rewind` handling; its WS transport + watchdog machinery later moves to
`degenbot-ingestion` (epic task `5WTYYQ`).

### Runtime lifecycle (orthogonal axis)

| Phase | Meaning | Stage-machine relation |
|---|---|---|
| **Boot** | Process init: config, telemetry, DB | No stage machine exists |
| **Subscribed** | WS subscriptions + topic/address filters up (degenbot-ingestion) | Streams feeding; no active epoch |
| **SnapshotLoaded** | Snapshot seed epoch `E(S)` established from the store | Fresh stage machine; gap `[S+1, W]` backfilled and owned by backfill |
| **Resumed** | Live log flow enters Streaming | The per-epoch stage cycle runs; delivery-cutoff monotony preserved across resumes |

`EnginePhase` is the in-code representation of this axis, remapped here from
any per-epoch reading; its registration/snapshot/backfill ordering role is
runtime-level, never per-epoch stage sub-state.

## Epoch invariants at the stage boundaries

See invariants I1–I7 above; the spike-derived numbers binding the stage table
are: writers confined to Streaming (log-burst window p99 ≤ 100 ms);
`state_lock_hold`/`state_lock_wait` p99 ≤ **0.1 ms** across 19.6 M
acquisitions on the still-contended pre-stage-machine architecture; rewind
bounds above.

## Seam retirement list

The parallel accounting system is deleted, not wrapped (Q6: hard cutover):

1. **`DrainSink`/`Engine` dual seam → one `StageHandlers` seam.** One trait; a
   future second engine implements it or there is no second engine.
   `NoopStubEngine` is the executable spec keeping the trait honest (see
   non-goals).
2. **`DirtySets` + `EngineSubscriber` classification → `EpochDelta`.** Log
   application records touched pools as a byproduct of dispatch; affected-path
   derivation reads the delta. `EngineSubscriber` shrinks to liveness +
   notification only.
3. **`SolveCoordinator` / `drain_lock` / `DispatchOwner` / `DrainerHealth`
   dissolved.** The stage machine owns the edge conditions; delivery and
   submission are sinks at the Published edge, not seams in front of the
   engine.
4. **Engine `Mutex` off the solve path.** Stage separation makes the
   cheap-read on Quiesced..Simulated uncontended by construction.
   Registration/FFI locking via `StateLock` remains (slow operator path, not
   the solve path).
5. **Anchor soup → a single `Epoch` on `BlockContext`.** solve/sim/verifier/
   backfill anchors, `last_solved_block`, `last_drained_block`, and
   `BlockMetadata` collapse into one `Epoch` on `BlockContext`.

## Non-goals

- **Multi-engine machinery.** No registry, no routing, no second-engine
  configuration surface. Landmine guard: **`NoopStubEngine`** implements
  `StageHandlers` alongside the real arb engine and is exercised in a scripted
  conformance harness (synthetic block stream driving the full lifecycle + a
  reorg + a backfill episode, asserting hook completeness, stage order, and
  epoch monotonicity — Q4). It is test-declared: a conformance harness, never
  runtime-selectable.
- **Materialization / COW StateView machinery.** Cheap-read won unambiguously
  (spike `KWKEVV`); the alternative mechanisms' costs are retained only as
  `Rewind`-bounding data.
- **Tripwire re-expansion.** In-process desync detection retires (desync
  unrepresentable post stage-confinement); the upstream, Published-edge
  RPC-disagreement check stays and stays loud (ADR-021 posture).
- **Backwards compatibility** of any retired surface (hard cutover, Q6).
  Pinned behavioral tests are ported; stale APIs do not get a parallel life.
- **Schema changes.** ADR-010/011 Alembic ownership and the 0.7 kill list (see
  the repository `AGENTS.md`) are untouched by this epic.

## Migration order (mirror of the epic task graph)

| # | Ergo task | Lands | Stage coverage |
|---|---|---|---|
| 1 | `7LKJFY` ✅ + `KAHU5W` | Typed `BotConfig` + 12-factor parity; migrate env reads | (config axis; orthogonal) |
| 2 | `KWKEVV` ✅ | StateView spike — cheap-read for all families; Q3 gate closed | data plane |
| 3 | `T6IYKY` | `Epoch` + `BlockContext`; delete the anchor soup (seam #5) | context for all stages |
| 4 | `LXDY4C` | `EpochDelta` dirty tracking; delete `DirtySets` + subscriber classification (seam #2) under a capture-replay parity gate | Streaming → Resolved |
| 5 | `2UVG3E` | StateView data plane: write confinement + engine lock off the solve path (seam #4); reposition the ADR-021 tripwire | Streaming ↔ Quiesced..Simulated; Published (`Verify`) — **landed (cheap-read branch): `DEGENBOT_DETACHED_SOLVES` default ON (engine Mutex off the solve path); the in-process chain-vs-solver tripwire module deleted; `EpochDelta` placeholder unified onto the real ledger; lock inventory + p99 replay in the feasibility doc §5.1** |
| 6 | `YM2FZR` | The `StageHandlers` trait + `NoopStubEngine` conformance harness (target shape of seam #1) | all (trait shape) |
| 7 | `7NFYQW` | Unified stage machine: fold the six machines, `BlockPump` → thin driver, pinned tests ported verbatim; Q2's only sanctioned internal A/B gate lives here and must be deleted by task end | all |
| 8 | `BF43PM` | Per-stage OTel spans + metrics carrying epoch attributes | observable across all |
| 9 | `SZJUKL` | Seam retirement: delete `DrainSink`/`Engine`/`SolveCoordinator`/`drain_lock`/`DispatchOwner`/`DrainerHealth` (seams #1, #3) | Published-edge + drive wiring |
| 10 | `5WTYYQ` | Extract `degenbot-ingestion` (pyo3-free); Python becomes a Published-edge sink | Subscribed + Published |
| 11 | `PLRGIN` | Regression: capture-replay sweep + live Jaeger soak A/B; flip ADR-041 to implemented; update `CONTEXT.md` vocabulary | proves all |

The order above is dependency-ordered per `ergo`; #8 (`BF43PM`) and #9
(`SZJUKL`) may interleave once #7 lands.
