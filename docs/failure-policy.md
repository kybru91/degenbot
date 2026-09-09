# Failure bucket configuration

Every failure the Rust core surfaces goes through a **closed bucket table**
([ADR-040](adr/ADR-040-per-bucket-failure-reactions.md)): each bucket declares
a severity, the surface it taints, and a default action. You can change the
action per bucket in `~/.config/degenbot/config.toml`:

```toml
[failure_policy]
ws_completeness = "degraded"              # keep running through WS drops (operator decision)
"solver_state_desync.missed_log" = "observe"  # reason-level override, quoted dotted key

[failure_policy.sim_failure]              # sub-table form is equivalent to quoting
pre_encode = "quarantine"
revert_economics = "observe"
```

## Actions

| Action | Meaning |
|---|---|
| `observe` | Metrics only; never surfaces through `degenbot.errors`. |
| `event` | Keyed loud event (deduped per pool, 10-block window); keep running. |
| `quarantine` | Loud event + exclude the taint scope from solve resolution; keep running. |
| `exit` | Loud event + flush + process exit (fail-fast). |

## Buckets

Closed set, one row per bucket (from `telemetry::error_kind` + reason):

| Bucket | Default | Overrides allowed? |
|---|---|---|
| `solver_state_desync.missed_log` / `.unhandled_reorg` / `.storage_mutated` / `.unclassified` | `quarantine` (pool) | yes — containment must be re-declared, never assumed off |
| `solver_state_desync.delivery_lag` | `event` (report-only per ADR-021 Part B) | yes |
| `late_log` | `event` (path) — delivery-jitter lateness, see below | yes |
| `sim_failure.pre_encode` | `quarantine` (path) | yes |
| `sim_failure.revert_pool_state` | `event` (pool, escalate on tripwire corroboration) | yes |
| `sim_failure.revert_economics` | `observe` (benign economics) | yes |
| `sim_failure.rpc` | `event` | yes |
| `ws_completeness` | `exit` | yes — `"event"` records the drop and keeps running; the resulting state gap surfaces via the desync/quarantine path, and `DegenbotProcessDown` covers real death |
| `submit_failure` / `monitor_failure` | `event` | yes |
| `verify_mismatch` | `quarantine` (deny admission) | yes |
| `drain_stall` | `exit` | yes — `"event"` records + re-arms the watchdog (WARNING: continuing past a stalled drainer means pricing on a frozen clock; intended for bisecting only) |

### The `late_log` bucket (HJ5HWF — late-log admission safety)

A forward log that arrives **after its block's D1 tombstone** (the first
successor log) is delivery-jitter lateness — the expected shape when a
tightened settle window (the pump debounce, 50ms → 16ms) publishes a block
before its final WS logs cross the quiesce edge, or when the WS feed delivers
out of order. The state machine takes the **benign late-admit path**: the log
is dropped **un-applied** (writers stay confined to the block's Streaming
window — invariant I4), the delivery cutoff the tombstone set never moves
(I7), and the pump **keeps running**.

Lateness is never a fatal, tainted, or tripwire signal:

- the raw rate lands in the dedicated metric family
  `degenbot.late_log.admitted` (zero or near-zero on healthy feeds; a
  sustained rate says the WS reordered, not that the state machine
  misbehaved);
- the deduped `late_log` `event` (one per 10 blocks per endpoint) names
  the benign path explicitly in its message so a burst of drops under a new
  settle window can never masquerade as a structural bug;
- it is distinct from `degenbot.reorg.recovery_dropped`, which counts only
  silent single-writer duplicates inside an authoritative catch-up's owned
  range.

The completeness verify at the tombstone/Published edge (the `ws_completeness`
cross-check) remains the loud safety net for genuinely **dropped** WS logs —
late-but-delivered logs are a disjoint, benign class.

Undeclared buckets (new kinds from an upgrade before you override them) follow
the conservative **degraded** floor and log a warning — they are never fatal
and never silent.

## Validation and boot behavior

- Config lives in `~/.config/degenbot/config.toml` (the same file as `otel`).
- Keys are **closed**: `kind` or `kind.reason` from the tables above. An
  unknown bucket is a **boot error**: the process prints
  `[failure_policy] invalid override - boot refused: unknown failure bucket: "..."`
  and exits 2 before any trading.
- Action values are the four strings above; anything else is also a boot
  error (`unknown failure action: "..."`).
- Every installed override is logged at boot (level INFO, greppable under
  `failure_policy overrides installed`) — a softened quarantine is an operator
  decision that stays visible.
- Changes require a bot restart; eval happens at import time, before any
  solve.

## How to choose (targets)

- Softer containment only ever buys you **uptime**, never correctness —
  quarantined pools are excluded from solve by construction, the solver
  cannot act on divergent state in any tier.
- `exit` remains sensible for `process`-scope buckets; a blind or wedged bot
  loses money as surely as a dead one, just more quietly.
- Override a **tainted** bucket only when you have a specific reason (e.g.
  bisecting a suspected decoder bug with the probe attrs)
  and monitor the `degenbot.quarantine.events` + `degenbot_engine_quarantined_pools`
  panels on the overview dashboard while it applies.
