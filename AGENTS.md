## Architecture

`degenbot` has migrated from a pure-Python library to a Rust core composed of standalone crates. The end state has two equally first-class consumers:

1. **Pure-Rust MEV bot.** Someone should be able to `cargo add degenbot` (the umbrella crate re-exporting the cores) and build a fully functional MEV bot using Rust components ONLY without involving Python. That core must own **everything** a functional MEV bot needs. The Rust core must be capable of performing every action the bot requires. **Rust is the engine; Python is a driver shell, not a co-implementation.**
2. **Python-driven MEV bot.** Someone in Python should be able to build a functional MEV bot using the Python interface as a **driver** over the same Rust core, via a thin PyO3 layer that translates Python calls into Rust calls.

## Backwards Compatibility
Design standalone features without a backwards compatibility layer. Implement add a feature flag to allow parallel implementations if necessary, followed by a hard cutover.

## Planning
Use `ergo` for all planning. Discover usage with `ergo --help` and `ergo quickstart`. Include detailed implementation and planning notes in the body of each task.

## Refactoring & Feature Development
Use red/green test-driven development when refactoring and adding new features. Use `/skill:tdd` for guidelines.

## Complex System State
Prefer enum-based finite state machines to manage transitions within systems. When you encounter an existing system with ad-hoc rules and detailed comments meant to clarify complex interactions, propose a refactor to encapsulate that logic into a state machine.

## Commands
See the justfile.

## Rebuilding the Rust `.so` after edits
`uv run maturin develop` and even `cargo clean -p <crate>` do **not** reliably force a from-source recompile of the PyO3 `.so` — maturin uses cached artifacts across different feature-flag hash variants and `uv sync` installs a pre-built wheel in milliseconds. An apparently successful rebuild (~0.3–6s compile, no errors) silently ships a **stale `.so`** that doesn't contain the changes. This has bitten multiple sessions.

The only reliable way to force the `.so` to pick up Rust source changes:

```bash
uv sync --reinstall-package degenbot
```

Workflow after any Rust edit — verify, don't guess:

1. `just verify-build-fresh`. Exit 0 ⇒ the installed extension already
   contains your edits; no rebuild needed.
2. Exit 1 ⇒ run the reinstall above, then verify again. Only trust a bot run
   (or a pytest suite) once the check exits 0.

### Verifying freshness with the build receipt

Do not trust a silent "successful" rebuild — verify it. Every compile of
`degenbot_rs` runs `rust/crates/degenbot-python/build.rs`, which fingerprints
the crate's sources and writes `<count> <fingerprint>` to a receipt file
(`.build-number`, gitignored, at the repo root), embedding both values in the
compiled library. The counter advances only when the fingerprint (source
content) changes, so test/feature-variant rebuilds never false-positive.

```bash
uv run --no-sync python -m degenbot.build_info   # exit 1 if stale
# or:
just verify-build-fresh
# or from Python:
from degenbot.build_info import verify_build_fresh; verify_build_fresh()
# raw values: degenbot._ffi.build_number() / degenbot._ffi.build_fingerprint()
```

The check compares the installed fingerprint against the repo receipt, so any
material built from different sources than the installed artifact (the
cached-wheel failure mode) is caught, and no-change recompiles stay fresh.
`pytest tests/test_build_info.py` gates on this too — a stale `.so` fails the
suite. The receipt lives outside `rust/target` so `cargo clean` and
`just gc-target` can never roll it back. After Rust edits expect the gate to
flag staleness until you rebuild the wheel (`uv sync --reinstall-package
degenbot`) — that is the detector working, so run the rebuild, not a skip. A
reported number of 0 (or a missing fingerprint) means `build.rs` did not run —
investigate before trusting the build.

## Python Environment
Use `uv`.

### Schema ownership & Alembic retention (see [ADR-010](docs/adr/ADR-010-alembic-retention-and-rust-schema-cutover.md))
The database schema is **Alembic-owned during the 0.6.x point releases** and becomes **Rust-owned** in a 0.7 release. The cutover mechanism (`degenbot database cutover` + the `ensure_schema` `RustOwned` branch) is built and opt-in during 0.6.x so `pip` users can upgrade a stale database through the final Alembic revision and then cutover at a time of their choosing. Dropping the Alembic dependency and deleting the migration scripts is gated to 0.7 (ergo task `JFFQV2`).

**Forbidden-until-0.7 kill list.** No change before the 0.7 retirement task may delete or stub any of:
- `src/degenbot/migrations/` (the Alembic migration scripts) — deletion is gated on the `heal` operation shipping and being proven (epic `TGIP5N`, tasks T2-T5; see ADR-011) **and** the 0.7.0 release decision (T6 / `OXKANZ`), not just on the 0.7.0 version bump;
- the `alembic` and `sqlalchemy` entries in `pyproject.toml`;
- `DatabaseSessionManager` and the SQLAlchemy `src/degenbot/database/models/` package;
- the `ALEMBIC_HEAD` constant in `rust/crates/degenbot-db/src/schema.rs`;
- the `alembic_version`-reading branch of `rust/crates/degenbot-db/src/migrate.rs::ensure_schema`;
- the `PRAGMA query_only=on` setting on the `AlembicCurrent` path in `DegenbotDb::open`.

## Grafana dashboard sync (do not edit provisioned dashboards via API)

The dashboards in `docs/grafana/` are the **source of truth**. A systemd **path unit** on the host
(`update-grafana-dashboards.path` → `update-grafana-dashboards.service`) watches those JSON files and
re-syncs them into the Grafana container within ~15s of any change. Consequences that have burned sessions:

- **Editing a dashboard through the Grafana HTTP API (`/api/dashboards/db`) is lost within seconds** for any
  dashboard that lives in `docs/grafana/` — the sync silently overwrites it from the repo file. Edits to
  dashboards NOT present in `docs/grafana/` (e.g. scratch/experiment dashboards) stick, which makes the
  failure look random.
- To make lasting changes to a synced dashboard, edit the JSON file in `docs/grafana/` directly and let the
  path unit pick it up (wait ~15s, or `systemctl --user start update-grafana-dashboards.service`).
- A hanging panel that shows **"Loading plugin panel..." forever is a corrupted/stale panel JSON symptom**
  (e.g. empty `pluginVersion`, empty `options`, or wrong target property names after an upgrade) — not a
  plugin problem. Normalize the panel JSON to the current Grafana schema (`pluginVersion` set to the running
  version, full `options`/`fieldConfig`, correct target keys) and it loads on a fresh page. When in doubt,
  edit a clone of a panel that renders and copy its exact shape.

- `docs/grafana/degenbot-overview.json` is **Grafana v2 dashboard schema** (`kind: Dashboard`, `apiVersion: dashboard.grafana.app/v2alpha1`), with all rows using `AutoGridLayout`. The classic `POST /api/dashboards/db` accepts this body wrapped as `{"dashboard": <file content>, "overwrite": true}` — wrap, don't transform. Don't "fix" it back to legacy `panels`+`gridPos` (schemaVersion 39) JSON: that would erase the auto-flowing row layouts and reintroduce hand-placed coordinate gaps. In v2 the layout lives under `spec.layout` (RowsLayout -> rows) and panels under `spec.elements`.
- Auto-grid sizing (from Grafana's `getNamedColumWidthInPixels`/`getNamedHeightInPixels`): `columnWidthMode` narrow=192px, standard=448px, wide=768px (custom=Npx); `rowHeightMode` short=168px, standard=320px, tall=512px. `maxColumnCount` only caps columns when the viewport is wide enough. Enum values are strict: an invalid string (e.g. `rowHeightMode: "auto"`) passes server validation but silently zeroes panel heights in the UI.
