# Migrating the operator config.toml to the typed BotConfig layout

**Ergo JLFE2F — Option B hard cutover (0.6, pre-release). No shim layer: retired items are refused at boot, not translated.**

## What changed

The operator file `~/.config/degenbot/config.toml` (or its `DEGENBOT_CONFIG` override) is now the **typed Rust file layer** of `degenbot-config`'s `BotConfigLoader`. Every top-level table must name a declared schema section, and every key must be a declared key — the loader fails closed, aggregating every problem before reporting.

The pre-0.6 file vocabulary (`[rpc]`, `[ws]`, `[database]`, `[otel]`, top-level `default_chain_id`) was Python-driver domain the typed schema never carried. Those items are **retired**: a file containing them is refused at boot with a pointed error naming its replacement.

## Replacement table

| Retired item | Replacement |
|---|---|
| `[rpc]` per-chain endpoints (`[rpc]\n1 = "http://…"`) | `DEGENBOT_RPC_HTTP_CHAINID_<chain>` env names (or the Python config cascade, `src/degenbot/config.py`) |
| `[ws]` per-chain endpoints | `DEGENBOT_RPC_WS_CHAINID_<chain>` env names (or the Python config cascade) |
| `[database]` `filepath` | Python config cascade (`src/degenbot/config.py`, `DatabaseSettings`) |
| `[otel]` `endpoint` / `enabled` | the modern `telemetry` section: `telemetry.otel` (toggle) and `telemetry.jaeger_endpoint` (OTLP endpoint) |
| top-level `default_chain_id` | Python config cascade (`src/degenbot/config.py`) |

## What is NOT retired

`[failure_policy]` is deliberately **not** typed and **not** rejected: it is the ADR-040 D3 free-form per-bucket override table, read as a raw TOML table from the same file the loader selected (`BotConfigLoader::file_path()`). Files may keep it unchanged.

## The new layout

Every schema section doubles as a config-file table. The authoritative key reference is generated from the schema — regenerate with `REGEN_CONFIG_DOCS=1 cargo test -p degenbot-config` (lands in [`rust-config-keys.md`](rust-config-keys.md)). Layer precedence: CLI override > `DEGENBOT_*` env > config file > declared default, with every assignment recorded in provenance.

## Example migration

Before (pre-0.6):

```toml
default_chain_id = 1

[rpc]
1 = "http://localhost:8545"

[database]
filepath = "./degenbot.db"

[otel]
endpoint = "http://localhost:4318"
```

After (0.6):

```toml
[telemetry]
otel = true
jaeger_endpoint = "http://localhost:4318"

[failure_policy]  # unchanged, still free-form
```

with the Python-domain settings supplied through the environment (`DEGENBOT_RPC_HTTP_CHAINID_1=http://localhost:8545`) or the Python config cascade (`BotConfig(database=…, rpc={1: "http://localhost:8545"}, default_chain_id=1)`).

Boot behavior: a surviving retired item fails the load and the process exits 2 with a message like

```
bot configuration invalid (1 problem(s)):
  - --config /home/you/.config/degenbot/config.toml: retired config-layout item [rpc] is no longer supported — move per-chain RPC endpoints to the DEGENBOT_RPC_HTTP_CHAINID_<chain> env names (or the Python config.py cascade); see docs/config-migration.md
```
