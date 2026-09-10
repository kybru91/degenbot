"""Tests for the 0.6 modern config.toml layout loading in Python (ergo JLFE2F).

The shared operator file ~/.config/degenbot/config.toml is now the typed
Rust BotConfig file layer: its tables are the schema sections (telemetry,
failure_policy, ...), and the pre-0.6 Python-domain sections ([database],
[rpc]/[ws], default_chain_id) have LEFT the file (they resolve through the
env/cascade layers). The Python DegenbotConfig therefore must load a
modern-layout file that carries NO Python-domain sections at all, defaulting
database to the standard DB_PATH and leaving rpc/ws empty (the RPC cascade
then resolves endpoints from env or the caller).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from degenbot.config import CONFIG_DIR, load_config_from_file

if TYPE_CHECKING:
    from pathlib import Path


# The modern layout: ONLY typed-Rust schema sections (both are also read by
# the live Python readers — failure_policy by the Rust log layer, telemetry
# by nothing on the Python side — plus it exercises the default extra-key
# posture for every other schema section).
MODERN_LAYOUT = """\
[telemetry]
otel = true
jaeger_endpoint = "http://localhost:4318"
metrics_addr = "0.0.0.0:9464"

[failure_policy]
"""


def test_modern_layout_file_loads_in_python(tmp_path: Path) -> None:
    """A modern-layout file with no Python-domain sections loads with defaults."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(MODERN_LAYOUT, encoding="utf-8")

    config = load_config_from_file(cfg)

    from degenbot.config import DB_PATH

    assert config.database.path == DB_PATH, "database defaults to the standard path"
    assert config.rpc == {}, "rpc is empty (env/cascade layer owns endpoints)"
    assert config.ws == {}, "ws is empty (env/cascade layer owns endpoints)"
    assert config.default_chain_id is None
    assert config.failure_policy == {}


def test_modern_layout_sections_are_ignored_not_errors(tmp_path: Path) -> None:
    """Unknown-to-Python schema sections (telemetry ...) load without error."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(MODERN_LAYOUT + "\n[runtime]\nio_workers = 4\n", encoding="utf-8")

    config = load_config_from_file(cfg)
    assert config.rpc == {}
    assert CONFIG_DIR.name == "degenbot", "sanity: standard config dir"
