"""PRG-3 (IRUMXD): the fleet registration intake station.

Under the fleet stance (`fleet.stance=fleet`) the crawl's pool-build
consumers ride the fleet's duty-counted `PoolStateUpdater` seats (census
row `fleet_pool_state_updater_slots`, Deferrable cordon class) instead of
a private `ThreadPoolExecutor`. These tests prove the surfaces:

- the FFI station end to end (subprocess with the stance env — the boot
  descriptor installs at module init + engine construction, so the
  process-wide stance cannot be flipped in-process): callables execute on
  named `work-fleet-poolupd-{n}` seats and results ride the receipt;
- the legacy stance keeps the incumbent worker pool (`submit_registration_unit`
  refuses loudly);
- the pipeline refuses to construct at all under a legacy stance (PRG-5:
  the hard cutover — fleet-hosted only, actionable refusal).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from degenbot._ffi import Bot

_FLEET_DRIVER = """
import threading

from degenbot._ffi import ArbitrageEngine, Bot

# The stance is read at module init (first-wins holder): this process boots
# WITH the fleet stance, and the ENGINE construction installs the intake
# boot descriptor (same typed config).
pre = Bot(1)
assert pre.registration_fleet_hosted() is False, 'pre-engine'
ArbitrageEngine(py_bot=pre)  # engine construction installs the intake boot
probe = Bot(1)
assert probe.registration_fleet_hosted() is True, 'post-engine'

names: set[str] = set()
lock = threading.Lock()

def work(i: int) -> int:
    with lock:
        names.add(threading.current_thread().name)
    return i * 2

receipts = [probe.submit_registration_unit(lambda i=i: work(i)) for i in range(16)]
assert [r.wait(timeout=30.0) for r in receipts] == [i * 2 for i in range(16)]
assert names, 'units executed'
assert all(n.startswith('work-fleet-poolupd-') for n in names), names
# Receipt probes: done() flips before result() delivers.
assert all(r.done() for r in receipts)
print('FLEET-OK')
"""


def test_fleet_station_executes_callables_on_named_poolupd_seats() -> None:
    """End-to-end: stance env -> boot install -> named-seat receipts."""
    proc = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true] — trusted binary, args list, no shell
        [sys.executable, "-c", _FLEET_DRIVER],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parents[2]),
        # PRG-3 test originally injected DEGENBOT_FLEET=fleet; that env var was
        # retired loudly by the CQLMM2 stance cutover (config cutover JLFE2F,
        # commit 2729b52bf) — the worker fleet is now the ONLY behavior, so the
        # subprocess just inherits the env and boots fleet-hosted by default.
        timeout=120,
        check=False,
    )
    assert "FLEET-OK" in proc.stdout, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"


def test_legacy_stance_keeps_the_incumbent_pool() -> None:
    """No engine stance installed (test-session default): intake refuses."""
    bot = Bot(1)
    # The pytest session boots WITHOUT the fleet stance (module-init holder
    # read the live env), so the intake gate refuses loudly and the legacy
    # worker pool remains the execution home.
    if bot.registration_fleet_hosted():
        pytest.skip("session env carries DEGENBOT_FLEET=fleet")
    with pytest.raises(RuntimeError, match="not fleet-hosted"):
        bot.submit_registration_unit(lambda: 1)


def test_legacy_stance_pipeline_construction_refuses() -> None:
    """PRG-5 hard cutover: a pipeline over a legacy-stance bot refuses."""
    bot = Bot(1)
    if bot.registration_fleet_hosted():
        pytest.skip("session env carries DEGENBOT_FLEET=fleet")
    ctx = SimpleNamespace(
        bot=bot,
        chain_id=1,
        db=None,
        uniswap_v3_tracker=None,
        sushiswap_v3_tracker=None,
        pancakeswap_v3_tracker=None,
        weth=None,
    )
    from degenbot.runner.build_paths import PathRegistrationPipeline

    with pytest.raises(RuntimeError, match="fleet-hosted only"):
        PathRegistrationPipeline(context=ctx, engine_registry=None)
