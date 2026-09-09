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
- the pipeline's `_run_build_offloaded` routes by the construction-time
  stance (poll-join on the fleet arm, run_in_executor on the legacy arm),
  re-raising the callable's exception either way.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from degenbot._ffi import Bot
from degenbot.runner.build_paths import PathRegistrationPipeline

if TYPE_CHECKING:
    from collections.abc import Callable

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
        [sys.executable, "-c", _FLEET_DRIVER],  # ruff: ignore[start-process-with-partial-path]
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parents[2]),
        env={**os.environ, "DEGENBOT_FLEET": "fleet"},
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


def _pipeline_with_bot(constr_bot: object) -> PathRegistrationPipeline:
    """A pipeline over a supplied construction Bot (the skip-gate pattern)."""
    ctx = SimpleNamespace(
        bot=constr_bot,
        chain_id=1,
        db=None,
        uniswap_v3_tracker=None,
        sushiswap_v3_tracker=None,
        pancakeswap_v3_tracker=None,
        weth=None,
    )
    return PathRegistrationPipeline(context=ctx, engine_registry=None)


class _ScriptedReceipt:
    """A receipt double: wait_async runs the stored work (raising included)."""

    def __init__(self, work: Callable[[], object]) -> None:
        self._work = work
        self._delivered: object | None = None


class _FakeReceiptAwaiter:
    def __init__(self, receipt: _ScriptedReceipt) -> None:
        self._receipt = receipt


# The receipt exposes the production join shape: coroutine-returning
# wait_async + a repeatable result(). The double serves both directly.
def _make_receipt(work: Callable[[], object]) -> object:
    class Receipt:
        def __init__(self) -> None:
            self._run = False
            self._value: object | None = None

        async def wait_async(self) -> None:
            self._value = work()  # raises here exactly like the Rust seat
            self._run = True

        def result(self) -> object:
            assert self._run, "await wait_async() first"
            return self._value

    return Receipt()


class _FakeFleetBot:
    """Bot double exposing the intake surface with a scripted receipt."""

    def __init__(self) -> None:
        self.submitted: list[Callable[[], object]] = []

    def registration_fleet_hosted(self) -> bool:
        return True

    def submit_registration_unit(self, fn: Callable[[], object]) -> object:
        self.submitted.append(fn)
        return _make_receipt(fn)


class _LegacyBot:
    def registration_fleet_hosted(self) -> bool:
        return False


async def test_fleet_arm_poll_joins_the_intake_receipt() -> None:
    """The fleet arm submits through the FFI and poll-joins the receipt."""
    pipeline = _pipeline_with_bot(_FakeFleetBot())
    result = await pipeline._run_build_offloaded(lambda: 5)
    assert result == 5


async def test_fleet_arm_reraises_the_callable_exception() -> None:
    """A failing build re-raises on the driver (skip tags stay identical)."""
    pipeline = _pipeline_with_bot(_FakeFleetBot())

    def failing() -> object:
        msg = "boom"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="boom"):
        await pipeline._run_build_offloaded(failing)


async def test_legacy_arm_uses_the_bounded_worker_pool() -> None:
    """The legacy stance keeps run_in_executor over the bounded pool."""
    pipeline = _pipeline_with_bot(_LegacyBot())
    assert pipeline._fleet_intake is False
    sentinel = object()
    result = await pipeline._run_build_offloaded(lambda: sentinel)
    assert result is sentinel
