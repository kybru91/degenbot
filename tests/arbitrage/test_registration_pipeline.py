"""The PRG-5 registration crawl: bounded-window submission over the fleet.

The bounded producer/consumer queue (`run_registration_pipeline`) retired
with the crawl shell — discovery iterates directly and every path leaves as
ONE `PoolStateUpdater` intake unit (build + verify lifecycles + path
registration, all Rust-coordinated). These tests prove, over receipt
doubles (the same scripted shape the intake-station tests use):

- the submission window: at most `REG_INTAKE_WINDOW` receipts outstanding,
  FIFO resolution order (the retired workers' ordering guarantee),
- the benign cap stop: the driver stops submitting after a cap outcome and
  drains the window (the engine is full — the drain is cheap),
- the fatal contract: a unit's VerificationMismatchError propagates through
  the receipt and aborts the crawl loudly,
- counter parity: `_absorb_outcome` folds the unit outcomes into the exact
  counter shapes the retired inline `_consume` produced.

All work runs through the SAME `_consume` body the operator surfaces use,
so behavior cannot diverge by input source (the NWTUM3 bar).

Adapted from the retired pipeline tests (the shell tests back to
5TSYKN/JKYVST, retargeted at the fleet intake by epic IRUMXD PRG-5).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from degenbot.exceptions import VerificationMismatchError
from degenbot.runner.build_paths import (
    REG_INTAKE_WINDOW,
    PathRegistrationPipeline,
    RegistrationUnitOutcome,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable


class _Receipt:
    """A receipt double: wait_async runs the stored work (raising included)."""

    def __init__(self, work: Callable[[], object]) -> None:
        self._work = work
        self._run = False
        self._value: object | None = None

    async def wait_async(self) -> None:
        self._value = self._work()  # raises here exactly like the Rust seat
        self._run = True

    def result(self) -> object:
        assert self._run, "await wait_async() first"
        return self._value

    def done(self) -> bool:
        return self._run


class _FleetBot:
    """Bot double: intake surface + in-flight bookkeeping, FIFO seat pool."""

    def __init__(self, seats: int = 2) -> None:
        self.seats = seats
        self.submitted: list[Callable[[], object]] = []
        self.receipts: list[_Receipt] = []
        self.max_inflight = 0
        self.inflight = 0

    def registration_fleet_hosted(self) -> bool:
        return True

    def submit_registration_unit(self, fn: Callable[[], object]) -> _Receipt:
        self.submitted.append(fn)
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        receipt = _Receipt(self._seat(fn))
        self.receipts.append(receipt)
        return receipt

    def _seat(self, fn: Callable[[], object]) -> Callable[[], object]:
        def _run() -> object:
            try:
                return fn()
            finally:
                self.inflight -= 1

        return _run


@dataclass
class _ScriptedPath:
    """An opaque path whose unit records its processing order."""

    name: str


@dataclass
class _OpaqueStep:
    """A step whose `type` is not a pool table — the unit skips it."""

    type: type
    address: str
    hash: object | None = None


def _pipeline_with_bot(
    constr_bot: object, engine_registry: object = None
) -> tuple[PathRegistrationPipeline, object]:
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
    pipeline = PathRegistrationPipeline(context=ctx, engine_registry=engine_registry)
    return pipeline, constr_bot


async def _producer(paths: list[object]) -> AsyncIterator[object]:
    for path in paths:
        # Production discovery is a cooperative async iterator (the Rust DFS
        # yields between loop ticks) — mirror that cadence.
        await asyncio.sleep(0)
        yield path


async def test_crawl_submits_one_unit_per_path_and_resolves_fifo() -> None:
    """Every discovered path = one intake unit; receipts resolve in order."""
    order: list[str] = []
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)

    def unit_shim(path_steps: object, directions: object = None) -> object:
        order.append(path_steps.name)  # type: ignore[attr-defined]
        return RegistrationUnitOutcome(kind="registered", created=True)

    pipeline._registration_unit = unit_shim  # type: ignore[method-assign]

    await pipeline.run_registration(
        producer=_producer([_ScriptedPath(f"p{i}") for i in range(20)]),
    )

    assert len(bot.submitted) == 20
    assert order == [f"p{i}" for i in range(20)], "FIFO resolution order"
    assert pipeline.path_count == 20
    assert not pipeline.capped


async def test_crawl_window_bounds_in_flight_units() -> None:
    """Backpressure is the submission window — never more than its bound."""
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)

    def unit_shim(path_steps: object, directions: object = None) -> object:
        return RegistrationUnitOutcome(kind="registered", created=True)

    pipeline._registration_unit = unit_shim  # type: ignore[method-assign]

    await pipeline.run_registration(
        producer=_producer([_ScriptedPath(f"p{i}") for i in range(100)]),
    )

    assert bot.max_inflight <= REG_INTAKE_WINDOW
    assert len(bot.submitted) == 100


async def test_crawl_stops_submitting_after_the_cap_and_drains() -> None:
    """A cap outcome stops discovery; the window drains cheaply (PRG-4)."""
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)
    processed: list[str] = []

    def unit_shim(path_steps: object, directions: object = None) -> object:
        name = path_steps.name  # type: ignore[attr-defined]
        if len(processed) >= 3:
            return RegistrationUnitOutcome(kind="cap", tag="path-cap")
        processed.append(name)
        return RegistrationUnitOutcome(kind="registered", created=True)

    pipeline._registration_unit = unit_shim  # type: ignore[method-assign]

    await pipeline.run_registration(
        producer=_producer([_ScriptedPath(f"p{i}") for i in range(50)]),
    )

    assert pipeline.capped is True
    # The stop bounds the waste: discovery does not keep climbing to 50.
    assert len(bot.submitted) < 50
    # The benign stop folds the caps into the skip/cap counters.
    assert pipeline.cap_skip_count >= 1
    assert pipeline.path_count == 3


async def test_crawl_fatal_verification_error_aborts_loudly() -> None:
    """A unit's VerificationMismatchError propagates — no swallow, no cap."""
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)

    def unit_shim(path_steps: object, directions: object = None) -> object:
        msg = "boom"
        raise VerificationMismatchError(msg)

    pipeline._registration_unit = unit_shim  # type: ignore[method-assign]

    with pytest.raises(VerificationMismatchError, match="boom"):
        await pipeline.run_registration(
            producer=_producer([_ScriptedPath(f"p{i}") for i in range(10)]),
        )


def test_absorb_outcome_counter_parity() -> None:
    """Outcome folds mirror the retired inline branches exactly."""
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)

    # Benign skip: skip_count + skip-reason tag.
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="skip", tag="build-v3:X"))
    assert pipeline.skip_count == 1
    assert pipeline._skip_reasons["build-v3:X"] == 1

    # V4 admission refusals are counted separately, NOT in skip_count.
    pipeline._absorb_outcome(
        RegistrationUnitOutcome(kind="skip", tag="v4-hook-rejected", counts_as_skip=False)
    )
    assert pipeline.v4_hook_rejected == 1
    assert pipeline.skip_count == 1
    pipeline._absorb_outcome(
        RegistrationUnitOutcome(kind="skip", tag="v4-dynamic-fee-rejected", counts_as_skip=False)
    )
    assert pipeline.v4_dynamic_fee_rejected == 1

    # Engine reject: engine_reject + other-Exception (parity with the two
    # retired except-branches that incremented both).
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="reject"))
    assert pipeline.engine_reject_count == 1
    assert pipeline.other_exc_count == 1

    # register-fail: its own counter + the bounded warning source.
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="register-fail", tag="ValueError: x"))
    assert pipeline.register_fail_count == 1

    # Registered: created and duplicate folds; v4 hops counted in BOTH
    # (the retired body incremented v4_pool_count pre-dedup).
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="registered", created=True, v4_hops=1))
    assert pipeline.path_count == 1
    assert pipeline.v4_pool_count == 1
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="registered", created=False, v4_hops=2))
    assert pipeline.path_count == 1
    assert pipeline.dup_count == 1
    assert pipeline.v4_pool_count == 3
    assert pipeline._skip_reasons["dup"] == 1
    # The cap fold stops the crawl.
    pipeline._absorb_outcome(RegistrationUnitOutcome(kind="cap", tag="path-cap"))
    assert pipeline.capped


def test_legacy_stance_pipeline_construction_refuses() -> None:
    """The hard cutover: no fleet intake, no crawl (loud, actionable)."""
    ctx = SimpleNamespace(
        bot=SimpleNamespace(registration_fleet_hosted=lambda: False),
        chain_id=1,
        db=None,
        uniswap_v3_tracker=None,
        sushiswap_v3_tracker=None,
        pancakeswap_v3_tracker=None,
        weth=None,
    )
    with pytest.raises(RuntimeError, match="fleet-hosted only"):
        PathRegistrationPipeline(context=ctx, engine_registry=None)


def test_retired_skip_gate_pipeline_tests_upgraded_shape() -> None:
    """The skip-gate pipeline shape still builds under the fleet intake."""
    bot = _FleetBot()
    pipeline, _bot = _pipeline_with_bot(bot)
    pipeline._record_skip("v4-no-hash")
    assert pipeline._skip_reasons["v4-no-hash"] == 1


# The retired helper's tests (backpressure/FIFO/validation/fatal abort over a
# `run_registration_pipeline` queue) were retargeted above: the queue is the
# submission WINDOW, the fatal contract is the receipt re-raise, and the
# non-fatal isolation is the outcome fold. The offload-executor probe (a
# build running off the event loop thread) retired with the executor — the
# seat execution is proven by the intake-station subprocess test.


@pytest.fixture(autouse=True)
def _no_progress_noise(monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep progress logs out of the capture buffer for CI readability.
    monkeypatch.setattr(
        PathRegistrationPipeline,
        "_PROGRESS_INTERVAL_S",
        1_000_000.0,
    )
