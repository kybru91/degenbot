"""The session watch's end-state matrix (ergo MJJUXL).

The cockpit's ONE owner of a pump session's end-state
(``degenbot.runner._session_watch`` — the CONTEXT.md *session watch* term):
the watch-set assembly ({consumer} + optional {registration, watchdog},
exactly the task sets the former twin await loops watched), the
``SessionEndVerdict`` ranking (a fail-fast registration verdict outranks a
watchdog verdict in the same wait batch — written once, not per loop), and
the idempotent teardown the ``run()`` finally / ``__aexit__`` / twin-loop
sites hand-rolled.

Pure asyncio — no engine, no RPC (ADR-019: the watch stays inside
``degenbot.runner`` and coordinates plain ``asyncio.Task`` objects).
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest

from degenbot.runner._session_watch import SessionEndVerdict, SessionWatch


async def _quick_consumer(ticks: int = 1) -> None:
    """A consumer that ends on its own after a few loop ticks."""
    for _ in range(ticks):
        await asyncio.sleep(0)


async def _hanging_consumer() -> None:
    await asyncio.Event().wait()


def _never_watchdog() -> Coroutine[Any, Any, bool]:
    """A watchdog whose pump never finishes (never returns True)."""

    async def _watchdog() -> bool:
        await asyncio.Event().wait()
        return False  # pragma: no cover - the event never sets

    return _watchdog()


def _instant_false_watchdog() -> Coroutine[Any, Any, bool]:
    """Injected/test engines: no pump-finished surface at all."""

    async def _watchdog() -> bool:
        return False

    return _watchdog()


def _pump_end_watchdog(consumer: asyncio.Task[Any]) -> Coroutine[Any, Any, bool]:
    """Production watchdog semantics (``_pump_finished_watchdog``): the
    watchdog itself cancels the idling consumer, then reports the pump end."""

    async def _watchdog() -> bool:
        if not consumer.done():
            consumer.cancel()
        return True

    return _watchdog()


def _watch(
    consumer: asyncio.Task[Any],
    watchdog_factory: Any,
    registration: asyncio.Task[Any] | None = None,
) -> SessionWatch:
    watch = SessionWatch()
    watch.attach(consumer_task=consumer, watchdog_factory=watchdog_factory)
    if registration is not None:
        watch.attach_registration(registration)
    return watch


class TestVerdictEnum:
    def test_verdict_members_pin_the_end_state(self) -> None:
        """The settled enum: exactly the three end-state members."""
        assert {verdict.name for verdict in SessionEndVerdict} == {
            "PumpEnded",
            "RegistrationFailed",
            "WatchdogTripped",
        }


class TestWatchSetMatrix:
    async def test_consumer_alone_ends_pump_ended(self) -> None:
        """{consumer} + watchdog: the consumer ending on its own is a
        ``PumpEnded`` verdict, and the watchdog is assembled + drained."""
        consumer = asyncio.create_task(_quick_consumer())
        factory_calls = 0

        def factory() -> Coroutine[Any, Any, bool]:
            nonlocal factory_calls
            factory_calls += 1
            return _never_watchdog()

        watch = _watch(consumer, factory)

        assert await watch.wait() is SessionEndVerdict.PumpEnded
        assert consumer.done() and not consumer.cancelled()
        # The watchdog was assembled into the watch-set (both former twins
        # always created it) and wait()'s exit drained it.
        assert factory_calls == 1
        watchdog_task = watch._watchdog_task
        assert watchdog_task is not None
        assert watchdog_task.done() and watchdog_task.cancelled()

    async def test_consumer_exception_propagates_from_wait(self) -> None:
        """The consumer's own exception surfaces from ``wait()`` — the plain
        ``await main_task`` tail the twins kept."""
        boom = RuntimeError("loud abort")

        async def consumer() -> None:
            raise boom

        task = asyncio.create_task(consumer())
        watch = _watch(task, _instant_false_watchdog)

        with pytest.raises(RuntimeError) as excinfo:
            await watch.wait()
        assert excinfo.value is boom

    async def test_registration_completing_cleanly_is_dropped(self) -> None:
        """A clean registration completion is a no-op: dropped from the
        watch-set, the loop keeps blocking on {consumer, watchdog}."""
        consumer = asyncio.create_task(_quick_consumer(ticks=5))

        async def clean_registration() -> None:
            await asyncio.sleep(0)

        registration = asyncio.create_task(clean_registration())
        watch = _watch(consumer, _never_watchdog, registration)

        assert await watch.wait() is SessionEndVerdict.PumpEnded
        assert consumer.done() and not consumer.cancelled()
        assert registration.done()
        assert registration.exception() is None
        assert watch.registration_error is None

    async def test_fatal_registration_fails_fast(self) -> None:
        """A fatal registration error cancels the main-loop consumer and is
        delivered as the ``RegistrationFailed`` verdict (error retrievable
        from the watch for the caller to re-raise)."""
        consumer = asyncio.create_task(_hanging_consumer())
        boom = ValueError("verification mismatch")

        async def failing_registration() -> None:
            await asyncio.sleep(0)
            raise boom

        registration = asyncio.create_task(failing_registration())
        watch = _watch(consumer, _never_watchdog, registration)

        assert await watch.wait() is SessionEndVerdict.RegistrationFailed
        assert watch.registration_error is boom
        # Fail-fast cancelled the hot loop (and drained it).
        assert consumer.cancelled()
        assert registration.done()
        assert registration.exception() is boom

    async def test_same_batch_fail_fast_outranks_watchdog(self) -> None:
        """THE ranking, written once: when a fatal registration error and a
        watchdog trip complete in the SAME wait batch, the fail-fast verdict
        outranks the watchdog verdict (injected/fake engines return from the
        watchdog instantly — that completion is NOT a pump end)."""
        consumer = asyncio.create_task(_hanging_consumer())
        boom = ValueError("verification mismatch")

        async def failing_registration() -> None:
            raise boom

        registration = asyncio.create_task(failing_registration())

        def instant_true_watchdog() -> Coroutine[Any, Any, bool]:
            # Deliberately does NOT cancel the consumer: if this verdict won,
            # the consumer would be left pending and the verdict would be
            # WatchdogTripped — the assertion below pins which branch ran.
            async def _watchdog() -> bool:
                return True

            return _watchdog()

        watch = _watch(consumer, instant_true_watchdog, registration)

        assert await watch.wait() is SessionEndVerdict.RegistrationFailed
        assert watch.registration_error is boom
        # The consumer was cancelled by the FAIL-FAST path, not left idle.
        assert consumer.cancelled()

    async def test_watchdog_trip_ends_the_session_and_cancels_registration(
        self,
    ) -> None:
        """A real pump end outside ``stop()``: the watchdog cancels the
        consumer, the watch cancels the still-pending registration, and the
        session leaves via ``WatchdogTripped`` without raising."""
        consumer = asyncio.create_task(_hanging_consumer())

        async def climbing_registration() -> None:
            await asyncio.Event().wait()

        registration = asyncio.create_task(climbing_registration())
        watch = _watch(consumer, lambda: _pump_end_watchdog(consumer), registration)

        verdict = await asyncio.wait_for(watch.wait(), timeout=2.0)
        assert verdict is SessionEndVerdict.WatchdogTripped
        assert consumer.cancelled()
        # The watch requested the registration cancel (the former twin's
        # watchdog-True branch)...
        assert registration.cancelling() >= 1
        # ...and the teardown duty drains it to a clean cancelled state.
        await watch.teardown()
        assert registration.cancelled()

    async def test_instant_false_watchdog_is_dropped_not_a_pump_end(self) -> None:
        """The injected-engine shape: an instantly-False watchdog must be
        dropped from the watch-set instead of misread as a pump end — and a
        LATER registration failure must still be surfaced (never swallowed)."""
        consumer = asyncio.create_task(_quick_consumer(ticks=5))
        boom = ValueError("late registration failure")

        async def late_failing_registration() -> None:
            for _ in range(2):
                await asyncio.sleep(0)
            raise boom

        registration = asyncio.create_task(late_failing_registration())
        watch = _watch(consumer, _instant_false_watchdog, registration)

        assert await watch.wait() is SessionEndVerdict.RegistrationFailed
        assert watch.registration_error is boom
        assert consumer.cancelled()

    async def test_instant_false_watchdog_with_consumer_alone_still_ends(
        self,
    ) -> None:
        """{consumer} + instantly-False watchdog (injected engine, no
        registration): the session still ends through the consumer — the
        False is not a pump end and not a hang."""
        consumer = asyncio.create_task(_quick_consumer(ticks=3))
        watch = _watch(consumer, _instant_false_watchdog)

        assert await watch.wait() is SessionEndVerdict.PumpEnded
        assert consumer.done() and not consumer.cancelled()


class TestTeardown:
    async def test_teardown_cancels_pending_consumer_and_registration(self) -> None:
        """The ``__aexit__`` consumer-cancel + ``run()``-finally registration
        drain duties with ``wait()`` never called (a run() that raised before
        the main loop)."""
        consumer = asyncio.create_task(_hanging_consumer())

        async def climbing_registration() -> None:
            await asyncio.Event().wait()

        registration = asyncio.create_task(climbing_registration())
        watch = _watch(consumer, _never_watchdog, registration)

        await watch.teardown()
        assert consumer.cancelled()
        assert registration.cancelled()

    async def test_teardown_is_idempotent(self) -> None:
        """A second ``teardown()`` is a no-op — nothing to cancel, no raise."""
        consumer = asyncio.create_task(_quick_consumer())
        watch = _watch(consumer, _never_watchdog)

        assert await watch.wait() is SessionEndVerdict.PumpEnded
        await watch.teardown()
        await watch.teardown()
        assert consumer.done()

    async def test_teardown_with_no_attached_tasks_is_a_noop(self) -> None:
        """The ``__aexit__``-before-``run()`` shape: a bare watch tears down
        nothing and does not raise."""
        watch = SessionWatch()
        await watch.teardown()
