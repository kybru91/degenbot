"""CLI `degenbot fleet posture` client tests (JCI2FW Part B).

The command functions are invoked directly (bypassing the `degenbot` root
group, which loads a bot config) against a live :class:`OperatorServer`
running on a background thread — proving the wire + exit/error behavior and
that each command builds the right op/payload dict. The op layer's routing
(`handle_fleet_posture_op`) is covered by
`tests/operator/test_fleet_posture_ops.py`; the harness mirrors
`tests/cli/test_path_cli.py`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any

import click
import pytest

from degenbot.cli.fleet import posture_set, posture_show
from degenbot.operator.operator_channel import OperatorServer, handle_fleet_posture_op


def _is_listening(path: str) -> bool:
    """Return True if a Unix socket at ``path`` is accepting connections.

    ``Path(path).exists()`` is NOT a readiness signal: the socket file
    appears at ``bind()`` and ``connect(2)`` is refused with ECONNREFUSED
    until ``listen()`` completes (the same kernel gate
    ``tests/cli/test_path_cli.py`` pins).
    """
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.25)
    try:
        probe.connect(path)
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _wait_until_listening(path: str, timeout: float = 5.0) -> None:
    """Block until the server at ``path`` is actually LISTENING."""
    deadline = time.monotonic() + timeout
    while not _is_listening(path):
        if time.monotonic() >= deadline:
            msg = f"unix socket at {path} not listening after {timeout}s"
            raise RuntimeError(msg)
        time.sleep(0.005)


def _start_server(handler, socket_path: str):
    """Start an :class:`OperatorServer` on a background thread + its loop.

    The CLI command functions own their own event loop (`asyncio.run`), so
    the server must live on a separate thread's loop and be reached over
    the socket.
    """
    loop = asyncio.new_event_loop()
    server = OperatorServer(handler, socket_path=socket_path)
    state: dict[str, object] = {}

    def _run() -> None:
        asyncio.set_event_loop(loop)
        state["task"] = loop.create_task(server.serve())
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    _wait_until_listening(socket_path)
    return server, loop, thread, state


def _stop_server(server, loop, thread, state) -> None:
    """Cancel the serve task, stop the background loop, and unlink the socket."""

    async def _shutdown() -> None:
        task = state["task"]
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        loop.stop()

    loop.call_soon_threadsafe(lambda: loop.create_task(_shutdown()))
    thread.join(timeout=10)
    loop.close()
    Path(server._socket_path).unlink(missing_ok=True)  # harness teardown: private attr


def _fleet_handler() -> tuple[Any, dict[str, Any]]:
    """A host handler routing through the shared fleet-posture op helper."""
    seen: dict[str, Any] = {}

    async def handler(op: str, payload: dict[str, Any]) -> dict[str, Any]:
        await asyncio.sleep(0)
        seen["op"] = op
        seen["payload"] = payload
        return handle_fleet_posture_op(op, payload)

    return handler, seen


def test_posture_set_builds_the_right_op_dict_and_prints_the_echo(tmp_path, capsys) -> None:
    """Supplied flags become exactly the set_fleet_posture payload keys."""
    handler, seen = _fleet_handler()
    socket_path = str(tmp_path / "bot.sock")
    server, loop, thread, state = _start_server(handler, socket_path)
    try:
        posture_set.callback(
            socket_path,
            cordon_enter_events=7,
            cordon_duty_percent=2.5,
        )
    finally:
        _stop_server(server, loop, thread, state)

    assert seen["op"] == "set_fleet_posture"
    # A partial patch: ONLY the supplied flags cross the wire.
    assert seen["payload"] == {"cordon_enter_events": 7, "cordon_duty_percent": 2.5}
    out = capsys.readouterr().out.strip()
    effective = json.loads(out)
    assert effective["cordon_enter_events"] == 7
    assert effective["cordon_duty_percent"] == pytest.approx(2.5)
    assert effective["posture"] in ("Nominal", "Cordoned")


def test_posture_show_prints_the_effective_policy_json(tmp_path, capsys) -> None:
    """A show sends get_fleet_posture and prints one JSON line."""
    handler, seen = _fleet_handler()
    socket_path = str(tmp_path / "bot.sock")
    server, loop, thread, state = _start_server(handler, socket_path)
    try:
        posture_show.callback(socket_path)
    finally:
        _stop_server(server, loop, thread, state)

    assert seen["op"] == "get_fleet_posture"
    assert seen["payload"] == {}
    effective = json.loads(capsys.readouterr().out.strip())
    assert effective["posture"] in ("Nominal", "Cordoned")
    for key in (
        "cordon_enter_events",
        "cordon_enter_window_ms",
        "cordon_duty_percent",
        "cordon_duty_window_ms",
        "cordon_exit_clean_ms",
        "cordon_sim_intake_floor",
    ):
        assert key in effective


def test_posture_set_refuses_an_empty_patch_locally(tmp_path) -> None:
    """No --cordon-* flag: a UsageError before anything touches the wire."""
    with pytest.raises(click.UsageError, match="at least one --cordon"):
        posture_set.callback(str(tmp_path / "never-bound.sock"))


def test_posture_set_failure_raises_click_exception(tmp_path) -> None:
    """A rejecting bot (typed refusal) surfaces as click.ClickException."""
    handler, _ = _fleet_handler()
    socket_path = str(tmp_path / "bot.sock")
    server, loop, thread, state = _start_server(handler, socket_path)
    try:
        with pytest.raises(click.ClickException, match="cordon_duty_percent"):
            posture_set.callback(socket_path, cordon_duty_percent=0.0)
    finally:
        _stop_server(server, loop, thread, state)
