"""CLI commands to inspect and re-tune the LIVE fleet posture (JCI2FW Part B).

`degenbot fleet posture show` reads the running bot's cordon posture
(effective thresholds + `Nominal|Cordoned`); `degenbot fleet posture set`
re-tunes a subset of the six typed cordon thresholds on the live process
posture owner. Both talk to the bot's `OperatorServer` Unix domain socket —
a separate process from the CLI — over the JSON-lines wire protocol in
:mod:`degenbot.operator.operator_channel`. No local config work happens
here: the client is a thin :func:`send_command` shell.

The target bot must be running with `--operator-socket <path>`. The CLI
connects to that path. The `degenbot` root group loads the normal bot config
on every invocation, so a config file is still required even though the
fleet commands themselves only need the socket path.

The set command is a PARTIAL patch: only the `--cordon-*` flags actually
supplied change the live policy; everything else keeps its current value.
At least one flag is required (an empty patch is refused). Validation of
the values lives once in the Rust core — a refusal (unknown key, out-of-range
threshold, empty patch) surfaces as the wire's `{"ok": false, "error": ...}`
and the CLI prints it as a one-line failure.
"""

from __future__ import annotations

import asyncio
import json

import click

from degenbot.cli import cli
from degenbot.operator.operator_channel import send_command


@cli.group()
def fleet() -> None:
    """Steer the live worker fleet over the operator command channel."""


@fleet.group()
def posture() -> None:
    """Inspect or re-tune the live cordon posture thresholds."""


@posture.command("show")
@click.option(
    "--socket",
    "socket_path",
    required=True,
    help="Unix domain socket path of the running bot's OperatorServer.",
)
def posture_show(socket_path: str) -> None:
    """Show the LIVE cordon posture: thresholds + Nominal|Cordoned.

    Prints one JSON line: the effective policy (all six `cordon_*` fields)
    plus the current `posture`.

    Raises:
        click.ClickException: if the bot rejects the command or the socket
            is unreachable.

    """
    response = asyncio.run(send_command(socket_path, "get_fleet_posture", {}))
    if not response.get("ok"):
        raise click.ClickException(response.get("error", "get_fleet_posture failed"))
    click.echo(json.dumps(response.get("effective", {}), sort_keys=True))


@posture.command("set")
@click.option(
    "--socket",
    "socket_path",
    required=True,
    help="Unix domain socket path of the running bot's OperatorServer.",
)
@click.option(
    "--cordon-enter-events",
    type=click.IntRange(min=1),
    default=None,
    help="Throttle events within the enter window that cordon the fleet.",
)
@click.option(
    "--cordon-duty-percent",
    type=click.FloatRange(min=0.0, min_open=True, max=100.0),
    default=None,
    help="Throttled-time duty percent over the duty window that cordons the fleet.",
)
@click.option(
    "--cordon-enter-window-ms",
    type=click.IntRange(min=1),
    default=None,
    help="Rolling window (ms) for the throttle-event burst enter trigger.",
)
@click.option(
    "--cordon-duty-window-ms",
    type=click.IntRange(min=1),
    default=None,
    help="Trailing window (ms) over which throttled-time duty is evaluated.",
)
@click.option(
    "--cordon-exit-clean-ms",
    type=click.IntRange(min=1),
    default=None,
    help="Clean-window hysteresis (ms) required before cordon exits.",
)
@click.option(
    "--cordon-sim-intake-floor",
    type=click.IntRange(min=1),
    default=None,
    help="SimDriver new-lease cap while cordoned.",
)
def posture_set(
    socket_path: str,
    *,
    cordon_enter_events: int | None = None,
    cordon_duty_percent: float | None = None,
    cordon_enter_window_ms: int | None = None,
    cordon_duty_window_ms: int | None = None,
    cordon_exit_clean_ms: int | None = None,
    cordon_sim_intake_floor: int | None = None,
) -> None:
    """Re-tune the LIVE cordon thresholds (a partial patch).

    Only the supplied `--cordon-*` flags change the live policy; the rest
    keep their current value. Boot config stays the default source: the
    re-tune applies to the running process only.

    Raises:
        click.UsageError: if no --cordon-* flag was supplied (an empty
            patch is refused before it touches the wire).
        click.ClickException: if the bot rejects the patch (unknown key,
            out-of-range threshold) or the socket is unreachable.

    """
    patch: dict[str, int | float] = {}
    if cordon_enter_events is not None:
        patch["cordon_enter_events"] = cordon_enter_events
    if cordon_duty_percent is not None:
        patch["cordon_duty_percent"] = cordon_duty_percent
    if cordon_enter_window_ms is not None:
        patch["cordon_enter_window_ms"] = cordon_enter_window_ms
    if cordon_duty_window_ms is not None:
        patch["cordon_duty_window_ms"] = cordon_duty_window_ms
    if cordon_exit_clean_ms is not None:
        patch["cordon_exit_clean_ms"] = cordon_exit_clean_ms
    if cordon_sim_intake_floor is not None:
        patch["cordon_sim_intake_floor"] = cordon_sim_intake_floor
    if not patch:
        msg = "supply at least one --cordon-* threshold (an empty patch is refused)"
        raise click.UsageError(msg)
    response = asyncio.run(send_command(socket_path, "set_fleet_posture", patch))
    if not response.get("ok"):
        raise click.ClickException(response.get("error", "set_fleet_posture failed"))
    click.echo(json.dumps(response.get("effective", {}), sort_keys=True))
