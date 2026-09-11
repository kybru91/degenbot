"""CLI commands for degenbot operations."""

import click

from degenbot.bot import Bot
from degenbot.exceptions import BootRefused, DatabaseSchemaStale

# FF-T1 (BPHR6F): the degenbot binary maps the typed fleet boot refusal
# (the library never aborts the host process on that arm) to its loud
# named fail-fast exit. 78 is sysexits EX_CONFIG: the host cannot host
# the fleet configuration.
EXIT_FLEET_BOOT_REFUSED = 78


class DegenbotCLI(click.Group):
    """The degenbot CLI root group.

    Catches :class:`DatabaseSchemaStale` (raised by the degenbot-db PyO3 seam
    when the DB is stamped at a prior Alembic revision) and prints a friendly
    one-line remediation hint instead of a Python traceback. End users see
    "run `degenbot database upgrade`", not a wall of stack frames.

    Also maps the typed fleet boot refusal (:class:`BootRefused`, FF-T1) to
    a single named line and exit code 78 (sysexits EX_CONFIG): the library
    never aborts the host process on the boot-refusal arm; the binary owns
    the loud fail-fast exit.

    The ``database upgrade`` command itself is unaffected: its
    :func:`upgrade_existing_sqlite_database` shell catches
    :class:`DatabaseSchemaStale` first and runs the Alembic migration. This
    outer catch is the safety net for every *other* subcommand that trips a
    stale DB mid-operation.
    """

    def invoke(self, ctx: click.Context) -> object:
        """Run the CLI, translating stale-DB errors into a friendly hint.

        Returns:
            The result of delegating ``invoke`` to the parent group, unless a
            stale-DB error is caught (then ``ctx.exit(1)`` terminates).

        """
        try:
            return super().invoke(ctx)
        except DatabaseSchemaStale as exc:
            click.echo(str(exc), err=True)
            ctx.exit(1)
        except BootRefused as exc:
            click.echo(f"[fleet-boot] REFUSED — {exc}", err=True)
            ctx.exit(EXIT_FLEET_BOOT_REFUSED)


@click.group(cls=DegenbotCLI)
@click.version_option()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Perform cli."""
    ctx.obj = Bot.from_config_file()


from . import (  # ruff:ignore[unused-import, module-import-not-at-top-of-file]
    aave,
    database,
    exchange,
    fleet,
    path,
    pool,
)
