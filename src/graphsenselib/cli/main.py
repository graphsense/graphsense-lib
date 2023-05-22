import click
from rich.traceback import install

from .. import __version__
from ..config.cli import config_cli
from ..convert.cli import convert_cli
from ..db.cli import db_cli
from ..deltaupdate.cli import deltaupdate_cli
from ..ingest.cli import ingest_cli
from ..monitoring.cli import monitoring_cli
from ..rates.cli import rates_cli
from ..schema.cli import schema_cli
from ..utils.console import console
from ..utils.logging import configure_logging
from ..utils.slack import ClickSlackErrorNotificationContext
from ..watch.cli import watch_cli
from .common import try_load_config

__author__ = "iknaio"
__copyright__ = "iknaio"
__license__ = "MIT"


@click.group()
def version():
    """Print version info."""
    pass


@version.command("version")
def version_cmd():
    """Display the current version."""
    console.print(__version__)


@click.command(
    cls=click.CommandCollection,
    sources=[
        rates_cli,
        schema_cli,
        ingest_cli,
        db_cli,
        deltaupdate_cli,
        config_cli,
        convert_cli,
        monitoring_cli,
        watch_cli,
        version,
    ],
    epilog="GraphSense - https://graphsense.github.io/",
)
@click.option(
    "-v", "--verbose", count=True, help="One v for warning, two for info etc."
)
@click.option(
    "--config-file",
    type=str,
    help="Change the config file to use. If blank default config location is loaded.",
    required=False,
)
@click.pass_context
def cli(ctx, verbose: int, config_file: str):
    """Commandline interface of graphsense-lib

    graphsense-cli exposes many tools and features to manager your
    graphsense crypto-analytics database.
    \f
    Args:
        verbose (int): One v stands for loglevel warning, two for info and so on...
    """
    config = try_load_config(config_file)
    ctx.with_resource(
        ClickSlackErrorNotificationContext(
            config.get_slack_exception_notification_hook_urls()
        )
    )
    configure_logging(verbose)


def main():
    """install rich as traceback handler for all cli commands"""
    install(show_locals=True, suppress=[click])

    cli()


if __name__ == "__main__":
    main()
