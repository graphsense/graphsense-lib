import click

from .. import __version__
from ..config.cli import config_cli
from ..db.cli import db_cli
from ..deltaupdate.cli import deltaupdate_cli
from ..rates.cli import rates_cli
from ..schema.cli import schema_cli
from ..utils import configure_logging
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
    click.echo(__version__)


@click.command(
    cls=click.CommandCollection,
    sources=[rates_cli, schema_cli, db_cli, deltaupdate_cli, config_cli, version],
    epilog="GraphSense - https://graphsense.github.io/",
)
@click.option(
    "-v", "--verbose", count=True, help="One v for warning, two for info etc."
)
def cli(verbose: int):
    """Commandline interface of graphsense-lib

    graphsense-cli exposes many tools and features to manager your
    graphsense crypto-analytics database.
    \f
    Args:
        verbose (int): One v stands for loglevel warning, two for info and so on...
    """
    configure_logging(verbose)


def main():
    # This also validates the configuration
    try_load_config()
    cli()


if __name__ == "__main__":
    main()
