import click

from ..utils import subkey_get
from ..utils.console import console
from .config import config as cfg


@click.group()
def config_cli():
    pass


@config_cli.group("config")
def config():
    """Inspect the current configuration of graphsenselib."""
    pass


@config.command("show")
@click.option("--json/--text", default=False)
def show(json):
    """Prints the configuration used in the environment."""
    if json:
        console.print_json(cfg.json())
    else:
        console.print(cfg.text())


@config.command("get")
@click.option(
    "--path",
    help="path in the config file sep. is a dot (.)",
    type=str,
    required=True,
    default=False,
)
def get(path):
    """Prints the configuration used in the environment."""
    console.print(subkey_get(cfg.dict(), path.split(".")))


@config.command("path")
def path():
    """Prints the path where the config is loaded from."""
    console.print(cfg.path())


@config.command("template")
def default():
    """Generates a configuration template."""
    console.print(cfg.generate_yaml(DEBUG=False))
