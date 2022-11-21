import click

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
def show():
    """Prints the configuration used in the environment."""
    console.print(cfg.text())


@config.command("path")
def path():
    """Prints the path where the config is loaded from."""
    console.print(cfg.path())


@config.command("template")
def default():
    """Generates a configuration template."""
    console.print(cfg.generate_yaml(DEBUG=False))
