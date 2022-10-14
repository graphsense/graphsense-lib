import click

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
    click.echo(cfg.text())


@config.command("path")
def path():
    """Prints the path where the config is loaded from."""
    click.echo(cfg.path())


@config.command("template")
def default():
    """Generates a configuration template."""
    click.echo(cfg.generate_yaml(DEBUG=False))
