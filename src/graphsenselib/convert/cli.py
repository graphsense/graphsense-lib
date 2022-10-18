import click

from .accountsetl import convert_etl_to_ingestable_logs


@click.group()
def convert_cli():
    pass


@convert_cli.group()
def convert():
    """Useful file convertions tools for the graphsense infrastructure."""
    pass


@convert.command("log-to-ingestable-log")
@click.argument("inputfile", type=click.Path(exists=True))
@click.option("--prefix", type=str, default="ingestable")
def state(inputfile, prefix):
    """Summary
    Converts a log (events) file as exported by the ethereum etl component to
    a format that can be easily ingested in the log table using dsbulk.
    See schema/resources for the exact schema of the log table.
    \f
    Args:
        file (str): file to convert
        prefix (str): prefix for teh newly created file.
    """
    convert_etl_to_ingestable_logs(inputfile, prefix)
