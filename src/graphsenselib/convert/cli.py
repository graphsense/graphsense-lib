import click

from .accountsetl import convert_etl_to_ingestable_logs
from .address_scan.cli import scan_for_addresses_cmd
from .gs_files.cli import gs_files_cli


@click.group()
def convert_cli():
    pass


@convert_cli.group()
def convert():
    """Useful file convertions tools for the graphsense infrastructure."""
    pass


convert.add_command(gs_files_cli)
convert.add_command(scan_for_addresses_cmd)

# `file` is an alias for the `convert` group so the file tools can also be
# reached as `graphsense-cli file ...` (the commands here operate on files).
convert_cli.add_command(convert, name="file")


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
