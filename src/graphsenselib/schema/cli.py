"""Summary"""

import sys

import click

from ..cli.common import require_currency, require_environment, require_schema_type
from ..config import keyspace_types
from .schema import GraphsenseSchemas


def print_schema_validation_report(env, currency):
    report = GraphsenseSchemas().get_db_validation_report(env, currency)
    if any(report):
        for row in report:
            click.secho(f"Error: {row}", fg="red")
        sys.exit(101)
    else:
        click.echo(
            f"Db schema matches the expectation on {env} for currency {currency}."
        )


def keyspace_types_option(function):
    function = click.option(
        "--keyspace-type",
        type=click.Choice(keyspace_types, case_sensitive=False),
        help="Environment to work on",
        required=False,
    )(function)
    return function


@click.group()
def schema_cli():
    """ """
    pass


@schema_cli.group()
def schema():
    """Creating and validating the db schema."""
    pass


@schema.command(
    "show-by-currency",
    short_help="Prints the current db schema expected from graphsenselib",
)
@require_currency()
@keyspace_types_option
def showc(currency, keyspace_type):
    """Prints the current db schema expected from graphsenselib
    \f
    Args:
        currency (str): currency to work on
        keyspace_type (str): type of the keyspace
    """
    schemas = GraphsenseSchemas().get_by_currency(currency, keyspace_type=keyspace_type)
    for file, schema in schemas:
        click.echo(f"// ######### {file}")
        click.echo(schema.original_schema)


@schema.command(
    "show-by-schema-type",
    short_help="Prints the current db schema expected from graphsenselib",
)
@require_schema_type()
@keyspace_types_option
def shows(schema, keyspace_type):
    """Prints the current db schema expected from graphsenselib
    \f
    Args:
        currency (str): currency to work on
        keyspace_type (str): type of the keyspace
    """
    schemas = GraphsenseSchemas().get_by_schema_type(
        schema, keyspace_type=keyspace_type
    )
    for file, schema in schemas:
        click.echo(f"// ######### {file}")
        click.echo(schema.original_schema)


@schema.command(
    "create", short_help="Creates the necessary graphsense tables in Cassandra."
)
@require_environment()
@require_currency()
@keyspace_types_option
def create(env, currency, keyspace_type):
    """Summary
        Creates the necessary graphsense tables in Cassandra if they don't exist.
        \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    if keyspace_type is None:
        GraphsenseSchemas().create_keyspaces_if_not_exist(env, currency)
    else:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env, currency, keyspace_type=keyspace_type
        )

    click.echo("// ######### Validating deployed schema")
    print_schema_validation_report(env, currency)


@schema.command(
    "create-new-transformed", short_help="Creates new/empty transformed keyspace."
)
@require_environment()
@require_currency()
@click.option(
    "--suffix",
    type=str,
    required=False,
    help="suffix to append to default "
    "[currency]_transformed_[date]_[suffix] keyspace name.",
)
@click.option(
    "--no-date",
    is_flag=True,
    help="omits the date in the keyspace name",
)
def create_new_tf(env, currency, suffix, no_date):
    """Summary
        Creates new/empty transformed keyspace.
        \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
        suffix (str): suffix for the new keyspace
    """

    # flake8: noqa: T201
    print(
        GraphsenseSchemas().create_new_transformed_ks_if_not_exist(
            env, currency, suffix, no_date
        )
    )


@schema.command(
    "validate", short_help="Validates if the expected schema matches the database."
)
@require_environment()
@require_currency()
def validate(env, currency):
    """Summary
        Validates if the expected schema matches the database.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    print_schema_validation_report(env, currency)
