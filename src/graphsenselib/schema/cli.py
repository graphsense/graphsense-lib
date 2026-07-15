"""Summary"""

import sys

import click

from ..cli.common import require_currency, require_environment, require_schema_type
from ..config import keyspace_types
from .schema import GraphsenseSchemas


def print_schema_validation_report(env, currency, schema_type=None):
    report = GraphsenseSchemas().get_db_validation_report(
        env, currency, schema_type=schema_type
    )
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
@click.option(
    "--keyspace-name",
    type=str,
    required=False,
    help="Create under this keyspace name instead of the configured one "
    "(e.g. a new keyspace for a rebuild). Requires --keyspace-type.",
)
@click.option(
    "--replication-config",
    type=str,
    required=False,
    help="Replication config for the new keyspace, overriding the configured "
    "one, e.g. \"{'class': 'NetworkTopologyStrategy', 'DC1': '2'}\".",
)
def create(env, currency, keyspace_type, keyspace_name, replication_config):
    """Summary
        Creates the necessary graphsense tables in Cassandra if they don't exist.
        \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    if keyspace_name is not None and keyspace_type is None:
        raise click.UsageError("--keyspace-name requires --keyspace-type.")
    if keyspace_type is None:
        GraphsenseSchemas().create_keyspaces_if_not_exist(env, currency)
    else:
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env,
            currency,
            keyspace_type=keyspace_type,
            keyspace_name_override=keyspace_name,
            replication_config_override=replication_config,
        )

    if keyspace_name is not None:
        # the report validates the YAML-configured keyspaces, not the override
        click.echo(
            f"Created {keyspace_name}; skipping schema validation report "
            "(it only covers the configured keyspaces)."
        )
        return
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
@click.option(
    "--schema-type",
    type=str,
    required=False,
    help="what type of keyspace to validate",
)
def validate(env, currency, schema_type):
    """Summary
        Validates if the expected schema matches the database.
    \f
    Args:
        env (str): Environment to work on
        currency (str): currency to work on
    """
    print_schema_validation_report(env, currency, schema_type=schema_type)
