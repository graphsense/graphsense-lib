# ruff: noqa: T201
from typing import Optional

import click

from graphsenselib.tagpack import __version__

from graphsenselib.tagstore.config import TagstoreSettings
from graphsenselib.tagstore.db.database import (
    get_db_engine,
    get_table_ddl_sql,
    get_views_ddl_sql,
    init_database,
)


@click.group()
def tagstore_cli():
    pass


@tagstore_cli.group("tagstore")
def tagstore():
    """Tagstore CLI - GraphSense tag store management tool."""
    pass


@tagstore.command()
def version():
    """Print version information."""
    print(__version__)


@tagstore.command()
@click.option(
    "--db-url", type=str, help="Database URL to use instead of configured URL"
)
def init(db_url: Optional[str] = None):
    """Initialize the database with tables and views."""
    db_url_settings = TagstoreSettings().db_url
    init_database(get_db_engine(db_url or db_url_settings))


@tagstore.command("get-create-sql")
def get_ddl():
    """Print DDL SQL for creating tables and views."""
    print(get_table_ddl_sql())
    print(get_views_ddl_sql())
