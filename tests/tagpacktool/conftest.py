from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer

from tagpack.cli import exec_cli_command
from tagstore.db.database import get_db_engine, init_database

DATA_DIR_TP = Path(__file__).parent.resolve() / "testfiles" / "simple"
DATA_DIR_A = Path(__file__).parent.resolve() / "testfiles" / "actors"

postgres = PostgresContainer("postgres:16-alpine")


def insert_test_data(db_setup):
    db_url = db_setup["db_connection_string"]
    engine = get_db_engine(db_url)

    init_database(engine)

    exec_cli_command(
        ["actorpack", "insert", str(DATA_DIR_A), "-u", db_url, "--no_strict_check"]
    )

    tps = [
        (True, "config.yaml"),
        (False, "duplicate_tag.yaml"),
        (False, "empty_tag_list.yaml"),
        (True, "ex_addr_tagpack.yaml"),
        (True, "multiple_tags_for_address.yaml"),
        (True, "with_concepts.yaml"),
    ]
    for public, tpf in tps:
        exec_cli_command(
            [
                "tagpack",
                "insert",
                str(DATA_DIR_TP / tpf),
                "-u",
                db_url,
                "--no_strict_check",
                "--no_git",
            ]
            + (["--public"] if public else [])
        )

    exec_cli_command(["tagstore", "refresh_views", "-u", db_url])


@pytest.fixture(scope="session", autouse=True)
def db_setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)

    postgres_sync_url = postgres.get_connection_url()
    portgres_async_url = postgres_sync_url.replace("psycopg2", "asyncpg")

    setup = {
        "db_connection_string": postgres_sync_url.replace("+psycopg2", ""),
        "db_connection_string_psycopg2": postgres_sync_url,
        "db_connection_string_async": portgres_async_url,
    }

    insert_test_data(setup)

    return setup
