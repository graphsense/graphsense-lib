from pathlib import Path

from graphsenselib.tagpack.cli import cli
from graphsenselib.tagstore.db.database import (
    get_db_engine,
    init_database,
    with_session,
)
from sqlmodel import text

DATA_DIR_TP = Path(__file__).parent.resolve() / "data" / "packs"
DATA_DIR_A = Path(__file__).parent.resolve() / "data" / "actors"
DATA_SQL = Path(__file__).parent.resolve() / "data" / "data.sql"


def exec_cli_command(args):
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(cli, args, catch_exceptions=False)
    if result.exception:
        raise result.exception
    return result


def load_test_data(db_url: str):
    engine = get_db_engine(db_url)

    init_database(engine)

    exec_cli_command(
        [
            "actorpack",
            "insert",
            str(DATA_DIR_A),
            "-u",
            db_url,
            "--no-strict-check",
            "--no-git",
        ]
    )

    tps = [
        (True, "tagpack_public.yaml"),
        (False, "tagpack_private.yaml"),
        (False, "tagpack_uriX.yaml"),
        (True, "tagpack_uriY.yaml"),
    ]
    for public, tpf in tps:
        exec_cli_command(
            [
                "tagpack",
                "insert",
                str(DATA_DIR_TP / tpf),
                "-u",
                db_url,
                "--no-strict-check",
                "--no-git",
            ]
            + (["--public"] if public else [])
        )

    with with_session(engine) as session:
        with open(DATA_SQL) as f:
            session.exec(text(f.read()))
            session.commit()

    exec_cli_command(["tagstore", "refresh-views", "-u", db_url])
