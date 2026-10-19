import os
import time
from unittest.mock import patch

from click.testing import CliRunner

from graphsenselib.cli.main import cli

CONFIG = """
environments:
  dev:
    cassandra_nodes: ["127.0.0.1:1"]
    keyspaces:
      zec:
        raw_keyspace_name: zec_raw
        transformed_keyspace_name: zec_transformed
        schema_type: utxo
        ingest_config: {node_reference: "http://127.0.0.1:1"}
    sync:
      currencies:
        zec: {channel: CHANNEL}
"""


def test_run_passes_config_file_to_jobs_and_reloads(tmp_path):
    cfg = tmp_path / "gs.yaml"
    cfg.write_text(CONFIG.replace("CHANNEL", "canary"))
    seen = {}

    class FakeScheduler:
        def __init__(self, **kw):
            seen.update(kw)

        def run(self, once):
            return 0

    with patch("graphsenselib.sync.scheduler.SyncScheduler", FakeScheduler):
        result = CliRunner().invoke(
            cli,
            ["-vv", "--config-file", str(cfg), "sync", "run", "-e", "dev", "--once"],
        )
    assert result.exit_code == 0, result.output
    assert seen["runner"].global_args == ["-vv", "--config-file", str(cfg)]

    load = seen["load_environment"]
    assert load().sync.currencies["zec"].channel == "canary"
    cfg.write_text(CONFIG.replace("CHANNEL", "stable"))
    future = time.time() + 5
    os.utime(cfg, (future, future))
    assert load().sync.currencies["zec"].channel == "stable"
