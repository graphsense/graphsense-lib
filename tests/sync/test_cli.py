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
      networks:
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
    assert load().sync.networks["zec"].channel == "canary"
    cfg.write_text(CONFIG.replace("CHANNEL", "stable"))
    future = time.time() + 5
    os.utime(cfg, (future, future))
    assert load().sync.networks["zec"].channel == "stable"


def test_pause_status_resume_without_redis(tmp_path):
    cfg = tmp_path / "gs.yaml"
    cfg.write_text(CONFIG.replace("CHANNEL", "stable"))
    base = ["--config-file", str(cfg), "sync"]
    with patch("graphsenselib.sync.cli.FILE_STATE_DIR", str(tmp_path / "state")):
        r = CliRunner().invoke(
            cli, base + ["pause", "-e", "dev", "-c", "zec", "--reason", "resync"]
        )
        assert r.exit_code == 0, r.output
        from graphsenselib.sync.state import FileStateStore

        store = FileStateStore(str(tmp_path / "state" / "dev"))
        assert store.get_pause("zec")["reason"] == "resync"
        r = CliRunner().invoke(cli, base + ["status", "-e", "dev"])
        assert r.exit_code == 0, r.output
        r = CliRunner().invoke(cli, base + ["resume", "-e", "dev", "-c", "zec"])
        assert "Resumed" in r.output
    assert (tmp_path / "state" / "dev").is_dir()


class _FakeProbe:
    header = None
    error = None

    def __init__(self, env, network, ks):
        pass

    def wal_pending(self):
        if self.error is not None:
            raise self.error
        return self.header

    def close(self):
        pass


def test_wal_state(monkeypatch):
    from graphsenselib.sync import cli as sync_cli

    monkeypatch.setattr("graphsenselib.sync.probe.ChainProbe", _FakeProbe)
    assert sync_cli._wal_state("dev", "zec", object()) == "empty"
    _FakeProbe.header = {"block_lo": 5, "block_hi": 9, "code_version": "2.18.0"}
    assert sync_cli._wal_state("dev", "zec", object()) == "pending 5-9 (2.18.0)"
    _FakeProbe.header, _FakeProbe.error = None, RuntimeError("no cassandra")
    assert sync_cli._wal_state("dev", "zec", object()) == "?"
    _FakeProbe.error = None
    assert sync_cli._wal_state("dev", "zec", None) == ""


def test_pause_wait_until_cycle_finished(tmp_path, monkeypatch):
    import threading

    from graphsenselib.sync.state import FileStateStore

    cfg = tmp_path / "gs.yaml"
    cfg.write_text(CONFIG.replace("CHANNEL", "stable"))
    monkeypatch.setattr("graphsenselib.sync.cli.FILE_STATE_DIR", str(tmp_path / "st"))
    monkeypatch.setattr("graphsenselib.sync.cli._WAIT_POLL_SECONDS", 0.05)
    monkeypatch.setattr(
        "graphsenselib.sync.cli._wal_state", lambda env, network, ks: "empty"
    )
    store = FileStateStore(str(tmp_path / "st" / "dev"))
    store.announce_daemon("d1", {"channel": "stable", "running": ["zec"]})
    # the daemon's cycle ends shortly after the pause
    threading.Timer(
        0.3, lambda: store.announce_daemon("d1", {"channel": "stable", "running": []})
    ).start()

    r = CliRunner().invoke(
        cli,
        ["--config-file", str(cfg), "sync", "pause", "-e", "dev", "-c", "zec"]
        + ["--reason", "image switch", "--wait"],
    )
    assert r.exit_code == 0, r.output
    assert "Waiting for the running zec cycle" in r.output
    assert "No zec job running" in r.output
    assert "empty" in r.output  # status table with the WAL column
    assert store.get_pause("zec")["reason"] == "image switch"


def test_pause_wait_stops_when_pause_is_lifted(tmp_path, monkeypatch):
    import threading

    from graphsenselib.sync.state import FileStateStore

    cfg = tmp_path / "gs.yaml"
    cfg.write_text(CONFIG.replace("CHANNEL", "stable"))
    monkeypatch.setattr("graphsenselib.sync.cli.FILE_STATE_DIR", str(tmp_path / "st"))
    monkeypatch.setattr("graphsenselib.sync.cli._WAIT_POLL_SECONDS", 0.05)
    store = FileStateStore(str(tmp_path / "st" / "dev"))
    store.announce_daemon("d1", {"channel": "stable", "running": ["zec"]})
    threading.Timer(0.3, lambda: store.resume("zec")).start()

    r = CliRunner().invoke(
        cli,
        ["--config-file", str(cfg), "sync", "pause", "-e", "dev", "-c", "zec"]
        + ["--reason", "x", "--wait"],
    )
    assert r.exit_code == 0, r.output
    assert "no longer paused" in r.output


def test_fmt_build():
    from graphsenselib.sync.cli import _fmt_build

    assert _fmt_build("2.19.0.dev52+g1309bed27") == "dev build 1309bed (2.19.0.dev52)"
    assert _fmt_build("2.19.0.dev3") == "dev build (2.19.0.dev3)"
    assert _fmt_build("2.18.1") == "release 2.18.1"
    assert _fmt_build("2.18.1rc1") == "release 2.18.1rc1"
    assert _fmt_build("") == "?"


def test_status_blocks(tmp_path, monkeypatch):
    from graphsenselib.sync.state import FileStateStore

    cfg = tmp_path / "gs.yaml"
    cfg.write_text(CONFIG.replace("{channel: CHANNEL}", "{}"))
    monkeypatch.setattr("graphsenselib.sync.cli.FILE_STATE_DIR", str(tmp_path / "st"))
    monkeypatch.setattr(
        "graphsenselib.sync.cli._wal_state", lambda env, network, ks: "empty"
    )
    store = FileStateStore(str(tmp_path / "st" / "dev"))
    now = time.time()
    store.announce_daemon(
        "d1",
        {
            "channel": "default",
            "host": "h1",
            "version": "2.19.0.dev52+g1309bed27",
            "started_at": now - 1500,
            "running": [],
            "networks": ["zec"],
        },
    )
    store.claim("zec", "d1")
    store.write_status(
        "zec",
        {
            "node_tip": 1100,
            "target": 1000,
            "raw": 990,
            "transformed": 980,
            "last_result": "ok",
            "last_success_at": now - 300,
            "next_cycle_at": now + 360,
        },
    )
    r = CliRunner().invoke(
        cli, ["--config-file", str(cfg), "sync", "status", "-e", "dev"]
    )
    assert r.exit_code == 0, r.output
    out = r.output
    assert "zec · daemon d1 on h1" in out
    assert "dev build 1309bed (2.19.0.dev52), up 25m" in out
    assert "zec · idle · next cycle in 5m" in out
    assert "channel" not in out  # default channel is not shown
    assert "1,100  (target 1,000, backoff 100)" in out
    assert "990  10 behind target" in out
    assert "980  10 behind raw" in out
    assert "WAL          empty" in out
    assert "last         ok, " in out
