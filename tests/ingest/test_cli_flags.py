"""Tests for CLI flag forwarding and crash-safety in the new IngestRunner pipeline."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from graphsenselib.ingest.common import BlockRangeContent, Source
from graphsenselib.ingest.dump import PARTITIONSIZES, export_delta
from graphsenselib.ingest.ingestrunner import IngestRunner


class FakeSource(Source):
    def __init__(self, max_block=200, last_block_yesterday=150):
        self._max_block = max_block
        self._last_block_yesterday = last_block_yesterday

    def read_blockrange(self, start_block, end_block):
        blocks = [
            {"block_id": i, "timestamp": 1_700_000_000 + i * 12}
            for i in range(start_block, end_block + 1)
        ]
        return BlockRangeContent(
            table_contents={"block": blocks},
            start_block=start_block,
            end_block=end_block,
        )

    def read_blockindep(self):
        return BlockRangeContent(table_contents={})

    def get_last_synced_block(self):
        return self._max_block

    def get_last_block_yesterday(self):
        return self._last_block_yesterday


def test_run_no_crash_on_immediate_shutdown():
    """run() must not raise UnboundLocalError when no blocks are processed."""
    runner = IngestRunner(partition_batch_size=1000, file_batch_size=100)
    runner.addSource(FakeSource())
    runner.addTransformer(MagicMock(network="eth"))
    runner.addSink(MagicMock())

    # start > end → zero partitions → loop body never executes.
    # Before the fix this raised UnboundLocalError on the final log line.
    assert runner.run(10, 5) is None


def test_summary_stats_skipped_when_no_blocks():
    """Summary stats must not be written when runner.run returns None."""
    with (
        patch("graphsenselib.ingest.dump.SourceUTXO") as MockSource,
        patch("graphsenselib.ingest.dump.TransformerUTXO"),
        patch("graphsenselib.ingest.dump.IngestRunner") as MockRunner,
        patch("graphsenselib.ingest.dump.create_lock"),
        patch("graphsenselib.ingest.dump.get_reorg_backoff_blocks", return_value=0),
        patch(
            "graphsenselib.ingest.dump.ingest_summary_statistics_cassandra_utxo"
        ) as mock_stats,
        patch("graphsenselib.ingest.dump.ingest_configuration_cassandra_utxo"),
    ):
        MockSource.return_value = FakeSource(max_block=200)
        runner = MagicMock(sinks=[], run=MagicMock(return_value=None))
        MockRunner.return_value = runner

        export_delta(
            currency="btc",
            sources=["http://localhost:8332"],
            directory=None,
            start_block=0,
            end_block=100,
            provider_timeout=3600,
            db=MagicMock(),
        )
        mock_stats.assert_not_called()


def test_summary_stats_uses_actual_last_block():
    """Summary stats must use actual last block processed, not the requested end."""
    mock_db = MagicMock()
    with (
        patch("graphsenselib.ingest.dump.SourceUTXO") as MockSource,
        patch("graphsenselib.ingest.dump.TransformerUTXO") as MockTransformer,
        patch("graphsenselib.ingest.dump.IngestRunner") as MockRunner,
        patch("graphsenselib.ingest.dump.create_lock"),
        patch("graphsenselib.ingest.dump.get_reorg_backoff_blocks", return_value=0),
        patch(
            "graphsenselib.ingest.dump.ingest_summary_statistics_cassandra_utxo"
        ) as mock_stats,
        patch("graphsenselib.ingest.dump.ingest_configuration_cassandra_utxo"),
    ):
        MockSource.return_value = FakeSource(max_block=200)
        MockTransformer.return_value = MagicMock(
            _last_block_ts=1_700_000_060, _next_tx_id=42
        )
        MockRunner.return_value = MagicMock(sinks=[], run=MagicMock(return_value=5))

        export_delta(
            currency="btc",
            sources=["http://localhost:8332"],
            directory=None,
            start_block=0,
            end_block=100,
            provider_timeout=3600,
            db=mock_db,
        )
        mock_stats.assert_called_once_with(
            mock_db,
            timestamp=1_700_000_060,
            total_blocks=6,
            total_txs=42,
        )


def test_previous_day_limits_end_block():
    """--previous-day must cap end_block to source.get_last_block_yesterday()."""
    with (
        patch("graphsenselib.ingest.dump.SourceETH") as MockSource,
        patch("graphsenselib.ingest.dump.IngestRunner") as MockRunner,
        patch("graphsenselib.ingest.dump.create_lock"),
        patch("graphsenselib.ingest.dump.get_reorg_backoff_blocks", return_value=0),
    ):
        MockSource.return_value = FakeSource(max_block=200, last_block_yesterday=150)
        runner = MagicMock(sinks=[], run=MagicMock(return_value=150))
        MockRunner.return_value = runner

        export_delta(
            currency="eth",
            sources=["http://localhost:8545"],
            directory=None,
            start_block=0,
            end_block=None,
            provider_timeout=3600,
            previous_day=True,
        )
        runner.run.assert_called_once()
        assert runner.run.call_args[0][1] <= 150


def test_info_flag_returns_without_running():
    """--info must return without calling runner.run()."""
    with (
        patch("graphsenselib.ingest.dump.SourceETH") as MockSource,
        patch("graphsenselib.ingest.dump.IngestRunner") as MockRunner,
        patch("graphsenselib.ingest.dump.create_lock"),
        patch("graphsenselib.ingest.dump.get_reorg_backoff_blocks", return_value=0),
    ):
        MockSource.return_value = FakeSource(max_block=200)
        runner = MagicMock(sinks=[])
        MockRunner.return_value = runner

        export_delta(
            currency="eth",
            sources=["http://localhost:8545"],
            directory=None,
            start_block=0,
            end_block=100,
            provider_timeout=3600,
            info=True,
        )
        runner.run.assert_not_called()


def test_batch_size_override():
    """file_batch_size param must override the default FILESIZES[currency]."""
    with (
        patch("graphsenselib.ingest.dump.SourceETH") as MockSource,
        patch("graphsenselib.ingest.dump.IngestRunner") as MockRunner,
        patch("graphsenselib.ingest.dump.create_lock"),
        patch("graphsenselib.ingest.dump.get_reorg_backoff_blocks", return_value=0),
    ):
        MockSource.return_value = FakeSource(max_block=200)
        MockRunner.return_value = MagicMock(sinks=[], run=MagicMock(return_value=100))

        export_delta(
            currency="eth",
            sources=["http://localhost:8545"],
            directory=None,
            start_block=0,
            end_block=100,
            provider_timeout=3600,
            file_batch_size=500,
        )
        MockRunner.assert_called_once_with(PARTITIONSIZES["eth"], 500)


def test_unsupported_mode_rejected():
    """Legacy-only modes must be rejected with exit code 11 in the new pipeline."""
    from graphsenselib.ingest.cli import ingest

    mock_config = MagicMock()
    mock_ks_config = MagicMock()
    mock_ks_config.ingest_config.legacy_ingest = False
    mock_config.get_keyspace_config.return_value = mock_ks_config

    with (
        patch("graphsenselib.ingest.cli.get_config", return_value=mock_config),
        patch("graphsenselib.ingest.cli.GraphsenseSchemas"),
        patch("graphsenselib.ingest.cli.DbFactory"),
    ):
        result = CliRunner().invoke(
            ingest,
            ["--env", "test", "--currency", "btc", "--mode", "utxo_only_tx_graph"],
        )
        assert result.exit_code == 11


def test_sigint_after_first_partition_returns_partial():
    """Simulated SIGINT after the first partition must return partial progress.

    The shutdown check runs after each partition. Returning True on the first
    check means partition 0-99 has been fully processed and written, then the
    loop breaks before partition 100-199 starts.
    """

    @contextmanager
    def fake_graceful_shutdown():
        yield lambda: True  # signal shutdown immediately after first partition

    sink = MagicMock()
    sink.lock_name.return_value = None

    runner = IngestRunner(partition_batch_size=100, file_batch_size=100)
    runner.addSource(FakeSource(max_block=300))
    runner.addTransformer(MagicMock(network="eth", transform=lambda d: d))
    runner.addSink(sink)

    with patch(
        "graphsenselib.ingest.ingestrunner.graceful_ctlc_shutdown",
        fake_graceful_shutdown,
    ):
        result = runner.run(0, 299)

    # Only the first partition (0-99) should have been processed
    assert result == 99

    # Sink received writes for the first partition only
    written_blocks = set()
    for call in sink.write.call_args_list:
        content = call[0][0]
        for b in content.table_contents["block"]:
            written_blocks.add(b["block_id"])

    assert written_blocks == set(range(100))
    assert 100 not in written_blocks
