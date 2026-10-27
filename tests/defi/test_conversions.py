from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

import graphsenselib.defi.conversions as conversions_mod
from graphsenselib.defi.bridging.delegated import UnresolvedBridge
from graphsenselib.defi.models import Trace

from .test_delegated_bridges import FIXTURE, db_rows, without_cctp_events


class TestGetConversionsFromDbGather:
    """get_conversions_from_db fetches the tx's logs and traces via two
    independent block-partition scans. They used to run one after another;
    the perf fix runs them concurrently with asyncio.gather. This must not
    swap which result feeds the log decoder vs. the trace normalizer - a
    classic bug in gather-based rewrites.
    """

    async def test_logs_and_traces_are_fetched_and_routed_without_swapping(self):
        tx = {"tx_hash": b"\x01\x02", "block_id": 5}
        logs_sentinel = [{"marker": "logs"}]
        traces_raw_sentinel = [{"marker": "traces-raw"}]
        normalized_traces_sentinel = ["NORMALIZED_TRACES"]

        db = MagicMock()
        db.fetch_transaction_logs = AsyncMock(return_value=logs_sentinel)
        db.fetch_transaction_traces = AsyncMock(return_value=traces_raw_sentinel)

        received = {}

        def fake_normalize(network, trace_dicts, tx_):
            received["normalize_input"] = trace_dicts
            return normalized_traces_sentinel

        def fake_decode(logs):
            received["decode_input"] = logs
            return [({"name": "Transfer"}, {"marker": "logs", "log_index": 0})]

        async def fake_bridges(
            network,
            db_,
            tx_,
            decoded_log_data,
            logs_raw_filtered,
            traces,
            included_bridges,
        ):
            received["bridge_traces"] = traces
            return []

        def fake_swaps(
            decoded_log_data, logs_raw_filtered, traces, visualize, **kwargs
        ):
            received["swap_traces"] = traces
            received["swap_kwargs"] = kwargs
            return []

        with (
            patch.object(conversions_mod, "decode_logs_dict", side_effect=fake_decode),
            patch.object(Trace, "dicts_to_normalized", side_effect=fake_normalize),
            patch.object(
                conversions_mod,
                "get_bridges_from_decoded_logs",
                new=AsyncMock(side_effect=fake_bridges),
            ),
            patch(
                "graphsenselib.defi.swaps.get_swap_from_decoded_logs",
                side_effect=fake_swaps,
            ),
        ):
            result = await conversions_mod.get_conversions_from_db("eth", db, tx)

        db.fetch_transaction_logs.assert_awaited_once_with("eth", tx)
        db.fetch_transaction_traces.assert_awaited_once_with("eth", tx)

        # the raw traces scan result must feed the trace normalizer ...
        assert received["normalize_input"] is traces_raw_sentinel
        # ... and the raw logs scan result must feed the log decoder - not swapped
        assert received["decode_input"] is logs_sentinel

        # downstream bridge/swap extraction must see the *normalized* traces
        assert received["bridge_traces"] == normalized_traces_sentinel
        assert received["swap_traces"] == normalized_traces_sentinel
        # CoW Protocol settlements are detected unless turned off
        assert received["swap_kwargs"]["cow_protocol"] is True

        assert result == conversions_mod.DetectedConversions()

    async def test_no_logs_short_circuits_without_decoding(self):
        tx = {"tx_hash": b"\x01\x02", "block_id": 5}
        db = MagicMock()
        db.fetch_transaction_logs = AsyncMock(return_value=[])
        db.fetch_transaction_traces = AsyncMock(return_value=[])

        with patch.object(conversions_mod, "decode_logs_dict") as mock_decode:
            result = await conversions_mod.get_conversions_from_db("eth", db, tx)

        db.fetch_transaction_logs.assert_awaited_once_with("eth", tx)
        db.fetch_transaction_traces.assert_awaited_once_with("eth", tx)
        mock_decode.assert_not_called()
        assert result == conversions_mod.DetectedConversions()


class TestUnresolvedBridges:
    """A tx's bridge events graphsense-lib cannot pair (CCTP) come back with
    its conversions. They are found in the logs already loaded, so detecting
    them adds no read, and included_bridges does not decide whether they are
    reported: whoever acts on them is configured elsewhere."""

    CCTP_BURN = UnresolvedBridge("cctp", "eth", FIXTURE["v2_burn"]["tx_hash"], 698)

    @staticmethod
    def fake_db(rows):
        db = MagicMock()
        db.fetch_transaction_logs = AsyncMock(return_value=rows)
        db.fetch_transaction_traces = AsyncMock(return_value=[])
        return db

    @staticmethod
    def tx_of(part):
        return {
            "tx_hash": bytes.fromhex(FIXTURE[part]["tx_hash"][2:]),
            "block_id": FIXTURE[part]["block_id"],
            "input": None,
        }

    @pytest.mark.parametrize("included_bridges", [(), ("thorchain",), ("cctp",)])
    async def test_a_cctp_tx_reports_its_unresolved_bridge(self, included_bridges):
        db = self.fake_db(db_rows("v2_burn"))
        result = await conversions_mod.get_conversions_from_db(
            "eth", db, self.tx_of("v2_burn"), included_bridges=included_bridges
        )
        assert result.unresolved_bridges == (self.CCTP_BURN,)

    @pytest.mark.parametrize(
        "rows",
        [db_rows("v2_burn"), db_rows("v2_burn", without_cctp_events("v2_burn"))],
        ids=["cctp", "other"],
    )
    async def test_detection_reads_nothing_beyond_the_logs_and_traces(self, rows):
        db = self.fake_db(rows)
        tx = self.tx_of("v2_burn")
        await conversions_mod.get_conversions_from_db("eth", db, tx)
        assert db.mock_calls == [
            call.fetch_transaction_logs("eth", tx),
            call.fetch_transaction_traces("eth", tx),
        ]

    async def test_any_other_tx_reports_none(self):
        rows = db_rows("v2_burn", without_cctp_events("v2_burn"))
        result = await conversions_mod.get_conversions_from_db(
            "eth", self.fake_db(rows), self.tx_of("v2_burn")
        )
        assert result.unresolved_bridges == ()

    async def test_logs_that_decode_to_nothing_still_report_it(self):
        with patch.object(conversions_mod, "decode_logs_dict", return_value=[]):
            result = await conversions_mod.get_conversions_from_db(
                "eth", self.fake_db(db_rows("v2_burn")), self.tx_of("v2_burn")
            )
        assert result == conversions_mod.DetectedConversions(
            conversions=(), unresolved_bridges=(self.CCTP_BURN,)
        )
