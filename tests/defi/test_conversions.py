from unittest.mock import AsyncMock, MagicMock, patch

import graphsenselib.defi.conversions as conversions_mod
from graphsenselib.defi.models import Trace


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

        def fake_swaps(decoded_log_data, logs_raw_filtered, traces, visualize):
            received["swap_traces"] = traces
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

        assert result == []

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
        assert result == []
