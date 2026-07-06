import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.cassandra import (
    Cassandra,
    check_height_bounds_impossible,
)


class TestCheckHeightBoundsImpossible:
    """Tests for check_height_bounds_impossible function."""

    def test_min_height_exceeds_last_block(self):
        """When min_height > last_height, should return True."""
        assert check_height_bounds_impossible(5000, None, 999) is True

    def test_max_height_negative(self):
        """When max_height < 0, should return True."""
        assert check_height_bounds_impossible(None, -1, 999) is True

    def test_both_impossible(self):
        """When both conditions are impossible, should return True."""
        assert check_height_bounds_impossible(5000, -1, 999) is True

    def test_valid_range(self):
        """When range is valid, should return False."""
        assert check_height_bounds_impossible(100, 500, 999) is False

    def test_min_height_equals_last_block(self):
        """When min_height == last_height, should return False (edge case)."""
        assert check_height_bounds_impossible(999, None, 999) is False

    def test_max_height_zero(self):
        """When max_height == 0, should return False (genesis block is valid)."""
        assert check_height_bounds_impossible(None, 0, 999) is False

    def test_no_constraints(self):
        """When no constraints provided, should return False."""
        assert check_height_bounds_impossible(None, None, 999) is False


# ---------------------------------------------------------------------------
# execute_async_core: cancellation race regression
#
# A client disconnect (or a cancelled gather sibling / wait_for timeout) can
# cancel the Future between the driver callback's cancelled() check and the
# delivery on the loop thread. Delivering onto an already-cancelled Future
# raises InvalidStateError. The guard must keep that path silent while normal
# delivery still works.
# ---------------------------------------------------------------------------


class _FakeResponseFuture:
    """Captures the driver callbacks so the test can fire them by hand."""

    def __init__(self):
        self._paging_state = None
        self.on_done = None
        self.on_err = None

    def add_callbacks(self, on_done, on_err):
        self.on_done = on_done
        self.on_err = on_err


class _FakeSession:
    def __init__(self, response_future):
        self._rf = response_future

    def execute_async(self, *args, **kwargs):
        return self._rf


class _FakeCassandra:
    """Minimal stand-in providing only what execute_async_core touches."""

    def __init__(self, response_future):
        self.session = _FakeSession(response_future)
        self.prepared_statements = {}

    def get_prepared_statement(self, query):
        return SimpleNamespace(fetch_size=None)


def _capture_loop_errors():
    loop = asyncio.get_running_loop()
    errors = []
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))
    return errors


async def test_result_delivery_after_cancellation_is_silent():
    rf = _FakeResponseFuture()
    errors = _capture_loop_errors()

    fut = Cassandra.execute_async_core(_FakeCassandra(rf), "SELECT 1")

    # Driver callback fires while the future is still live: it passes the
    # cancelled() check and schedules delivery on the loop thread.
    rf.on_done([{"x": 1}])
    # The request is cancelled (e.g. client disconnect) before delivery runs.
    fut.cancel()
    # Run the scheduled delivery; the guard must skip it without raising.
    await asyncio.sleep(0)

    assert fut.cancelled()
    assert errors == [], f"delivery onto cancelled future raised: {errors}"


async def test_exception_delivery_after_cancellation_is_silent():
    rf = _FakeResponseFuture()
    errors = _capture_loop_errors()

    fut = Cassandra.execute_async_core(_FakeCassandra(rf), "SELECT 1")

    rf.on_err(RuntimeError("driver boom"))
    fut.cancel()
    await asyncio.sleep(0)

    assert fut.cancelled()
    assert errors == [], f"exception delivery onto cancelled future raised: {errors}"


async def test_normal_result_delivery_still_works():
    rf = _FakeResponseFuture()

    fut = Cassandra.execute_async_core(_FakeCassandra(rf), "SELECT 1")

    rows = [{"x": 1}, {"x": 2}]
    rf.on_done(rows)
    result = await fut

    assert result.current_rows == rows


async def test_normal_exception_delivery_still_propagates():
    rf = _FakeResponseFuture()

    fut = Cassandra.execute_async_core(_FakeCassandra(rf), "SELECT 1")

    rf.on_err(RuntimeError("driver boom"))
    with pytest.raises(RuntimeError, match="driver boom"):
        await fut


# ---------------------------------------------------------------------------
# GraphsenseFallbackToLocalOneRetryPolicy
# ---------------------------------------------------------------------------
#
# The policy's job is one specific thing: on the FIRST attempt of a read at
# LOCAL_QUORUM, if at least one replica is alive/responding, downgrade to
# LOCAL_ONE. Everything else (further retries, other consistency levels,
# unrelated error types, writes) must delegate to the parent's behavior
# unchanged. The tests below cover both the downgrade branches and a
# representative pass-through.

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra.policies import RetryPolicy  # noqa: E402

from graphsenselib.db.asynchronous.cassandra import (  # noqa: E402
    GraphsenseFallbackToLocalOneRetryPolicy,
)


def _policy() -> GraphsenseFallbackToLocalOneRetryPolicy:
    # max_retries=3 mirrors the production wire-up in Cassandra.connect().
    return GraphsenseFallbackToLocalOneRetryPolicy(max_retries=3)


class TestFallbackToLocalOneOnUnavailable:
    def test_first_attempt_at_local_quorum_with_alive_replica_downgrades(self):
        decision, new_cl = _policy().on_unavailable(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_replicas=2,
            alive_replicas=1,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETRY
        assert new_cl == ConsistencyLevel.LOCAL_ONE

    def test_first_attempt_at_local_quorum_with_no_alive_replicas_does_not_downgrade(
        self,
    ):
        # Downgrading to LOCAL_ONE would still fail, so don't paper over —
        # delegate to the parent policy.
        decision, new_cl = _policy().on_unavailable(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_replicas=2,
            alive_replicas=0,
            retry_num=0,
        )
        assert decision != RetryPolicy.RETRY or new_cl != ConsistencyLevel.LOCAL_ONE

    def test_second_attempt_does_not_downgrade(self):
        # After the first attempt the parent's retry/backoff machinery takes
        # over; the downgrade is a one-shot, not a loop.
        decision, new_cl = _policy().on_unavailable(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_replicas=2,
            alive_replicas=1,
            retry_num=1,
        )
        assert new_cl is None or new_cl != ConsistencyLevel.LOCAL_ONE

    def test_stronger_consistency_is_not_downgraded(self):
        # If the caller picked QUORUM the availability/consistency trade-off
        # was deliberate — don't silently weaken it.
        decision, new_cl = _policy().on_unavailable(
            query=None,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=2,
            alive_replicas=1,
            retry_num=0,
        )
        assert new_cl is None or new_cl != ConsistencyLevel.LOCAL_ONE

    def test_local_one_is_not_downgraded(self):
        # Already at the floor — nothing to downgrade to.
        decision, new_cl = _policy().on_unavailable(
            query=None,
            consistency=ConsistencyLevel.LOCAL_ONE,
            required_replicas=1,
            alive_replicas=0,
            retry_num=0,
        )
        assert new_cl is None or new_cl != ConsistencyLevel.LOCAL_ONE


class TestFallbackToLocalOneOnReadTimeout:
    def test_first_attempt_at_local_quorum_with_partial_responses_downgrades(self):
        decision, new_cl = _policy().on_read_timeout(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETRY
        assert new_cl == ConsistencyLevel.LOCAL_ONE

    def test_first_attempt_with_zero_responses_does_not_downgrade(self):
        # Zero responses = nothing to read at LOCAL_ONE either; fall back to
        # the parent's plain retry-and-backoff behavior.
        result = _policy().on_read_timeout(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_responses=2,
            received_responses=0,
            data_retrieved=False,
            retry_num=0,
        )
        # Parent retries (subject to max_retries) without changing CL.
        assert result[1] is None or result[1] != ConsistencyLevel.LOCAL_ONE

    def test_second_attempt_does_not_downgrade(self):
        result = _policy().on_read_timeout(
            query=None,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=1,
        )
        assert result[1] is None or result[1] != ConsistencyLevel.LOCAL_ONE

    def test_stronger_consistency_is_not_downgraded(self):
        result = _policy().on_read_timeout(
            query=None,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=0,
        )
        assert result[1] is None or result[1] != ConsistencyLevel.LOCAL_ONE


# ---------------------------------------------------------------------------
# list_txs_by_ids_eth: missing tx-id mappings
#
# While the delta-updater persists a batch, address_transactions rows can be
# visible before the matching transaction_ids_by_transaction_id_group row
# (data writes are sharded without cross-table ordering; summary statistics
# commit strictly afterwards). Such in-flight txs always lie above the last
# committed block and must be tolerated (dropped, aligned with None
# placeholders); misses at or below it are real inconsistencies and raise.
# Regression for a prod 500: `TypeError: 'NoneType' object is not
# subscriptable` on /eth/addresses/<a>/txs?direction=out&order=desc.
# ---------------------------------------------------------------------------

import logging  # noqa: E402

from graphsenselib.errors import DBInconsistencyException  # noqa: E402
from graphsenselib.utils.account import get_tx_id  # noqa: E402


class _FakeTxIdDb:
    """Stand-in providing only what list_txs_by_ids_eth touches."""

    list_txs_by_ids_eth = Cassandra.list_txs_by_ids_eth
    _check_unresolvable_tx_ids_tolerable = (
        Cassandra._check_unresolvable_tx_ids_tolerable
    )

    def __init__(self, tx_hash_by_id, last_committed_block):
        self._tx_hash_by_id = tx_hash_by_id
        self._no_blocks = last_committed_block + 1
        self.logger = logging.getLogger("test")

    def get_tx_id_group(self, currency, id_):
        return id_ // 10_000

    async def concurrent_with_args(
        self, currency, keyspace, statement, params, filter_empty=True
    ):
        assert not filter_empty
        return [
            {"transaction": self._tx_hash_by_id[tx_id]}
            if tx_id in self._tx_hash_by_id
            else None
            for _, tx_id in params
        ]

    async def get_currency_statistics(self, currency):
        return {"no_blocks": self._no_blocks}

    async def list_txs_by_hashes(self, currency, hashes, include_token_txs=False):
        txs = []
        for h in hashes:
            txs.append({"tx_hash": h})
            if include_token_txs:
                txs.append({"tx_hash": h, "type": "erc20"})
        return txs


async def test_all_tx_ids_resolvable_returns_aligned_txs():
    ids = [get_tx_id(100, 0), get_tx_id(100, 1)]
    db = _FakeTxIdDb(
        {ids[0]: b"aa", ids[1]: b"bb"},
        last_committed_block=100,
    )
    result = await db.list_txs_by_ids_eth("eth", ids)
    assert [tx["tx_hash"] for tx in result] == [b"aa", b"bb"]


async def test_miss_above_last_committed_block_is_dropped_with_placeholder():
    resolvable = get_tx_id(100, 0)
    in_flight = get_tx_id(101, 3)  # above last committed block 100
    db = _FakeTxIdDb({resolvable: b"aa"}, last_committed_block=100)

    result = await db.list_txs_by_ids_eth("eth", [resolvable, in_flight])

    # alignment with ids must survive: callers zip ids with the result
    assert result == [{"tx_hash": b"aa"}, None]


async def test_miss_at_or_below_last_committed_block_raises():
    resolvable = get_tx_id(100, 0)
    stale = get_tx_id(99, 1)  # below last committed block -> inconsistency
    db = _FakeTxIdDb({resolvable: b"aa"}, last_committed_block=100)

    with pytest.raises(DBInconsistencyException):
        await db.list_txs_by_ids_eth("eth", [resolvable, stale])


async def test_tolerated_miss_with_token_txs_drops_without_placeholder():
    # with include_token_txs the result interleaves token txs and is never
    # positionally aligned with ids, so no placeholders are inserted
    resolvable = get_tx_id(100, 0)
    in_flight = get_tx_id(101, 0)
    db = _FakeTxIdDb({resolvable: b"aa"}, last_committed_block=100)

    result = await db.list_txs_by_ids_eth(
        "eth", [resolvable, in_flight], include_token_txs=True
    )

    assert None not in result
    assert [tx["tx_hash"] for tx in result] == [b"aa", b"aa"]


class TestFallbackToLocalOneInheritedBehavior:
    def test_write_timeout_path_is_inherited_unchanged(self):
        # The web reader is read-only, but if a write ever flows through
        # this policy it must NOT be silently downgraded. The parent's
        # contract (only retry idempotent writes) must keep applying.
        non_idempotent_query = SimpleNamespace(is_idempotent=False)
        decision, _ = _policy().on_write_timeout(
            query=non_idempotent_query,
            consistency=ConsistencyLevel.LOCAL_QUORUM,
            write_type="SIMPLE",
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETHROW
