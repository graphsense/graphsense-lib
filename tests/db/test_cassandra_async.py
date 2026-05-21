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
