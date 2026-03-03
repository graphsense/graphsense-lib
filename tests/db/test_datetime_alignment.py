from datetime import datetime, timezone

from graphsenselib.db.analytics import _align_datetime_timezones


def test_align_datetime_timezones_handles_aware_and_naive_pair():
    block_ts = datetime(2009, 1, 14, 21, 27, 29, tzinfo=timezone.utc)
    input_ts = datetime(2009, 1, 14, 21, 27, 29)

    aligned_block_ts, aligned_input_ts = _align_datetime_timezones(block_ts, input_ts)

    assert aligned_block_ts.tzinfo == timezone.utc
    assert aligned_input_ts.tzinfo == timezone.utc
    assert aligned_block_ts <= aligned_input_ts


def test_align_datetime_timezones_sets_utc_for_both_naive_inputs():
    left = datetime(2020, 1, 1, 0, 0, 0)
    right = datetime(2020, 1, 1, 1, 0, 0)

    aligned_left, aligned_right = _align_datetime_timezones(left, right)

    assert aligned_left.tzinfo == timezone.utc
    assert aligned_right.tzinfo == timezone.utc
    assert aligned_left < aligned_right
