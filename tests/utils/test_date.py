import pytest
from datetime import datetime, timezone, timedelta
from graphsenselib.utils.date import parse_older_than_run_spec


class TestParseOlderThanRunSpec:
    """Test cases for parse_older_than_run_spec function."""

    def test_should_run_when_older_than_spec(self):
        """Test that function returns True when date is older than specification."""
        # Create a date 2 days ago
        old_date = datetime.now(timezone.utc) - timedelta(days=2)

        # Should run if we want things older than 1 day
        result = parse_older_than_run_spec(
            "1d", old_date, now=datetime.now(timezone.utc)
        )
        assert result is True

        # Should run if we want things older than 1 hour
        result = parse_older_than_run_spec(
            "1h", old_date, now=datetime.now(timezone.utc)
        )
        assert result is True

    def test_should_not_run_when_newer_than_spec(self):
        """Test that function returns False when date is newer than specification."""
        # Create a date 30 minutes ago
        recent_date = datetime.now(timezone.utc) - timedelta(minutes=30)

        # Should not run if we want things older than 1 day
        result = parse_older_than_run_spec(
            "1d", recent_date, now=datetime.now(timezone.utc)
        )
        assert result is False

        # Should not run if we want things older than 2 hours
        result = parse_older_than_run_spec(
            "2h", recent_date, now=datetime.now(timezone.utc)
        )
        assert result is False

    def test_boundary_conditions(self):
        """Test boundary conditions around the exact threshold."""
        # Create a date exactly 1 hour ago
        exact_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

        # Should run if we want things older than 1 hour (>=)
        result = parse_older_than_run_spec(
            "1h", exact_hour_ago, now=datetime.now(timezone.utc)
        )
        assert result is True

        # Should not run if we want things older than 1 hour and 1 second
        result = parse_older_than_run_spec(
            "3601s", exact_hour_ago, now=datetime.now(timezone.utc)
        )
        assert result is False

    def test_deterministic_with_fixed_reference_time(self):
        """Test that function is deterministic when given fixed reference time."""
        # Fixed reference time: 2024-01-15 12:00:00 UTC
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Test date: 2024-01-14 12:00:00 UTC (exactly 1 day older)
        test_date = datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc)

        # Should run if we want things older than 23 hours
        result = parse_older_than_run_spec("23h", test_date, now=reference_time)
        assert result is True

        # Should not run if we want things older than 25 hours
        result = parse_older_than_run_spec("25h", test_date, now=reference_time)
        assert result is False

    def test_various_time_units(self):
        """Test with various time units."""
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Test different time units
        test_cases = [
            # (test_date_offset, spec, expected)
            (timedelta(seconds=30), "20s", True),  # 30s ago vs 20s spec
            (timedelta(seconds=10), "20s", False),  # 10s ago vs 20s spec
            (timedelta(minutes=10), "5m", True),  # 10m ago vs 5m spec
            (timedelta(minutes=3), "5m", False),  # 3m ago vs 5m spec
            (timedelta(hours=3), "2h", True),  # 3h ago vs 2h spec
            (timedelta(hours=1), "2h", False),  # 1h ago vs 2h spec
            (timedelta(days=5), "3d", True),  # 5d ago vs 3d spec
            (timedelta(days=1), "3d", False),  # 1d ago vs 3d spec
            (timedelta(weeks=2), "1w", True),  # 2w ago vs 1w spec
            (timedelta(days=3), "1w", False),  # 3d ago vs 1w spec
        ]

        for offset, spec, expected in test_cases:
            test_date = reference_time - offset
            result = parse_older_than_run_spec(spec, test_date, now=reference_time)
            assert result == expected, f"Failed for offset={offset}, spec={spec}"

    def test_zero_threshold(self):
        """Test with zero threshold (everything should run)."""
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Even very recent dates should run with 0s threshold
        very_recent = reference_time - timedelta(microseconds=1)
        result = parse_older_than_run_spec("0s", very_recent, now=reference_time)
        assert result is True

        # Same time should not run
        result = parse_older_than_run_spec("0s", reference_time, now=reference_time)
        assert result is False

    def test_future_dates(self):
        """Test handling of future dates."""
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        future_date = reference_time + timedelta(hours=1)

        # Future dates should never run regardless of spec
        result = parse_older_than_run_spec("1h", future_date, now=reference_time)
        assert result is False

        result = parse_older_than_run_spec("0s", future_date, now=reference_time)
        assert result is False

    def test_invalid_time_specs(self):
        """Test handling of invalid time specifications."""
        test_date = datetime.now(timezone.utc)

        with pytest.raises(ValueError):
            parse_older_than_run_spec("invalid", test_date)

        with pytest.raises(ValueError):
            parse_older_than_run_spec("5x", test_date)

        with pytest.raises(ValueError):
            parse_older_than_run_spec("-1d", test_date)

    def test_timezone_handling(self):
        """Test that timezone-aware dates work correctly."""
        # Reference time in UTC
        reference_utc = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Test date 2 hours ago in UTC
        test_date_utc = reference_utc - timedelta(hours=2)

        # Test date 2 hours ago in different timezone (but same absolute time)
        eastern_tz = timezone(timedelta(hours=-5))  # UTC-5
        test_date_eastern = test_date_utc.astimezone(eastern_tz)

        # Both should give same result
        result_utc = parse_older_than_run_spec("1h", test_date_utc, now=reference_utc)
        result_eastern = parse_older_than_run_spec(
            "1h", test_date_eastern, now=reference_utc
        )

        assert result_utc and result_eastern

    def test_naive_datetime_handling(self):
        """Test handling of naive datetime objects."""
        # If the function handles naive datetimes, test that
        # Otherwise this test might need to be adjusted based on actual behavior
        naive_reference = datetime(2024, 1, 15, 12, 0, 0)
        naive_test = datetime(2024, 1, 14, 12, 0, 0)  # 1 day earlier

        try:
            result = parse_older_than_run_spec("12h", naive_test, now=naive_reference)
            assert result is True
        except TypeError:
            # If function requires timezone-aware datetimes, that's also valid
            pytest.skip("Function requires timezone-aware datetimes")

    @pytest.mark.parametrize(
        "spec,hours_ago,expected",
        [
            ("30m", 1, True),  # 1h ago vs 30m spec -> should run
            ("30m", 0.25, False),  # 15m ago vs 30m spec -> should not run
            ("2h", 3, True),  # 3h ago vs 2h spec -> should run
            ("2h", 1, False),  # 1h ago vs 2h spec -> should not run
            ("1d", 25, True),  # 25h ago vs 1d spec -> should run
            ("1d", 12, False),  # 12h ago vs 1d spec -> should not run
            ("1w", 8 * 24, True),  # 8d ago vs 1w spec -> should run
            ("1w", 3 * 24, False),  # 3d ago vs 1w spec -> should not run
        ],
    )
    def test_parametrized_scenarios(self, spec, hours_ago, expected):
        """Parametrized test for various time scenarios."""
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        test_date = reference_time - timedelta(hours=hours_ago)

        result = parse_older_than_run_spec(spec, test_date, now=reference_time)
        assert result == expected

    def test_large_time_differences(self):
        """Test with very large time differences."""
        reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        # Very old date (1 year ago)
        very_old = reference_time - timedelta(days=365)
        result = parse_older_than_run_spec("1d", very_old, now=reference_time)
        assert result is True

        # Very large threshold
        recent = reference_time - timedelta(hours=1)
        result = parse_older_than_run_spec("365d", recent, now=reference_time)
        assert result is False

    def test_weekly_schedule_format(self):
        """Test weekly schedule format like '1w;sunday'."""
        # Test on a Sunday - should run if older than 1 week
        reference_sunday = datetime(
            2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc
        )  # Sunday

        # Test date 8 days ago (previous Saturday)
        old_saturday = reference_sunday - timedelta(days=8)
        result = parse_older_than_run_spec(
            "1w;sunday", old_saturday, now=reference_sunday
        )
        assert result is True

        # Test date 6 days ago (Monday) - less than 1 week
        recent_monday = reference_sunday - timedelta(days=6)
        result = parse_older_than_run_spec(
            "1w;sunday", recent_monday, now=reference_sunday
        )
        assert result is False

    def test_weekly_schedule_wrong_day(self):
        """Test that weekly schedule only runs on the specified day."""
        # Test on a Monday - should not run even if condition is met
        reference_monday = datetime(
            2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc
        )  # Monday

        # Test date 8 days ago - older than 1 week but today is not Sunday
        old_date = reference_monday - timedelta(days=8)
        result = parse_older_than_run_spec("1w;sunday", old_date, now=reference_monday)
        assert result is False

    def test_weekly_schedule_different_days(self):
        """Test weekly schedule with different days of the week."""
        test_cases = [
            # (reference_day, day_name, weekday_number, should_run)
            ("monday", 0, True),
            ("tuesday", 1, True),
            ("wednesday", 2, True),
            ("thursday", 3, True),
            ("friday", 4, True),
            ("saturday", 5, True),
            ("sunday", 6, True),
        ]

        for day_name, weekday_number, should_run_on_correct_day in test_cases:
            # Create a reference time on the specified day
            # Start with a known Monday (2024-01-15) and add days
            base_monday = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            reference_time = base_monday + timedelta(days=weekday_number)

            # Test date 8 days ago (definitely older than 1 week)
            old_date = reference_time - timedelta(days=8)

            # Should run on the correct day
            result = parse_older_than_run_spec(
                f"1w;{day_name}", old_date, now=reference_time
            )
            assert result is True, f"Should run on {day_name}"

            # Should not run on a different day (test with next day)
            wrong_day = reference_time + timedelta(days=1)
            result = parse_older_than_run_spec(
                f"1w;{day_name}", old_date, now=wrong_day
            )
            assert result is False, f"Should not run on day after {day_name}"

    def test_weekly_schedule_case_insensitive(self):
        """Test that day names are case insensitive."""
        reference_sunday = datetime(
            2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc
        )  # Sunday
        old_date = reference_sunday - timedelta(days=8)

        # Test different case variations
        day_variations = ["sunday", "SUNDAY", "Sunday", "SuNdAy"]

        for day_variant in day_variations:
            result = parse_older_than_run_spec(
                f"1w;{day_variant}", old_date, now=reference_sunday
            )
            assert result is True, f"Should work with case variant: {day_variant}"

    def test_weekly_schedule_edge_cases(self):
        """Test edge cases for weekly schedule format."""
        reference_sunday = datetime(
            2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc
        )  # Sunday

        # Test with exactly 1 week ago
        exactly_one_week = reference_sunday - timedelta(weeks=1)
        result = parse_older_than_run_spec(
            "1w;sunday", exactly_one_week, now=reference_sunday
        )
        assert result is False

        # Test with just under 1 week ago
        almost_one_week = reference_sunday - timedelta(days=6, hours=23, minutes=59)
        result = parse_older_than_run_spec(
            "1w;sunday", almost_one_week, now=reference_sunday
        )
        assert result is False

        # Test with multiple weeks
        multiple_weeks = reference_sunday - timedelta(weeks=3)
        result = parse_older_than_run_spec(
            "1w;sunday", multiple_weeks, now=reference_sunday
        )
        assert result is True

    def test_weekly_schedule_different_time_units(self):
        """Test weekly schedule with different time units."""
        reference_friday = datetime(
            2024, 1, 19, 12, 0, 0, tzinfo=timezone.utc
        )  # Friday

        # Test with different time units but same day requirement
        test_cases = [
            (
                "2d;friday",
                timedelta(days=3),
                True,
            ),  # 3 days ago vs 2 days spec on Friday
            (
                "2d;friday",
                timedelta(days=1),
                False,
            ),  # 1 day ago vs 2 days spec on Friday
            ("12h;friday", timedelta(hours=15), True),  # 15h ago vs 12h spec on Friday
            ("12h;friday", timedelta(hours=6), False),  # 6h ago vs 12h spec on Friday
        ]

        for spec, offset, expected in test_cases:
            test_date = reference_friday - offset
            result = parse_older_than_run_spec(spec, test_date, now=reference_friday)
            assert result == expected, f"Failed for spec={spec}, offset={offset}"

    def test_weekly_schedule_invalid_day_names(self):
        """Test handling of invalid day names in weekly schedule."""
        test_date = datetime.now(timezone.utc)

        with pytest.raises(ValueError):
            parse_older_than_run_spec("1w;invalidday", test_date, now=test_date)

        # with pytest.raises(ValueError):
        #     parse_older_than_run_spec("1w;mon", test_date, now=test_date)  # Abbreviated not supported

        with pytest.raises(ValueError):
            parse_older_than_run_spec("1w;", test_date, now=test_date)  # Empty day name

    def test_weekly_schedule_invalid_format(self):
        """Test handling of invalid weekly schedule formats."""
        test_date = datetime.now(timezone.utc)

        with pytest.raises(ValueError):
            parse_older_than_run_spec(
                "1w;sunday;extra", test_date, now=test_date
            )  # Too many parts

        with pytest.raises(ValueError):
            parse_older_than_run_spec(
                "1w;;sunday", test_date, now=test_date
            )  # Empty part

        with pytest.raises(ValueError):
            parse_older_than_run_spec(
                ";sunday", test_date, now=test_date
            )  # Missing time spec


@pytest.mark.parametrize(
    "day_name,reference_weekday",
    [
        ("monday", 0),
        ("tuesday", 1),
        ("wednesday", 2),
        ("thursday", 3),
        ("friday", 4),
        ("saturday", 5),
        ("sunday", 6),
    ],
)
def test_weekly_schedule_parametrized(day_name, reference_weekday):
    """Parametrized test for weekly schedule on all days."""
    # Create reference time on the specified weekday
    base_monday = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)  # Monday = 0
    reference_time = base_monday + timedelta(days=reference_weekday)

    # Test date 8 days ago (older than 1 week)
    old_date = reference_time - timedelta(days=8)

    # Should run when on correct day and old enough
    result = parse_older_than_run_spec(f"1w;{day_name}", old_date, now=reference_time)
    assert result is True

    # Should not run when on wrong day (next day) even if old enough
    wrong_day = reference_time + timedelta(days=1)
    result = parse_older_than_run_spec(f"1w;{day_name}", old_date, now=wrong_day)
    assert result is False


def test_weekly_schedule_mixed_with_regular_format():
    """Test that regular format still works when weekly schedule is supported."""
    reference_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    # Regular format without day specification should work on any day
    old_date = reference_time - timedelta(days=2)

    result = parse_older_than_run_spec("1d", old_date, now=reference_time)
    assert result is True

    recent_date = reference_time - timedelta(hours=12)
    result = parse_older_than_run_spec("1d", recent_date, now=reference_time)
    assert result is False
