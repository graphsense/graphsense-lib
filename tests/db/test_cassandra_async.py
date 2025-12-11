from graphsenselib.db.asynchronous.cassandra import check_height_bounds_impossible


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
