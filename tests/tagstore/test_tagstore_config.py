import pytest
from pydantic import ValidationError
from graphsenselib.config.tagstore_config import (
    TagStoreReaderConfig,
    get_active_tagstore_config,
    get_tagstore_max_concurrency,
    set_active_tagstore_config,
)


class TestTagStoreReaderConfig:
    """Test suite for TagStoreReaderConfig validation."""

    def test_valid_config(self):
        """Test creating config with valid values."""
        config = TagStoreReaderConfig(url="postgresql://localhost:5432/db")
        assert config.url == "postgresql://localhost:5432/db"
        assert config.pool_size == 50
        assert config.pool_timeout == 10
        assert config.max_overflow == 10
        assert config.pool_recycle == 3600
        # Derived from pool_size (50 // 3 = 16).
        assert config.max_concurrency == 16
        assert config.enable_prepared_statements_cache is False

    def test_pool_size_validation(self):
        """Test pool_size validation."""
        # Valid values
        config = TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_size=1)
        assert config.pool_size == 1

        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_size=100
        )
        assert config.pool_size == 100

        # Invalid values
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_size=0)
        assert "pool_size must be at least 1" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_size=-1)
        assert "pool_size must be at least 1" in str(exc_info.value)

    def test_pool_timeout_validation(self):
        """Test pool_timeout validation."""
        # Valid values
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_timeout=1
        )
        assert config.pool_timeout == 1

        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_timeout=600
        )
        assert config.pool_timeout == 600

        # Invalid values
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_timeout=0)
        assert "pool_timeout must be at least 1" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_timeout=-5)
        assert "pool_timeout must be at least 1" in str(exc_info.value)

    def test_max_overflow_validation(self):
        """Test max_overflow validation."""
        # Valid values
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", max_overflow=0
        )
        assert config.max_overflow == 0

        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", max_overflow=20
        )
        assert config.max_overflow == 20

        # Invalid values
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", max_overflow=-1)
        assert "max_overflow must be non-negative" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", max_overflow=-10)
        assert "max_overflow must be non-negative" in str(exc_info.value)

    def test_pool_recycle_validation(self):
        """Test pool_recycle validation."""
        # Valid values
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_recycle=1
        )
        assert config.pool_recycle == 1

        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_recycle=7200
        )
        assert config.pool_recycle == 7200

        # Invalid values
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_recycle=0)
        assert "pool_recycle must be at least 1" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(url="postgresql://localhost:5432/db", pool_recycle=-1)
        assert "pool_recycle must be at least 1" in str(exc_info.value)

    def test_get_connection_url_without_cache(self):
        """Test get_connection_url when prepared statements cache is disabled."""
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", enable_prepared_statements_cache=False
        )
        result = config.get_connection_url()
        assert (
            result == "postgresql://localhost:5432/db?prepared_statement_cache_size=0"
        )

    def test_get_connection_url_with_existing_params(self):
        """Test get_connection_url when URL already has parameters."""
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db?sslmode=require",
            enable_prepared_statements_cache=False,
        )
        result = config.get_connection_url()
        assert (
            result
            == "postgresql://localhost:5432/db?sslmode=require&prepared_statement_cache_size=0"
        )

    def test_get_connection_url_with_cache_enabled(self):
        """Test get_connection_url when prepared statements cache is enabled."""
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", enable_prepared_statements_cache=True
        )
        result = config.get_connection_url()
        assert result == "postgresql://localhost:5432/db"

    def test_required_url_field(self):
        """Test that url field is required."""
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig()
        assert "Field required" in str(exc_info.value)

    def test_max_concurrency_explicit_override(self):
        """Explicit values override the derived default."""
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", max_concurrency=4
        )
        assert config.max_concurrency == 4

        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(
                url="postgresql://localhost:5432/db", max_concurrency=0
            )
        assert "max_concurrency must be at least 1" in str(exc_info.value)

    def test_max_concurrency_derived_default(self):
        """When unset, max_concurrency is derived as max(2, pool_size // 3)."""
        # Floor: tiny pool still gets parallelism.
        cfg = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_size=4, max_overflow=10
        )
        assert cfg.max_concurrency == 2

        # Linear region.
        cfg = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_size=21, max_overflow=10
        )
        assert cfg.max_concurrency == 7

        cfg = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db", pool_size=100, max_overflow=10
        )
        assert cfg.max_concurrency == 33

    def test_pool_capacity_must_cover_max_concurrency(self):
        """Pool capacity must satisfy the bounded fan-out cap."""
        # boundary: capacity == max_concurrency is allowed
        config = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db",
            pool_size=2,
            max_overflow=6,
            max_concurrency=8,
        )
        assert config.pool_size + config.max_overflow == config.max_concurrency

        # below boundary: must fail
        with pytest.raises(ValidationError) as exc_info:
            TagStoreReaderConfig(
                url="postgresql://localhost:5432/db",
                pool_size=2,
                max_overflow=4,
                max_concurrency=8,
            )
        assert "must be >= max_concurrency" in str(exc_info.value)


class TestRuntimeMaxConcurrency:
    """Verify the runtime accessor reads from the active TagStoreReaderConfig."""

    def setup_method(self):
        self._previous = get_active_tagstore_config()

    def teardown_method(self):
        set_active_tagstore_config(self._previous)

    def test_default_when_no_config_active(self):
        set_active_tagstore_config(None)
        # Falls back to the package default when no config has been activated.
        assert get_tagstore_max_concurrency() == 8

    def test_reads_from_active_config(self):
        cfg = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db",
            pool_size=20,
            max_overflow=20,
            max_concurrency=16,
        )
        set_active_tagstore_config(cfg)
        assert get_tagstore_max_concurrency() == 16

    def test_active_config_swap_is_visible(self):
        cfg_a = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db",
            pool_size=20,
            max_overflow=20,
            max_concurrency=4,
        )
        cfg_b = TagStoreReaderConfig(
            url="postgresql://localhost:5432/db",
            pool_size=20,
            max_overflow=20,
            max_concurrency=12,
        )
        set_active_tagstore_config(cfg_a)
        assert get_tagstore_max_concurrency() == 4
        set_active_tagstore_config(cfg_b)
        assert get_tagstore_max_concurrency() == 12
