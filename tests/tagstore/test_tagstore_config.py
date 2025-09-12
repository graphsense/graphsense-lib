import pytest
from pydantic import ValidationError
from graphsenselib.config.tagstore_config import TagStoreReaderConfig


class TestTagStoreReaderConfig:
    """Test suite for TagStoreReaderConfig validation."""

    def test_valid_config(self):
        """Test creating config with valid values."""
        config = TagStoreReaderConfig(url="postgresql://localhost:5432/db")
        assert config.url == "postgresql://localhost:5432/db"
        assert config.pool_size == 50
        assert config.pool_timeout == 300
        assert config.max_overflow == 10
        assert config.pool_recycle == 3600
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
