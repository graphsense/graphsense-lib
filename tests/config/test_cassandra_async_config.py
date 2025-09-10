import pytest
from pydantic import ValidationError
from graphsenselib.config.cassandra_async_config import CassandraConfig, CurrencyConfig


class TestCurrencyConfig:
    """Test cases for CurrencyConfig model."""

    def test_currency_config_defaults(self):
        """Test CurrencyConfig with default values."""
        config = CurrencyConfig()
        assert config.raw is None
        assert config.transformed is None
        assert config.balance_provider is None

    def test_currency_config_with_values(self):
        """Test CurrencyConfig with provided values."""
        config = CurrencyConfig(
            raw="btc_raw",
            transformed="btc_transformed_20240101",
            balance_provider="node",
        )
        assert config.raw == "btc_raw"
        assert config.transformed == "btc_transformed_20240101"
        assert config.balance_provider == "node"

    def test_currency_config_partial_values(self):
        """Test CurrencyConfig with partial values."""
        config = CurrencyConfig(raw="eth_raw")
        assert config.raw == "eth_raw"
        assert config.transformed is None
        assert config.balance_provider is None


class TestCassandraConfig:
    """Test cases for CassandraConfig model."""

    def test_valid_minimal_config(self):
        """Test valid minimal configuration."""
        config_dict = {"currencies": {"btc": None, "eth": None}, "nodes": ["127.0.0.1"]}
        config = CassandraConfig(**config_dict)

        assert len(config.currencies) == 2
        assert isinstance(config.currencies["btc"], CurrencyConfig)
        assert isinstance(config.currencies["eth"], CurrencyConfig)
        assert config.nodes == ["127.0.0.1"]
        assert config.port == 9042  # default
        assert config.consistency_level == "LOCAL_ONE"  # default

    def test_valid_full_config(self):
        """Test valid full configuration."""
        config_dict = {
            "currencies": {
                "btc": {"raw": "btc_raw", "transformed": "btc_transformed"},
                "eth": {"raw": "eth_raw", "balance_provider": "node"},
            },
            "nodes": ["127.0.0.1", "127.0.0.2"],
            "port": 9043,
            "username": "cassandra",
            "password": "secret",
            "consistency_level": "QUORUM",
            "retry_interval": 10,
            "list_address_txs_ordered_legacy": True,
        }
        config = CassandraConfig(**config_dict)

        assert config.currencies["btc"].raw == "btc_raw"  # ty: ignore[possibly-unbound-attribute]
        assert config.currencies["btc"].transformed == "btc_transformed"  # ty: ignore[possibly-unbound-attribute]
        assert config.currencies["eth"].raw == "eth_raw"  # ty: ignore[possibly-unbound-attribute]
        assert config.currencies["eth"].balance_provider == "node"  # ty: ignore[possibly-unbound-attribute]
        assert config.nodes == ["127.0.0.1", "127.0.0.2"]
        assert config.port == 9043
        assert config.username == "cassandra"
        assert config.password == "secret"
        assert config.consistency_level == "QUORUM"
        assert config.retry_interval == 10
        assert config.list_address_txs_ordered_legacy is True

    def test_missing_required_fields(self):
        """Test validation errors for missing required fields."""
        # Missing currencies
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(nodes=["127.0.0.1"])
        assert "currencies" in str(exc_info.value)

        # Missing nodes
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(currencies={"btc": None})
        assert "nodes" in str(exc_info.value)

    def test_currencies_validation(self):
        """Test currencies field validation."""
        # Invalid type for currencies
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(currencies="invalid", nodes=["127.0.0.1"])  # ty: ignore[invalid-argument-type]
        assert "currencies must be a dictionary" in str(exc_info.value)

        # Invalid currency config type
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(currencies={"btc": "invalid_config"}, nodes=["127.0.0.1"])
        assert "Invalid config type for currency btc" in str(exc_info.value)

        # Valid currency configs
        config = CassandraConfig(
            currencies={
                "btc": None,
                "eth": {"raw": "eth_raw"},
                "ltc": CurrencyConfig(transformed="ltc_transformed"),
            },
            nodes=["127.0.0.1"],
        )
        assert isinstance(config.currencies["btc"], CurrencyConfig)
        assert isinstance(config.currencies["eth"], CurrencyConfig)
        assert isinstance(config.currencies["ltc"], CurrencyConfig)
        assert config.currencies["eth"].raw == "eth_raw"
        assert config.currencies["ltc"].transformed == "ltc_transformed"

    def test_nodes_validation(self):
        """Test nodes field validation."""
        # Empty nodes list
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(currencies={"btc": None}, nodes=[])
        assert "nodes list cannot be empty" in str(exc_info.value)

        # Valid nodes list
        config = CassandraConfig(
            currencies={"btc": None},
            nodes=["127.0.0.1", "192.168.1.1", "cassandra-node.example.com"],
        )
        assert len(config.nodes) == 3

    def test_consistency_level_validation(self):
        """Test consistency level validation."""
        # Invalid consistency level
        with pytest.raises(ValidationError) as exc_info:
            CassandraConfig(
                currencies={"btc": None},
                nodes=["127.0.0.1"],
                consistency_level="INVALID_LEVEL",
            )
        assert "consistency_level must be one of" in str(exc_info.value)

        # Valid consistency levels
        valid_levels = [
            "ANY",
            "ONE",
            "TWO",
            "THREE",
            "QUORUM",
            "ALL",
            "LOCAL_QUORUM",
            "EACH_QUORUM",
            "SERIAL",
            "LOCAL_SERIAL",
            "LOCAL_ONE",
        ]

        for level in valid_levels:
            config = CassandraConfig(
                currencies={"btc": None}, nodes=["127.0.0.1"], consistency_level=level
            )
            assert config.consistency_level == level

    def test_type_validation(self):
        """Test type validation for various fields."""
        # Invalid port type
        with pytest.raises(ValidationError):
            CassandraConfig(
                currencies={"btc": None},
                nodes=["127.0.0.1"],
                port="invalid_port",  # ty: ignore[invalid-argument-type]
            )

        # Invalid retry_interval type
        with pytest.raises(ValidationError):
            CassandraConfig(
                currencies={"btc": None},
                nodes=["127.0.0.1"],
                retry_interval="invalid_interval",  # ty: ignore[invalid-argument-type]
            )

        # Invalid list_address_txs_ordered_legacy type
        with pytest.raises(ValidationError):
            CassandraConfig(
                currencies={"btc": None},
                nodes=["127.0.0.1"],
                list_address_txs_ordered_legacy="invalid_bool",  # ty: ignore[invalid-argument-type]
            )

    def test_extra_fields_allowed(self):
        """Test that extra fields are allowed due to Config.extra = 'allow'."""
        config = CassandraConfig(
            currencies={"btc": None},
            nodes=["127.0.0.1"],
            custom_field="custom_value",  # ty: ignore[unknown-argument]
            another_extra_field=123,  # ty: ignore[unknown-argument]
        )

        # Extra fields should be accessible
        assert hasattr(config, "custom_field")
        assert config.custom_field == "custom_value"
        assert hasattr(config, "another_extra_field")
        assert config.another_extra_field == 123

    def test_config_serialization(self):
        """Test configuration serialization and deserialization."""
        original_config = CassandraConfig(
            currencies={"btc": {"raw": "btc_raw"}, "eth": None},
            nodes=["127.0.0.1"],
            port=9043,
            username="user",
        )

        # Serialize to dict
        config_dict = original_config.model_dump()

        # Deserialize back
        new_config = CassandraConfig(**config_dict)

        # Compare
        assert (
            new_config.currencies["btc"].raw == "btc_raw"  # ty: ignore[possibly-unbound-attribute]
        )
        assert isinstance(new_config.currencies["eth"], CurrencyConfig)
        assert new_config.nodes == ["127.0.0.1"]
        assert new_config.port == 9043
        assert new_config.username == "user"

    def test_json_serialization(self):
        """Test JSON serialization and deserialization."""
        config = CassandraConfig(
            currencies={"btc": {"raw": "btc_raw"}}, nodes=["127.0.0.1"]
        )

        # Serialize to JSON
        json_str = config.model_dump_json()

        # Deserialize from JSON
        new_config = CassandraConfig.model_validate_json(json_str)

        assert new_config.currencies["btc"].raw == "btc_raw"
        assert new_config.nodes == ["127.0.0.1"]


if __name__ == "__main__":
    pytest.main([__file__])
