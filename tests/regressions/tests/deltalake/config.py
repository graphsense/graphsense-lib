"""Test configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DeltaTestConfig:
    """Configuration for Delta Lake cross-version compatibility tests."""

    ref_version: str = field(
        default_factory=lambda: os.environ.get("DELTA_REF_VERSION", "v25.11.18")
    )
    currency: str = field(
        default_factory=lambda: os.environ.get("DELTA_CURRENCY", "eth")
    )
    start_block: int = field(
        default_factory=lambda: int(os.environ.get("DELTA_START_BLOCK", "1000000"))
    )
    base_blocks: int = field(
        default_factory=lambda: int(os.environ.get("DELTA_BASE_BLOCKS", "50"))
    )
    append_blocks: int = field(
        default_factory=lambda: int(os.environ.get("DELTA_APPEND_BLOCKS", "50"))
    )
    node_url: str = field(
        default_factory=lambda: os.environ.get("NODE_URL", "")
    )
    gslib_path: Path = field(
        default_factory=lambda: Path(
            os.environ.get(
                "GSLIB_PATH",
                str(Path(__file__).resolve().parents[4]),
            )
        )
    )

    @property
    def base_end_block(self) -> int:
        return self.start_block + self.base_blocks

    @property
    def append_start_block(self) -> int:
        return self.base_end_block + 1

    @property
    def append_end_block(self) -> int:
        return self.append_start_block + self.append_blocks - 1

    @property
    def tables(self) -> list[str]:
        """Tables to compare — currency-dependent."""
        if self.currency == "eth":
            return ["block", "transaction", "log", "trace"]
        elif self.currency == "trx":
            return ["block", "transaction", "trace"]
        else:
            # UTXO currencies
            return ["block", "transaction"]


def get_minio_storage_options(endpoint: str, access_key: str, secret_key: str) -> dict[str, str]:
    """Build AWS-compatible storage options dict for deltalake / S3."""
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
