"""Chain-truth assertion: ETH execution-layer rewards stop at the Merge.

Pre-Merge (block_id < 15_537_394) every block carries one synthesized
``trace_type='reward'`` entry from ``trace_block``. Post-Merge there are
zero -- validators are paid on the consensus layer, not the execution
layer. A regression that re-introduces phantom reward rows for post-Merge
blocks (as happened historically in test/Cassandra ingests) silently
inflates miner/builder balances; this test fails fast on that.

Runs only on the ``eth/merge_boundary`` parametrization; skips otherwise.
"""

import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable

from tests.transformation.config import TransformationConfig
from tests.transformation.ingest_runner import run_ingest_delta_only

pytestmark = pytest.mark.transformation

MERGE_BLOCK = 15_537_394  # first PoS block on Ethereum mainnet


class TestEthMergeBoundary:
    """No execution-layer reward traces for post-Merge ETH blocks."""

    def test_no_reward_traces_post_merge(
        self,
        transformation_config: TransformationConfig,
        minio_config: dict[str, str],
        storage_options: dict[str, str],
        current_venv,
    ):
        if transformation_config.currency != "eth":
            pytest.skip("ETH-specific chain-truth assertion")
        if transformation_config.range_id != "merge_boundary":
            pytest.skip("only runs on the merge_boundary range")

        bucket = minio_config["bucket"]
        delta_path = f"s3://{bucket}/eth/merge_boundary/chain_truth"
        trace_path = f"{delta_path}/trace"

        print(
            f"\n  delta-only ingest "
            f"[{transformation_config.start_block:,}, "
            f"{transformation_config.end_block:,}] ..."
        )
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=transformation_config,
            delta_directory=delta_path,
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        dt = DeltaTable(trace_path, storage_options=storage_options)
        traces = dt.to_pyarrow_dataset().to_table(
            columns=["block_id", "trace_type"]
        )

        is_reward = pc.equal(traces["trace_type"], "reward")
        is_pre_merge = pc.less(traces["block_id"], MERGE_BLOCK)
        is_post_merge = pc.greater_equal(traces["block_id"], MERGE_BLOCK)

        pre_merge_rewards = pc.sum(
            pc.cast(pc.and_(is_reward, is_pre_merge), "int64")
        ).as_py()
        post_merge_rewards = pc.sum(
            pc.cast(pc.and_(is_reward, is_post_merge), "int64")
        ).as_py()

        n_pre = max(0, MERGE_BLOCK - transformation_config.start_block)
        n_post = max(0, transformation_config.end_block + 1 - MERGE_BLOCK)

        print(
            f"  pre-Merge blocks in range:  {n_pre} -> "
            f"reward rows: {pre_merge_rewards}"
        )
        print(
            f"  post-Merge blocks in range: {n_post} -> "
            f"reward rows: {post_merge_rewards}"
        )

        # Hard assertion: the chain has no execution-layer rewards post-Merge.
        assert post_merge_rewards == 0, (
            f"phantom reward traces detected post-Merge: "
            f"{post_merge_rewards} rows with trace_type='reward' "
            f"and block_id >= {MERGE_BLOCK}"
        )

        # Soft sanity: pre-Merge blocks should each carry one reward row.
        # (Edge: uncle inclusion can add extras; we only require >= 1 per block.)
        if n_pre > 0:
            assert pre_merge_rewards >= n_pre, (
                f"expected at least one reward per pre-Merge block, got "
                f"{pre_merge_rewards} rows for {n_pre} blocks"
            )
