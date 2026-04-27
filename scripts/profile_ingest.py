# ruff: noqa: T201
"""Profile the UTXO ingest pipeline by running a real ingest with cProfile.

Connects to the actual node (from .graphsense.yaml), fetches blocks,
transforms, and writes to a local temp directory.

Usage:
    uv run python scripts/profile_ingest.py -c bch --start-block 700000 --blocks 100
    uv run python scripts/profile_ingest.py -c btc --start-block 600000 --blocks 10
"""

import argparse
import cProfile
import io
import logging
import pstats
import tempfile

from graphsenselib.config import get_config
from graphsenselib.ingest.dump import export_delta

logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(description="Profile UTXO ingest pipeline")
    parser.add_argument("-c", "--currency", required=True)
    parser.add_argument("-e", "--env", default="dev")
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--blocks", type=int, default=100)
    parser.add_argument("--top", type=int, default=50)
    args = parser.parse_args()

    end_block = args.start_block + args.blocks - 1
    config = get_config()
    ks_config = config.get_keyspace_config(args.env, args.currency)
    ic = ks_config.ingest_config
    sources = ic.all_node_references

    with tempfile.TemporaryDirectory(prefix="gs-profile-") as tmpdir:
        print(
            f"Profiling {args.currency} ingest: blocks {args.start_block}-{end_block}"
        )
        print(f"Node: {sources}")
        print(f"Output dir: {tmpdir}\n")

        profiler = cProfile.Profile()
        profiler.enable()

        export_delta(
            currency=args.currency,
            sources=sources,
            directory=tmpdir,
            start_block=args.start_block,
            end_block=end_block,
            provider_timeout=300,
            write_mode="overwrite",
            ignore_overwrite_safechecks=True,
            lock_disabled=True,
        )

        profiler.disable()

        print("\n" + "=" * 70)
        print(f"cProfile: top {args.top} by TOTAL time (where CPU burns)")
        print("=" * 70)
        s = io.StringIO()
        pstats.Stats(profiler, stream=s).sort_stats("tottime").print_stats(args.top)
        print(s.getvalue())

        print("=" * 70)
        print(f"cProfile: top {args.top} by CUMULATIVE time")
        print("=" * 70)
        s2 = io.StringIO()
        pstats.Stats(profiler, stream=s2).sort_stats("cumulative").print_stats(args.top)
        print(s2.getvalue())

        profiler.dump_stats("/tmp/ingest_profile.prof")
        print("Full profile saved to /tmp/ingest_profile.prof")
        print("View with: uv run snakeviz /tmp/ingest_profile.prof")


if __name__ == "__main__":
    main()
