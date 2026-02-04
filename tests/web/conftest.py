import json
import subprocess
from collections import defaultdict
from datetime import datetime
from os import environ
from pathlib import Path

import docker
import pytest
from testcontainers.cassandra import CassandraContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from tests.web import BaseTestCase
from tests.web.cassandra.insert import load_test_data as cas_load_test_data
from tests.web.tagstore.insert import load_test_data as tags_load_test_data

# Global timing data collection for migration tests
_migration_timing_data = []

postgres = PostgresContainer("postgres:16-alpine")
redis = RedisContainer("redis:7-alpine")

# Pre-baked Cassandra image with schemas and fast startup settings already configured
# Build with: make build-test-cassandra
# Baked-in optimizations: NUM_TOKENS=1, ring_delay_ms=100, skip_wait_for_gossip=0
CASSANDRA_TEST_IMAGE = environ.get(
    "CASSANDRA_TEST_IMAGE", "graphsense/cassandra-test:4.1.4"
)


def ensure_cassandra_image_exists():
    """Build Cassandra test image if it doesn't exist locally."""
    client = docker.from_env()
    try:
        client.images.get(CASSANDRA_TEST_IMAGE)
    except docker.errors.ImageNotFound:
        dockerfile_path = Path(__file__).parent / "cassandra"
        subprocess.run(
            ["docker", "build", "-t", CASSANDRA_TEST_IMAGE, str(dockerfile_path)],
            check=True,
        )


ensure_cassandra_image_exists()
cassandra = CassandraContainer(CASSANDRA_TEST_IMAGE)


@pytest.fixture(scope="session", autouse=True)
def gs_rest_db_setup(request):
    SKIP_REST_CONTAINER_SETUP = environ.get("SKIP_REST_CONTAINER_SETUP", False)
    if SKIP_REST_CONTAINER_SETUP:
        return

    postgres.start()
    cassandra.start()
    redis.start()

    def remove_container():
        postgres.stop()
        cassandra.stop()
        redis.stop()

    request.addfinalizer(remove_container)

    cas_host = cassandra.get_container_host_ip()
    cas_port = cassandra.get_exposed_port(9042)

    postgres_sync_url = postgres.get_connection_url()
    portgres_async_url = postgres_sync_url.replace("psycopg2", "asyncpg")

    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}"

    config = {
        "logging": {"level": "DEBUG"},
        "database": {
            "driver": "cassandra",
            "port": cas_port,
            "nodes": [cas_host],
            "strict_data_validation": False,
            "currencies": {
                "btc": {
                    "raw": "resttest_btc_raw",
                    "transformed": "resttest_btc_transformed",
                },
                "ltc": {
                    "raw": "resttest_ltc_raw",
                    "transformed": "resttest_ltc_transformed",
                },
                "eth": {
                    "raw": "resttest_eth_raw",
                    "transformed": "resttest_eth_transformed",
                },
                "trx": {
                    "raw": "resttest_trx_raw",
                    "transformed": "resttest_trx_transformed",
                },
            },
        },
        "gs-tagstore": {"url": portgres_async_url},
        "show_private_tags": {"on_header": {"Authorization": "x"}},
        "tag_access_logger": {"redis_url": redis_url},
    }

    # Ugly hack to pass parameters
    BaseTestCase.config = config

    cas_load_test_data(cas_host, cas_port)

    tags_load_test_data(postgres_sync_url.replace("+psycopg2", ""))

    return config


def record_migration_timing(
    endpoint: str, old_time: float, new_time: float, pattern: str = None
):
    """Record timing data for a migration test endpoint."""
    _migration_timing_data.append(
        {
            "endpoint": endpoint,
            "pattern": pattern or endpoint,
            "old_time": old_time,
            "new_time": new_time,
            "speedup": old_time / new_time if new_time > 0 else 0,
        }
    )


@pytest.fixture(scope="session", autouse=True)
def migration_timing_report(request):
    """Generate timing report at end of migration test session."""
    yield  # Run all tests first

    if not _migration_timing_data:
        return

    # Get pytest's terminal writer for output
    terminalreporter = request.config.pluginmanager.get_plugin("terminalreporter")
    write = terminalreporter.write_line if terminalreporter else lambda x: None

    # Calculate totals
    total_old = sum(t["old_time"] for t in _migration_timing_data)
    total_new = sum(t["new_time"] for t in _migration_timing_data)

    # Group by pattern
    by_pattern = defaultdict(list)
    for t in _migration_timing_data:
        by_pattern[t["pattern"]].append(t)

    # Calculate pattern averages
    pattern_stats = []
    for pattern, timings in by_pattern.items():
        old_avg = sum(t["old_time"] for t in timings) / len(timings)
        new_avg = sum(t["new_time"] for t in timings) / len(timings)
        pattern_stats.append(
            {
                "pattern": pattern,
                "count": len(timings),
                "old_avg": old_avg,
                "new_avg": new_avg,
                "speedup": old_avg / new_avg if new_avg > 0 else 0,
                "diff_ms": (new_avg - old_avg) * 1000,
            }
        )

    # Sort by slowdown (new slower than old)
    pattern_stats.sort(key=lambda x: x["speedup"])

    # Write report
    write("")
    write("=" * 80)
    write("MIGRATION TIMING REPORT")
    write("=" * 80)
    write(f"Total endpoints tested: {len(_migration_timing_data)}")
    write(f"Total old server time:  {total_old:.2f}s")
    write(f"Total new server time:  {total_new:.2f}s")
    write(
        f"Overall speedup:        {total_old / total_new:.2f}x"
        if total_new > 0
        else "N/A"
    )

    # Significantly slower endpoints (new > 1.5x slower than old)
    slower = [p for p in pattern_stats if p["speedup"] < 0.67]
    if slower:
        write(f"\n⚠️  SIGNIFICANTLY SLOWER ENDPOINTS ({len(slower)} patterns):")
        write("-" * 80)
        for p in slower[:20]:
            write(f"  {p['pattern'][:60]:<60}")
            write(
                f"    Count: {p['count']:4d}  Old: {p['old_avg'] * 1000:7.1f}ms  "
                f"New: {p['new_avg'] * 1000:7.1f}ms  "
                f"Slower by: {p['diff_ms']:+.1f}ms ({p['speedup']:.2f}x)"
            )

    # Significantly faster endpoints (new > 1.5x faster than old)
    faster = [p for p in pattern_stats if p["speedup"] > 1.5]
    if faster:
        write(f"\n✅ SIGNIFICANTLY FASTER ENDPOINTS ({len(faster)} patterns):")
        write("-" * 80)
        for p in sorted(faster, key=lambda x: -x["speedup"])[:10]:
            write(f"  {p['pattern'][:60]:<60}")
            write(
                f"    Count: {p['count']:4d}  Old: {p['old_avg'] * 1000:7.1f}ms  "
                f"New: {p['new_avg'] * 1000:7.1f}ms  "
                f"Faster by: {-p['diff_ms']:+.1f}ms ({p['speedup']:.2f}x)"
            )

    write("")
    write("=" * 80)

    # Save detailed report to file
    report_path = Path("tests/migration_timing_report.json")
    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_endpoints": len(_migration_timing_data),
            "total_old_time_s": total_old,
            "total_new_time_s": total_new,
            "overall_speedup": total_old / total_new if total_new > 0 else 0,
        },
        "pattern_stats": pattern_stats,
        "raw_data": _migration_timing_data,
    }
    report_path.write_text(json.dumps(report, indent=2))
    write(f"Detailed report saved to: {report_path}")


@pytest.fixture
async def redis_client(gs_rest_db_setup):
    """Provide an async Redis client for tests."""
    from redis import asyncio as aioredis

    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}"

    client = await aioredis.from_url(redis_url)
    yield client
    await client.flushdb()
    await client.aclose()
