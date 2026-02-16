"""Fixtures for REST API regression tests."""

import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.rest.version_utils import get_baseline_image

# Global timing data collection for regression tests
_regression_timing_data = []


@pytest.fixture(scope="session")
def current_server_url():
    """URL for the current server under test.

    Set CURRENT_SERVER env var to override (default: http://localhost:9000).
    """
    return os.environ.get("CURRENT_SERVER", "http://localhost:9000")


@pytest.fixture(scope="session")
def baseline_server_url():
    """Start baseline server container for comparison.

    The baseline version is determined by:
    1. BASELINE_VERSION env var (explicit override)
    2. Previous stable git tag (auto-detected)

    Set BASELINE_SERVER env var to use an existing server instead of starting a container.
    Set SKIP_BASELINE_CONTAINER=1 to skip this fixture entirely.
    """
    if os.environ.get("SKIP_BASELINE_CONTAINER"):
        pytest.skip("Baseline container disabled via SKIP_BASELINE_CONTAINER")

    # Allow using an existing baseline server
    if os.environ.get("BASELINE_SERVER"):
        yield os.environ["BASELINE_SERVER"]
        return

    # Generate config - either from env var or use defaults
    config_path = os.environ.get("CONFIG_FILE")
    if config_path:
        config_file_to_mount = config_path
        cleanup_config = False
    else:
        # Create minimal config pointing to production-like setup
        # This should be overridden via CONFIG_FILE env var in practice
        config = {
            "logging": {"level": "INFO"},
            "database": {
                "driver": "cassandra",
                "port": 9042,
                "nodes": ["localhost"],
            },
        }
        config_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, prefix="gs_baseline_config_"
        )
        config_file.write(yaml.dump(config))
        config_file.close()
        config_file_to_mount = config_file.name
        cleanup_config = True

    # Start baseline container with host networking
    image = get_baseline_image()
    baseline_port = os.environ.get("GS_REST_BASELINE_PORT", "9001")

    container = (
        DockerContainer(image)
        .with_network_mode("host")
        .with_volume_mapping(config_file_to_mount, "/config.yaml", "ro")
        .with_env("CONFIG_FILE", "/config.yaml")
        .with_env("GS_REST_PORT", baseline_port)
        .with_env("NUM_WORKERS", "1")
        .with_env("NUM_THREADS", "1")
    )
    container.start()
    wait_for_logs(container, "Application startup complete", timeout=120)

    yield f"http://localhost:{baseline_port}"

    container.stop()
    if cleanup_config:
        os.unlink(config_file_to_mount)


def record_regression_timing(
    endpoint: str, baseline_time: float, current_time: float, pattern: str = None
):
    """Record timing data for a regression test endpoint."""
    _regression_timing_data.append(
        {
            "endpoint": endpoint,
            "pattern": pattern or endpoint,
            "baseline_time": baseline_time,
            "current_time": current_time,
            "speedup": baseline_time / current_time if current_time > 0 else 0,
        }
    )


@pytest.fixture(scope="session", autouse=True)
def regression_timing_report(request):
    """Generate timing report at end of regression test session."""
    yield  # Run all tests first

    if not _regression_timing_data:
        return

    # Get pytest's terminal writer for output
    terminalreporter = request.config.pluginmanager.get_plugin("terminalreporter")
    write = terminalreporter.write_line if terminalreporter else print

    # Calculate totals
    total_baseline = sum(t["baseline_time"] for t in _regression_timing_data)
    total_current = sum(t["current_time"] for t in _regression_timing_data)

    # Group by pattern
    by_pattern = defaultdict(list)
    for t in _regression_timing_data:
        by_pattern[t["pattern"]].append(t)

    # Calculate pattern averages
    pattern_stats = []
    for pattern, timings in by_pattern.items():
        baseline_avg = sum(t["baseline_time"] for t in timings) / len(timings)
        current_avg = sum(t["current_time"] for t in timings) / len(timings)
        pattern_stats.append(
            {
                "pattern": pattern,
                "count": len(timings),
                "baseline_avg": baseline_avg,
                "current_avg": current_avg,
                "speedup": baseline_avg / current_avg if current_avg > 0 else 0,
                "diff_ms": (current_avg - baseline_avg) * 1000,
            }
        )

    # Sort by slowdown (current slower than baseline)
    pattern_stats.sort(key=lambda x: x["speedup"])

    # Write report
    write("")
    write("=" * 80)
    write("REGRESSION TIMING REPORT")
    write("=" * 80)
    write(f"Total endpoints tested: {len(_regression_timing_data)}")
    write(f"Total baseline time:    {total_baseline:.2f}s")
    write(f"Total current time:     {total_current:.2f}s")
    write(
        f"Overall speedup:        {total_baseline / total_current:.2f}x"
        if total_current > 0
        else "N/A"
    )

    # Significantly slower endpoints (current > 1.5x slower than baseline)
    slower = [p for p in pattern_stats if p["speedup"] < 0.67]
    if slower:
        write(f"\n  SIGNIFICANTLY SLOWER ENDPOINTS ({len(slower)} patterns):")
        write("-" * 80)
        for p in slower[:20]:
            write(f"  {p['pattern'][:60]:<60}")
            write(
                f"    Count: {p['count']:4d}  Baseline: {p['baseline_avg'] * 1000:7.1f}ms  "
                f"Current: {p['current_avg'] * 1000:7.1f}ms  "
                f"Slower by: {p['diff_ms']:+.1f}ms ({p['speedup']:.2f}x)"
            )

    # Significantly faster endpoints (current > 1.5x faster than baseline)
    faster = [p for p in pattern_stats if p["speedup"] > 1.5]
    if faster:
        write(f"\n  SIGNIFICANTLY FASTER ENDPOINTS ({len(faster)} patterns):")
        write("-" * 80)
        for p in sorted(faster, key=lambda x: -x["speedup"])[:10]:
            write(f"  {p['pattern'][:60]:<60}")
            write(
                f"    Count: {p['count']:4d}  Baseline: {p['baseline_avg'] * 1000:7.1f}ms  "
                f"Current: {p['current_avg'] * 1000:7.1f}ms  "
                f"Faster by: {-p['diff_ms']:+.1f}ms ({p['speedup']:.2f}x)"
            )

    write("")
    write("=" * 80)

    # Save detailed report to file
    report_dir = Path("reports")
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / "regression_timing_report.json"
    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_endpoints": len(_regression_timing_data),
            "total_baseline_time_s": total_baseline,
            "total_current_time_s": total_current,
            "overall_speedup": (
                total_baseline / total_current if total_current > 0 else 0
            ),
        },
        "pattern_stats": pattern_stats,
        "raw_data": _regression_timing_data,
    }
    report_path.write_text(json.dumps(report, indent=2))
    write(f"Detailed report saved to: {report_path}")
