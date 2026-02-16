"""Utilities for baseline version detection."""

import re
import subprocess
from functools import lru_cache
from os import environ


@lru_cache(maxsize=1)
def get_previous_stable_version() -> str:
    """Get previous stable version for baseline comparison.

    Priority:
    1. BASELINE_VERSION env var (explicit override)
    2. Previous stable git tag (vYY.MM.patch pattern, no rc/alpha/beta)
    3. Fallback to hardcoded version
    """
    if environ.get("BASELINE_VERSION"):
        return environ["BASELINE_VERSION"]

    # Try to get sorted git tags from graphsense-lib repo
    try:
        result = subprocess.run(
            ["git", "tag", "--sort=-v:refname"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        tags = [t.strip() for t in result.stdout.split("\n") if t.strip()]

        # Filter stable releases (vYY.MM.patch without suffixes)
        stable_pattern = re.compile(r"^v\d+\.\d+\.\d+$")
        stable_tags = [t for t in tags if stable_pattern.match(t)]

        # Get current version
        current_result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        current = current_result.stdout.strip()

        # Return first stable tag that's not current
        for tag in stable_tags:
            if tag != current:
                return tag
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    return "v25.11.17"  # Fallback


def get_baseline_image() -> str:
    """Get Docker image reference for baseline container."""
    version = get_previous_stable_version()
    registry = environ.get("BASELINE_REGISTRY", "ghcr.io/graphsense/graphsense-lib")
    return f"{registry}:{version}"
