"""API versioning utilities.

The project uses two versioning schemes:
- Date versioning: v25.11.18 (YYYY.MM.patch) - for internal library releases
- Semantic versioning: v2.8.18 (major.minor.patch) - for REST API clients

The REST API and Python client should always use semantic versioning for
backward compatibility and proper semver semantics.
"""

import re
from pathlib import Path


def _find_makefile() -> Path:
    """Find the Makefile by searching upward from this file's location."""
    current = Path(__file__).resolve().parent
    for _ in range(10):  # Max 10 levels up
        makefile = current / "Makefile"
        if makefile.exists():
            return makefile
        if current.parent == current:  # Reached root
            break
        current = current.parent
    raise FileNotFoundError("Makefile not found in parent directories")


def get_api_version() -> str:
    """Get semantic version (v2.x.y format) for the API.

    Reads RELEASESEM from the Makefile as the single source of truth.

    Returns:
        Semantic version string without 'v' prefix (e.g., "2.8.18")

    Raises:
        FileNotFoundError: If Makefile cannot be found
        ValueError: If RELEASESEM cannot be parsed from Makefile
    """
    makefile = _find_makefile()
    content = makefile.read_text()

    # Match RELEASESEM := 'v2.8.18' or RELEASESEM := "v2.8.18"
    match = re.search(r"RELEASESEM\s*:=\s*['\"]?v?([^'\"]+)['\"]?", content)
    if not match:
        raise ValueError("RELEASESEM not found in Makefile")

    return match.group(1).strip()


# Cache the API version at module load time
__api_version__ = get_api_version()
