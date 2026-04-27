# ruff: noqa: T201
import re
from pathlib import Path


SEMVER_BASE = r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"

# Pre-release per SemVer 2.0 §9: dot-separated identifiers of [0-9A-Za-z-];
# numeric identifiers must not have leading zeroes.
_PRERELEASE_IDENT = r"(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9A-Za-z-]*)"
SEMVER_ALLOWED_PRERELEASE = rf"(?:-{_PRERELEASE_IDENT}(?:\.{_PRERELEASE_IDENT})*)?"

# Build metadata per SemVer 2.0 §10: dot-separated identifiers of [0-9A-Za-z-].
_BUILD_IDENT = r"[0-9A-Za-z-]+"
SEMVER_ALLOWED_BUILD = rf"(?:\+{_BUILD_IDENT}(?:\.{_BUILD_IDENT})*)?"

SEMVER_CORE_RE = re.compile(
    rf"^{SEMVER_BASE}{SEMVER_ALLOWED_PRERELEASE}{SEMVER_ALLOWED_BUILD}$"
)
SEMVER_WITH_V_RE = re.compile(
    rf"^v{SEMVER_BASE}{SEMVER_ALLOWED_PRERELEASE}{SEMVER_ALLOWED_BUILD}$"
)


def _extract(pattern: re.Pattern[str], text: str, source: str) -> str:
    match = pattern.search(text)
    if not match:
        raise ValueError(f"Could not extract value from {source}")
    return match.group(1)


def _is_semver_core(version: str) -> bool:
    return bool(SEMVER_CORE_RE.fullmatch(version))


def _is_semver_with_optional_v(version: str) -> bool:
    return _is_semver_core(version) or bool(SEMVER_WITH_V_RE.fullmatch(version))


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    makefile_text = (repo_root / "Makefile").read_text(encoding="utf-8")
    web_version_text = (repo_root / "src/graphsenselib/web/version.py").read_text(
        encoding="utf-8"
    )
    client_pyproject_text = (repo_root / "clients/python/pyproject.toml").read_text(
        encoding="utf-8"
    )

    release_sem = _extract(
        re.compile(r"^RELEASESEM\s*:=\s*['\"]?([^'\"\n]+)['\"]?", re.MULTILINE),
        makefile_text,
        "Makefile RELEASESEM",
    )
    webapi_sem = _extract(
        re.compile(r"^WEBAPISEM\s*:=\s*['\"]?([^'\"\n]+)['\"]?", re.MULTILINE),
        makefile_text,
        "Makefile WEBAPISEM",
    )
    api_version = _extract(
        re.compile(r'^__api_version__\s*=\s*"([^"]+)"', re.MULTILINE),
        web_version_text,
        "src/graphsenselib/web/version.py __api_version__",
    )
    client_version = _extract(
        re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE),
        client_pyproject_text,
        "clients/python/pyproject.toml [project].version",
    )

    errors: list[str] = []

    if not _is_semver_with_optional_v(release_sem):
        errors.append(
            f"RELEASESEM must be SemVer (optionally prefixed with 'v'), got: {release_sem}"
        )
    if not _is_semver_with_optional_v(webapi_sem):
        errors.append(
            f"WEBAPISEM must be SemVer (optionally prefixed with 'v'), got: {webapi_sem}"
        )
    if not _is_semver_core(api_version):
        errors.append(
            "src/graphsenselib/web/version.py __api_version__ must be "
            f"SemVer 2.0, got: {api_version}"
        )
    if not _is_semver_core(client_version):
        errors.append(
            "clients/python/pyproject.toml version must be "
            f"SemVer 2.0, got: {client_version}"
        )

    if errors:
        print("SemVer validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("SemVer validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
