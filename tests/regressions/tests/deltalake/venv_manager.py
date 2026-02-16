"""Create and cache uv virtual environments for reference and current versions."""

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path


VENV_CACHE_DIR = Path("/tmp/gslib-deltalake-testvenvs")


def _venv_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()[:12]


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return result


def _check_cli_exists(venv_dir: Path) -> bool:
    """Return True if graphsense-cli exists and is callable in the venv."""
    cli_bin = venv_dir / "bin" / "graphsense-cli"
    if not cli_bin.exists():
        return False
    # Just check the binary runs (--help works on all versions)
    result = subprocess.run(
        [str(cli_bin), "--help"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def get_venv_package_versions(venv_dir: Path, packages: list[str] | None = None) -> dict[str, str]:
    """Return installed package versions from a venv.

    If *packages* is given, only those are returned. Otherwise returns
    pyarrow, deltalake, and graphsense-lib by default.
    """
    if packages is None:
        packages = ["pyarrow", "deltalake", "graphsense-lib"]

    python_bin = str(venv_dir / "bin" / "python")
    result = subprocess.run(
        ["uv", "pip", "list", "--format=json", "--python", python_bin],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return {p: "unknown" for p in packages}

    installed = {
        entry["name"].lower(): entry["version"]
        for entry in json.loads(result.stdout)
    }

    lookup = {p.lower(): p for p in packages}
    return {
        original: installed.get(key, "not installed")
        for key, original in lookup.items()
    }


def get_or_create_reference_venv(ref_version: str, gslib_repo_url: str = "https://github.com/graphsense/graphsense-lib.git") -> Path:
    """Create (or reuse cached) venv with graphsense-lib at *ref_version*.

    Clones the repo at the reference tag and uses ``uv sync`` so that
    the ``uv.lock`` from that version is respected, giving us the exact
    dependency versions (pyarrow, deltalake, …) that shipped with it.

    The venv is cached under ``/tmp/gslib-deltalake-testvenvs/<hash>/``
    and reused when the graphsense-cli binary already exists.
    """
    venv_dir = VENV_CACHE_DIR / f"ref-{_venv_hash(ref_version)}"
    clone_dir = VENV_CACHE_DIR / f"ref-src-{_venv_hash(ref_version)}"
    marker = venv_dir / ".ref_version"

    # Fast path: already valid
    if venv_dir.exists() and marker.exists() and marker.read_text().strip() == ref_version:
        if _check_cli_exists(venv_dir):
            return venv_dir

    # Recreate from scratch
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    if clone_dir.exists():
        shutil.rmtree(clone_dir)

    # Clone repo at the reference tag (preserves uv.lock)
    _run(["git", "clone", "--depth", "1", "--branch", ref_version, gslib_repo_url, str(clone_dir)])

    # Use uv sync with the lock file from that version.
    # UV_PROJECT_ENVIRONMENT tells uv where to put the venv.
    env = {
        **os.environ,
        "UV_PROJECT_ENVIRONMENT": str(venv_dir),
    }
    _run(
        ["uv", "sync", "--extra", "ingest", "--frozen", "--python", "3.11"],
        cwd=str(clone_dir),
        env=env,
    )

    # Validate
    if not _check_cli_exists(venv_dir):
        raise RuntimeError(
            f"graphsense-cli not found in reference venv after install (ref={ref_version})"
        )

    marker.write_text(ref_version)
    return venv_dir


def get_or_create_current_venv(gslib_path: Path) -> Path:
    """Create (or reuse cached) venv with the *current* local graphsense-lib.

    Installs the local checkout in editable mode so that any code change
    is immediately reflected.
    """
    resolved = gslib_path.resolve()
    venv_dir = VENV_CACHE_DIR / f"current-{_venv_hash(str(resolved))}"

    # Always reinstall current to pick up latest changes
    if not venv_dir.exists():
        venv_dir.mkdir(parents=True, exist_ok=True)
        _run(["uv", "venv", str(venv_dir), "--python", "3.11"])

    _run([
        "uv", "pip", "install", "-e", f"{resolved}[ingest]",
        "--python", str(venv_dir / "bin" / "python"),
    ])

    if not _check_cli_exists(venv_dir):
        raise RuntimeError(
            f"graphsense-cli not found in current venv after install (path={resolved})"
        )

    return venv_dir
