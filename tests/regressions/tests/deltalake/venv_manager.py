"""Create and cache uv virtual environments for reference and current versions."""

import hashlib
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


def get_or_create_reference_venv(ref_version: str, gslib_repo_url: str = "https://github.com/graphsense/graphsense-lib.git") -> Path:
    """Create (or reuse cached) venv with graphsense-lib at *ref_version*.

    Uses ``uv`` to create venvs.  The venv is cached under
    ``/tmp/gslib-deltalake-testvenvs/<hash>/`` and reused when the
    graphsense-cli binary already reports the expected version.
    """
    venv_dir = VENV_CACHE_DIR / f"ref-{_venv_hash(ref_version)}"
    marker = venv_dir / ".ref_version"

    # Fast path: already valid
    if venv_dir.exists() and marker.exists() and marker.read_text().strip() == ref_version:
        if _check_cli_exists(venv_dir):
            return venv_dir

    # Recreate from scratch
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    venv_dir.mkdir(parents=True, exist_ok=True)

    # Create venv
    _run(["uv", "venv", str(venv_dir), "--python", "3.12"])

    # Install graphsense-lib at the reference git ref
    install_spec = f"graphsense-lib[ingest] @ git+{gslib_repo_url}@{ref_version}"
    _run(["uv", "pip", "install", install_spec, "--python", str(venv_dir / "bin" / "python")])

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
        _run(["uv", "venv", str(venv_dir), "--python", "3.12"])

    _run([
        "uv", "pip", "install", "-e", f"{resolved}[ingest]",
        "--python", str(venv_dir / "bin" / "python"),
    ])

    if not _check_cli_exists(venv_dir):
        raise RuntimeError(
            f"graphsense-cli not found in current venv after install (path={resolved})"
        )

    return venv_dir
