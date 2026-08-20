"""Fetch graphsense-spark release jars and build the spark-submit command.

This drives the external Scala graphsense-spark job for the raw -> transformed
"full transform". The jar is downloaded from a public GitHub Release asset
(token-free) and cached; the job is then launched via spark-submit.
"""

import json
import logging
import os
import subprocess
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# cassandra-analytics is Provided in graphsense-spark, so it is NOT in the fat
# jar — the sidecar bulk-write path needs it added via --packages either way.
SIDECAR_PACKAGE = "org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0"

# JDK module flags the Cassandra SSTable bulk writer needs. The temp-dir
# redirect is appended separately (it depends on spark.local.dir).
_SIDECAR_MODULE_FLAGS = (
    "--add-exports java.base/jdk.internal.misc=ALL-UNNAMED "
    "--add-exports java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
)


# Tag prefix of the Spark release track inside the graphsense-lib monorepo.
# The repo carries several release tracks (library ``vX.Y.Z``, web API
# ``webapi-v*``, Rust wheels ``gs-clustering-v*``), so Spark jars are published
# under their own prefix. The archived standalone graphsense-spark repo had a
# single track and used bare ``vX.Y.Z`` tags — both shapes must keep resolving,
# because production configs pin jar versions from either.
SPARK_TAG_PREFIX = "spark-"


def strip_tag_prefix(version: str) -> str:
    """Version number of a release tag, without track prefix or leading ``v``.

    ``spark-v26.08.0`` and ``v26.08.0`` both yield ``26.08.0``.
    """
    if version.startswith(SPARK_TAG_PREFIX):
        version = version[len(SPARK_TAG_PREFIX) :]
    return version.lstrip("v")


def asset_name(artifact: str, version: str) -> str:
    """Release asset filename for an artifact + version.

    The release tag keeps its track prefix and leading ``v`` but the jar
    filename does not, e.g. tag ``spark-v26.08.0`` (or the legacy
    ``v26.06.0``) -> ``graphsense-spark-assembly-26.08.0.jar``.
    """
    v = strip_tag_prefix(version)
    if artifact == "fat":
        return f"graphsense-spark-assembly-{v}.jar"
    if artifact == "slim":
        return f"graphsense-spark_2.12-{v}.jar"
    raise ValueError(f"Unknown artifact '{artifact}' (expected 'fat' or 'slim')")


def resolve_latest_release(repo: str, tag_prefix: Optional[str] = None) -> str:
    """Resolve the latest stable Spark release tag for ``repo``.

    Without ``tag_prefix`` this uses the token-free ``/releases/latest``
    endpoint, which GitHub defines as the most recent non-draft, non-prerelease
    release — correct for a repository whose releases are all Spark jars (the
    archived standalone graphsense-spark).

    In the monorepo that endpoint is wrong: it reports the newest release of
    ANY track, so a Python-only library release would resolve to a tag that
    carries no jar, and the download would 404. With a ``tag_prefix`` the
    release list is scanned instead and the newest stable release whose tag
    starts with that prefix wins.
    """
    if not tag_prefix:
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        logger.info(f"Resolving latest stable graphsense-spark release from {url}")
        req = Request(url, headers={"Accept": "application/vnd.github+json"})
        with urlopen(req, timeout=30) as resp:  # noqa: S310
            data = json.load(resp)
        tag = data.get("tag_name")
        if not tag:
            raise ValueError(
                f"GitHub API returned no tag_name for the latest release of {repo}"
            )
        return tag

    # Releases come back newest-first, so the first stable match wins.
    url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    logger.info(f"Resolving latest stable '{tag_prefix}' release of {repo} from {url}")
    req = Request(url, headers={"Accept": "application/vnd.github+json"})
    with urlopen(req, timeout=30) as resp:  # noqa: S310
        releases = json.load(resp)
    for release in releases:
        if release.get("draft") or release.get("prerelease"):
            continue
        tag = release.get("tag_name") or ""
        if tag.startswith(tag_prefix):
            return tag
    raise ValueError(
        f"No stable release with tag prefix '{tag_prefix}' found in {repo}. "
        "Pin an explicit jar version in full_transform_args.version or pass "
        "--version."
    )


def release_jar_url(repo: str, version: str, artifact: str) -> str:
    """Public, token-free download URL for a release asset."""
    return (
        f"https://github.com/{repo}/releases/download/"
        f"{version}/{asset_name(artifact, version)}"
    )


def fetch_release_jar(repo: str, version: str, artifact: str, cache_dir: str) -> str:
    """Download the release jar into ``cache_dir`` (skip if cached); return path."""
    if not version:
        raise ValueError(
            "full_transform_args.version is not set — pin a graphsense-spark "
            "release tag (e.g. 'v26.06.0') in config or pass --version."
        )
    name = asset_name(artifact, version)
    jar_dir = os.path.join(os.path.expanduser(cache_dir), "spark-jars")
    os.makedirs(jar_dir, exist_ok=True)
    dest = os.path.join(jar_dir, name)
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        logger.info(f"Using cached graphsense-spark jar {dest}")
        return dest

    url = release_jar_url(repo, version, artifact)
    logger.info(f"Downloading {url}")
    tmp = dest + ".part"
    try:
        with urlopen(url, timeout=60) as resp, open(tmp, "wb") as f:  # noqa: S310
            while chunk := resp.read(1 << 20):
                f.write(chunk)
        os.replace(tmp, dest)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    logger.info(f"Cached graphsense-spark jar at {dest}")
    return dest


def apply_sidecar(
    spark_props: Dict[str, str],
    packages: List[str],
    jar_args: List[str],
    *,
    contact_points: List[str],
    local_dc: Optional[str],
    consistency_level: str,
) -> Tuple[Dict[str, str], List[str], List[str]]:
    """Return (spark_props, packages, jar_args) augmented for the sidecar writer.

    Adds the analytics package, appends the SSTable-writer JVM flags (with the
    temp-dir redirected to spark.local.dir) to driver+executor extraJavaOptions,
    and appends the --writer/--sidecar-* job arguments. Inputs are not mutated.
    """
    if not contact_points:
        raise ValueError("sidecar.contact_points must be set when sidecar is enabled")
    local_dir = spark_props.get("spark.local.dir")
    if not local_dir:
        raise ValueError(
            "sidecar writer needs spark.local.dir set (in the spark_config "
            "profile) to redirect the SSTable/Vert.x temp dir off the root disk"
        )

    props = dict(spark_props)
    jvm = f"{_SIDECAR_MODULE_FLAGS} -Djava.io.tmpdir={local_dir} -Dvertx.cacheDirBase={local_dir}"
    for key in ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"):
        existing = props.get(key, "").strip()
        props[key] = f"{existing} {jvm}".strip() if existing else jvm

    pkgs = list(packages)
    if SIDECAR_PACKAGE not in pkgs:
        pkgs.append(SIDECAR_PACKAGE)

    args = list(jar_args) + [
        "--writer",
        "sidecar",
        "--sidecar-contact-points",
        ",".join(contact_points),
    ]
    if local_dc:
        args += ["--sidecar-local-dc", local_dc]
    args += ["--sidecar-consistency-level", consistency_level]
    return props, pkgs, args


def build_spark_submit(
    *,
    spark_home: Optional[str],
    jar_path: str,
    main_class: str,
    spark_props: Dict[str, str],
    packages: List[str],
    repositories: List[str],
    jar_args: List[str],
    extra_submit_args: List[str],
) -> List[str]:
    """Assemble the spark-submit argv (no execution)."""
    submit = (
        os.path.join(os.path.expanduser(spark_home), "bin", "spark-submit")
        if spark_home
        else "spark-submit"
    )
    cmd = [submit, "--class", main_class, "--verbose"]
    if packages:
        cmd += ["--packages", ",".join(packages)]
        if repositories:
            cmd += ["--repositories", ",".join(repositories)]
    for key, value in spark_props.items():
        cmd += ["--conf", f"{key}={value}"]
    cmd += list(extra_submit_args)
    cmd += [jar_path, *jar_args]
    return cmd


def run_spark_submit(cmd: List[str]) -> int:
    """Run spark-submit, streaming its output; return the exit code."""
    logger.info("spark-submit:\n  " + " \\\n  ".join(cmd))
    return subprocess.run(cmd, check=False).returncode  # noqa: S603
