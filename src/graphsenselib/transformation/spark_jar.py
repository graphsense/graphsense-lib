"""Fetch graphsense-spark release jars and build the spark-submit command.

This drives the external Scala graphsense-spark job for the raw -> transformed
"full transform". The jar is downloaded from a public GitHub Release asset
(token-free) and cached; the job is then launched via spark-submit.
"""

import logging
import os
import subprocess
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen

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


def asset_name(artifact: str, version: str) -> str:
    """Release asset filename for an artifact + version.

    The release tag keeps a leading ``v`` but the jar filename does not, e.g.
    tag ``v26.06.0`` -> ``graphsense-spark-assembly-26.06.0.jar``.
    """
    v = version.lstrip("v")
    if artifact == "fat":
        return f"graphsense-spark-assembly-{v}.jar"
    if artifact == "slim":
        return f"graphsense-spark_2.12-{v}.jar"
    raise ValueError(f"Unknown artifact '{artifact}' (expected 'fat' or 'slim')")


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
