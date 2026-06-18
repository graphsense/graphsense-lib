#!/usr/bin/env bash
#
# Pack a Python environment into a relocatable tarball for Spark `spark.archives`,
# so remote executors can run Python UDFs that import graphsenselib (or any other
# package with NATIVE deps, e.g. coincurve). `--py-files` does NOT suffice for
# native/compiled extensions — you must ship a whole interpreter + site-packages.
#
# Works with Spark >= 3.1 (spark.archives), including the 3.5.8 cluster.
#
# This is a GENERAL tool (not pubkey-specific): it packs whatever you pip-install
# into a fresh venv and emits the spark_config to use it.
#
# WHERE TO RUN THIS — read this or you will waste an afternoon:
#   The packed env embeds a Python interpreter + compiled .so files. They must be
#   binary-compatible with the EXECUTOR hosts (same OS / glibc / CPU arch / Python
#   minor version). Build it INSIDE the same image/base your Spark workers run, or
#   on a host identical to them. A mismatch shows up as ImportError on the
#   executor (often on coincurve / pyarrow), not here.
#
# Usage (env-var driven):
#   PACKAGES="graphsenselib[all,transformation]" ./scripts/pack_spark_env.sh
#   # install from a local wheel / checkout instead of PyPI (dev image builds):
#   PACKAGES="/tmp/wheels/graphsense_lib-*.whl[all,transformation]" \
#     ./scripts/pack_spark_env.sh
#   PACKAGES=".[all,transformation]" ./scripts/pack_spark_env.sh   # from a checkout
#
# Env vars:
#   PACKAGES        pip install spec(s); default graphsenselib[all,transformation]
#   PYTHON          base interpreter (MUST match executors); default python3
#   OUT             output tarball; default ./spark-env.tar.gz
#   ENV_NAME        archive alias / on-executor dir name; default 'environment'
#   EXCLUDE_PYSPARK drop pyspark+py4j from the pack so they don't clash with the
#                   cluster's own Spark python; default 1 (set 0 to keep)
#   PIP_ARGS        extra pip args, e.g. "--find-links /tmp/wheels"
#   KEEP_BUILD      keep the temp build venv for inspection; default 0
#
set -euo pipefail

PACKAGES="${PACKAGES:-graphsenselib[all,transformation]}"
PYTHON="${PYTHON:-python3}"
OUT="${OUT:-./spark-env.tar.gz}"
ENV_NAME="${ENV_NAME:-environment}"
EXCLUDE_PYSPARK="${EXCLUDE_PYSPARK:-1}"
PIP_ARGS="${PIP_ARGS:-}"
KEEP_BUILD="${KEEP_BUILD:-0}"

command -v "$PYTHON" >/dev/null || { echo "ERROR: '$PYTHON' not found" >&2; exit 1; }
PYTAG="$("$PYTHON" -c 'import sys; print(f"python{sys.version_info.major}.{sys.version_info.minor}")')"
echo ">>> base interpreter: $PYTHON ($("$PYTHON" --version 2>&1), tag $PYTAG)"
echo ">>> packages        : $PACKAGES"

BUILD="$(mktemp -d)"
VENV="$BUILD/$ENV_NAME"
cleanup() { [[ "$KEEP_BUILD" == "1" ]] || rm -rf "$BUILD"; }
trap cleanup EXIT

# --copies => the interpreter is copied, not symlinked, so the venv is portable.
echo ">>> creating relocatable venv at $VENV"
"$PYTHON" -m venv --copies "$VENV"
"$VENV/bin/python" -m pip install --quiet --upgrade pip

echo ">>> installing packages into the venv"
# shellcheck disable=SC2086  # PACKAGES / PIP_ARGS are intentionally word-split
"$VENV/bin/python" -m pip install $PIP_ARGS $PACKAGES

if [[ "$EXCLUDE_PYSPARK" == "1" ]]; then
  echo ">>> removing pyspark/py4j from the pack (executors use the cluster's Spark)"
  rm -rf "$VENV/lib/$PYTAG/site-packages/"pyspark* \
         "$VENV/lib/$PYTAG/site-packages/"py4j* 2>/dev/null || true
fi

# venv-pack makes the venv relocatable (rewrites shebangs / absolute paths) and
# tars it. Install it with the base interpreter so it is not part of the pack.
echo ">>> packing with venv-pack -> $OUT"
"$PYTHON" -m pip install --quiet venv-pack
"$PYTHON" -m venv_pack -p "$VENV" -o "$OUT" --force

SIZE="$(du -h "$OUT" | cut -f1)"
echo
echo ">>> wrote $OUT ($SIZE)"
echo
echo "Add to spark_config in graphsense.yaml (or pass via the job's spark_config):"
echo "-------------------------------------------------------------------------"
cat <<YAML
spark_config:
  spark.archives: "$(readlink -f "$OUT")#${ENV_NAME}"
  spark.pyspark.python: "./${ENV_NAME}/bin/python"          # executors: the shipped env
  spark.pyspark.driver.python: "$("$PYTHON" -c 'import sys; print(sys.executable)')"  # driver: this interpreter
YAML
echo "-------------------------------------------------------------------------"
echo "Notes:"
echo "  * The tarball path must be reachable by the DRIVER at submit time (Spark"
echo "    uploads it to executors). A local path on the driver host is fine."
echo "  * Spark unpacks it to ./${ENV_NAME}/ in each executor working dir."
echo "  * Verify on an executor host (same platform) before a big run:"
echo "      tar xzf $OUT -C /tmp/_t && /tmp/_t/bin/python -c 'import graphsenselib, coincurve; print(\"ok\")'"
