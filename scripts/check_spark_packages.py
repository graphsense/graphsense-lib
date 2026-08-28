# ruff: noqa: T201
"""Keep the Maven coordinates in config.py in sync with spark/build.sbt.

``DEFAULT_SCALA_JOB_PACKAGES`` mirrors the compile-scope dependencies of the
Scala build so the ``slim`` artifact can pass them to ``spark-submit
--packages``. Nothing enforced that mirroring, so the two drifted silently
whenever build.sbt changed -- and the failure is invisible until a slim run
hits a NoClassDefFoundError on a cluster.

This resolves build.sbt as the single source of truth: ``--check`` fails when
the two disagree, ``--fix`` rewrites the Python lists. Wired into the Makefile
and pre-commit the same way ``check-api-version`` is.

Only ``spark/build.sbt`` is parsed -- deliberately, not a full sbt resolve.
``sbt`` is not available in pre-commit or in the lint job, and the declaration
block is a flat literal list that a regex reads reliably. If that stops being
true this script should start failing loudly rather than guessing, which is
what STRICT_BLOCK below is for.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_SBT = REPO_ROOT / "spark" / "build.sbt"
CONFIG_PY = REPO_ROOT / "src" / "graphsenselib" / "config" / "config.py"

# A dependency line: "group" % "artifact" % "version" [% Scope] [exclude(...)]
DEP_RE = re.compile(
    r'"(?P<group>[^"]+)"\s*(?P<sep>%%?)\s*"(?P<artifact>[^"]+)"\s*%\s*"(?P<version>[^"]+)"'
    # To end of line, NOT to the next comma: an `exclude("g", "a")` call
    # contains one, and stopping there silently drops every exclusion.
    r"(?P<rest>[^\n]*)"
)
EXCLUDE_RE = re.compile(
    r'exclude\s*\(\s*"(?P<group>[^"]+)"\s*,\s*"(?P<artifact>[^"]+)"\s*\)'
)
SCALA_VERSION_RE = re.compile(
    r'ThisBuild\s*/\s*scalaVersion\s*:=\s*"(\d+)\.(\d+)\.\d+"'
)
STRICT_BLOCK = "libraryDependencies ++= Seq("


def _strip_comments(text: str) -> str:
    return "\n".join(line.split("//")[0] for line in text.splitlines())


def parse_build_sbt() -> Tuple[List[str], List[str]]:
    """Return (compile-scope maven coordinates, excluded ``group:artifact``)."""
    raw = BUILD_SBT.read_text(encoding="utf-8")

    m = SCALA_VERSION_RE.search(raw)
    if not m:
        raise SystemExit(f"Could not read scalaVersion from {BUILD_SBT}")
    binary_version = f"{m.group(1)}.{m.group(2)}"

    start = raw.find(STRICT_BLOCK)
    if start == -1:
        raise SystemExit(
            f"Could not find '{STRICT_BLOCK}' in {BUILD_SBT}. The declaration "
            "shape changed; update scripts/check_spark_packages.py rather than "
            "letting the lists drift."
        )
    depth, end = 0, None
    for i in range(start + len(STRICT_BLOCK) - 1, len(raw)):
        if raw[i] == "(":
            depth += 1
        elif raw[i] == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        raise SystemExit(f"Unbalanced parentheses in the {STRICT_BLOCK} block")

    block = _strip_comments(raw[start : end + 1])

    packages: List[str] = []
    excludes: List[str] = []
    for dep in DEP_RE.finditer(block):
        rest = dep.group("rest")
        # Provided is supplied by the cluster; Test never reaches a job.
        if re.search(r"%\s*(Provided|Test)\b", rest):
            continue
        artifact = dep.group("artifact")
        if dep.group("sep") == "%%":
            artifact = f"{artifact}_{binary_version}"
        packages.append(f"{dep.group('group')}:{artifact}:{dep.group('version')}")
        for exc in EXCLUDE_RE.finditer(rest):
            excludes.append(f"{exc.group('group')}:{exc.group('artifact')}")
    return packages, sorted(set(excludes))


def _list_literal(name: str, values: List[str]) -> str:
    body = "".join(f'    "{v}",\n' for v in values)
    return f"{name} = [\n{body}]"


def read_config_list(name: str) -> List[str]:
    text = CONFIG_PY.read_text(encoding="utf-8")
    m = re.search(rf"^{name} = \[\n(.*?)^\]", text, re.S | re.M)
    if not m:
        raise SystemExit(f"Could not find {name} in {CONFIG_PY}")
    return re.findall(r'"([^"]+)"', m.group(1))


def write_config_list(name: str, values: List[str]) -> None:
    text = CONFIG_PY.read_text(encoding="utf-8")
    new = re.sub(
        rf"^{name} = \[\n.*?^\]", _list_literal(name, values), text, flags=re.S | re.M
    )
    CONFIG_PY.write_text(new, encoding="utf-8")


PAIRS = [
    ("DEFAULT_SCALA_JOB_PACKAGES", 0),
    ("DEFAULT_SCALA_JOB_EXCLUDES", 1),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fix", action="store_true", help="rewrite config.py to match")
    args = ap.parse_args()

    expected = parse_build_sbt()
    failed = False
    for name, idx in PAIRS:
        want = expected[idx]
        have = read_config_list(name)
        if want == have:
            print(f"{name}: in sync ({len(want)} entries)")
            continue
        if args.fix:
            write_config_list(name, want)
            print(f"{name}: rewritten from spark/build.sbt")
            continue
        failed = True
        print(f"{name} is out of sync with spark/build.sbt:")
        for missing in [c for c in want if c not in have]:
            print(f"  + {missing}")
        for extra in [c for c in have if c not in want]:
            print(f"  - {extra}")
    if failed:
        print("\nRun 'make sync-spark-packages' to fix.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
