#!/usr/bin/env python
# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml>=6"]
# ///
# ruff: noqa: T201
"""Repair mis-encoded text (mojibake) in tagpack / actorpack YAML files.

Why this exists
---------------
Some imported labels are UTF-8 that was decoded as Latin-1 or cp1252 once,
e.g. ``Ð\\x9bÐ\\x98Ð¤Ð¨Ð\\x98Ð¦`` for ``ЛИФШИЦ`` or ``MÃ¼nchen`` for
``München``. graphsense-lib's tagpack validation now rejects such text
(``tagpack.tagpack.text_problem``), so these packs have to be fixed at the
source before they validate or insert again.

What it does
------------
Each run of characters that can be mojibake (U+0080-U+00FF, the cp1252
punctuation, and ``\\xNN`` / ``\\u00NN`` escapes for U+0080-U+00FF inside
double-quoted scalars) is turned back into bytes and decoded as UTF-8. Only
runs that decode, and decode to something different, are replaced, so
correct text such as ``Zürich`` or ``Café`` is left alone. The repair is a
text edit: comments, key order, quoting and ``!include`` headers stay as they
are.

Every changed file is parsed before and after; it is written only if the
structure is identical and only strings changed. Strings that still fail the
check afterwards (control characters that are not mojibake, mojibake split
across a folded line) are listed for a manual fix.

Usage
-----
    # report only (default): what would change, what is left
    uv run scripts/one-off-fixes/fix_tagpack_mojibake.py check PATH...
    # write the fixes
    uv run scripts/one-off-fixes/fix_tagpack_mojibake.py apply PATH...

PATH is a YAML file or a directory (searched recursively for *.yaml/*.yml).
Review the result with ``git diff`` in the tagpacks repository, then run
``graphsense-cli tagpack-tool tagpack validate`` on the changed packs.
"""

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import yaml

_CP1252_SPECIALS = "€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ"
# A run of characters that can be part of mojibake, or YAML escapes of
# U+0080-U+00FF inside double quotes: PyYAML writes C1 characters as \x9B,
# U+0085 as \N and U+00A0 as \_.
_RUN = re.compile(
    "(?:[\\u0080-\\u00ff"
    + re.escape(_CP1252_SPECIALS)
    + r"]|\\x[89a-fA-F][0-9a-fA-F]|\\u00[89a-fA-F][0-9a-fA-F]|\\N|\\_)+"
)
_ESCAPE = re.compile(r"\\x([0-9a-fA-F]{2})|\\u00([0-9a-fA-F]{2})|\\([N_])")
_NAMED_ESCAPES = {"N": 0x85, "_": 0xA0}
_ALLOWED_CONTROL = frozenset("\t\n\r")


# --- same rules as graphsenselib.tagpack.tagpack -------------------------


def repair_mojibake(text: str) -> Optional[str]:
    if text.isascii():
        return None
    fixed, _ = repair_text(text)
    return fixed if fixed != text else None


def text_problem(text: str) -> Optional[str]:
    fixed = repair_mojibake(text)
    if fixed is not None:
        return f"mis-encoded, probably {fixed!r}"
    bad = sorted(
        {
            c
            for c in text
            if unicodedata.category(c) == "Cc" and c not in _ALLOWED_CONTROL
        }
    )
    if bad:
        return "control characters " + ", ".join(f"U+{ord(c):04X}" for c in bad)
    return None


# --- repair ---------------------------------------------------------------


def _utf8_sequence_length(lead: int) -> int:
    if 0xC2 <= lead <= 0xDF:
        return 2
    if 0xE0 <= lead <= 0xEF:
        return 3
    if 0xF0 <= lead <= 0xF4:
        return 4
    return 0


def _decode_pieces(pieces: List[Tuple[int, str]]) -> str:
    """Every valid UTF-8 sequence becomes its character; other bytes keep
    their original text (same as graphsenselib's decode_mojibake_pieces)."""
    out = []
    left = []
    i = 0
    while i < len(pieces):
        n = _utf8_sequence_length(pieces[i][0])
        if n and i + n <= len(pieces):
            try:
                out.append(bytes(b for b, _ in pieces[i : i + n]).decode("utf-8"))
                i += n
                continue
            except UnicodeDecodeError:
                pass
        out.append(pieces[i][1])
        left.append(pieces[i][0])
        i += 1
    # A partial decode is only trusted if what is left looks like real text:
    # leftover cp1252 punctuation or C1 bytes mean the run was garbled some
    # other way (e.g. Mac Roman), and "repairing" it would garble it further.
    if any(0x80 <= b <= 0x9F for b in left):
        return "".join(t for _, t in pieces)
    return "".join(out)


def _run_pieces(run: str) -> Optional[List[Tuple[int, str]]]:
    """[(byte, source text)] for a run; escapes are one byte each."""
    pieces = []
    pos = 0
    for m in _ESCAPE.finditer(run):
        head = _char_pieces(run[pos : m.start()])
        if head is None:
            return None
        pieces += head
        if m.group(3):
            pieces.append((_NAMED_ESCAPES[m.group(3)], m.group(0)))
        else:
            pieces.append((int(m.group(1) or m.group(2), 16), m.group(0)))
        pos = m.end()
    tail = _char_pieces(run[pos:])
    if tail is None:
        return None
    return pieces + tail


def _char_pieces(chars: str) -> Optional[List[Tuple[int, str]]]:
    out = []
    for c in chars:
        if ord(c) < 256:
            out.append((ord(c), c))
        else:
            try:
                out.append((c.encode("cp1252")[0], c))
            except UnicodeError:
                return None
    return out


def repair_text(source: str) -> Tuple[str, List[Tuple[str, str]]]:
    """``source`` with every repairable run replaced, plus (old, new) pairs."""
    changes = []

    def fix(m):
        run = m.group(0)
        pieces = _run_pieces(run)
        if pieces is None:
            return run
        fixed = _decode_pieces(pieces)
        if fixed == run or any(
            unicodedata.category(c) == "Cc" and c not in _ALLOWED_CONTROL for c in fixed
        ):
            return run
        changes.append((run, fixed))
        return fixed

    return _RUN.sub(fix, source), changes


# --- verification -----------------------------------------------------------


class _Loader(yaml.SafeLoader):
    pass


def _keep_tag(loader, suffix, node):
    # !include and other application tags: keep the raw value, no resolving
    if isinstance(node, yaml.ScalarNode):
        return f"!{suffix} {loader.construct_scalar(node)}"
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    return loader.construct_mapping(node, deep=True)


_Loader.add_multi_constructor("!", _keep_tag)


def _strings(value, path="") -> Iterator[Tuple[str, str]]:
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for k, v in value.items():
            yield from _strings(k, f"{path}/<key>")
            yield from _strings(v, f"{path}/{k}")
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from _strings(v, f"{path}[{i}]")


def _same_structure(a, b) -> bool:
    if isinstance(a, str) and isinstance(b, str):
        return True
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return len(a) == len(b) and all(
            _same_structure(ka, kb) and _same_structure(a[ka], b[kb])
            for ka, kb in zip(a, b)
        )
    if isinstance(a, list):
        return len(a) == len(b) and all(_same_structure(x, y) for x, y in zip(a, b))
    return a == b


def _line_of(source: str, needle: str) -> Optional[int]:
    for i, line in enumerate(source.splitlines(), 1):
        if needle and needle[:40] in line:
            return i
    return None


# --- driver ---------------------------------------------------------------


def _yaml_files(paths: List[str]) -> Iterator[Path]:
    for p in map(Path, paths):
        if p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.suffix in (".yaml", ".yml") and f.is_file():
                    yield f
        elif p.is_file():
            yield p
        else:
            print(f"not found: {p}", file=sys.stderr)


def process(path: Path, apply: bool) -> Tuple[int, int, bool]:
    """(runs fixed, problems left, file ok). Prints its own report."""
    source = path.read_text(encoding="utf-8")
    fixed_source, changes = repair_text(source)

    try:
        after = yaml.load(fixed_source, _Loader)
    except yaml.YAMLError as e:
        print(f"{path}: cannot parse after repair ({e}); not touched")
        return 0, 0, False
    try:
        before = yaml.load(source, _Loader)
    except yaml.YAMLError:
        # Raw C1 characters are not allowed in YAML at all (the fast loader
        # used on insert accepts them, PyYAML does not). The edit only
        # touched mojibake runs and the result parses; that has to do.
        before = None
        if changes:
            print(f"{path}: not valid YAML before repair (raw control characters)")

    if changes and before is not None and not _same_structure(before, after):
        print(f"{path}: repair would change the structure; not touched")
        return 0, 0, False

    left = [
        (where, s, problem)
        for where, s in _strings(after)
        if (problem := text_problem(s)) is not None
    ]

    if changes:
        verb = "fixed" if apply else "would fix"
        print(f"{path}: {verb} {len(changes)} run(s)")
        seen = set()
        for old, new in changes:
            if (old, new) in seen:
                continue
            seen.add((old, new))
            if len(seen) > 5:
                print(f"    … {len(set(changes)) - 5} more distinct")
                break
            print(f"    {old!r} -> {new!r}")
        if apply:
            path.write_text(fixed_source, encoding="utf-8")
    for where, s, problem in left:
        line = _line_of(fixed_source, s.split("\n")[0])
        loc = f"{path}:{line}" if line else str(path)
        print(f"  LEFT {loc} {where}: {problem}: {s[:80]!r}")
    return len(changes), len(left), True


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("mode", choices=["check", "apply"])
    ap.add_argument("paths", nargs="+")
    args = ap.parse_args(argv)

    n_files = n_runs = n_left = n_bad = 0
    for f in _yaml_files(args.paths):
        runs, left, ok = process(f, args.mode == "apply")
        n_files += 1
        n_runs += runs
        n_left += left
        n_bad += not ok
    verb = "fixed" if args.mode == "apply" else "fixable"
    print(
        f"\n{n_files} files scanned: {n_runs} runs {verb}, "
        f"{n_left} strings need a manual fix, {n_bad} files not touched"
    )
    return 1 if (n_left or n_bad) else 0


if __name__ == "__main__":
    sys.exit(main())
