import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import yaml

from ..utils import subkey_get
from ..utils.console import console
from ._provenance import iter_field_sources
from .config import get_config
from .settings import get_settings


@click.group()
def config_cli():
    pass


@config_cli.group("config")
@click.option(
    "--env",
    "-e",
    "env",
    default=None,
    help=(
        "Environment name. Picks the per-env overlay file "
        "(graphsense.<env>.yaml) next to the base YAML. Also recognized "
        "as GRAPHSENSE_ENV."
    ),
)
@click.pass_context
def config(ctx: click.Context, env: Optional[str]):
    """Inspect the current configuration of graphsenselib."""
    ctx.ensure_object(dict)
    ctx.obj["env"] = env

    # The main CLI entrypoint (cli/main.py → try_load_config) sniffs
    # --env from argv and builds the Settings singleton with the right
    # per-env overlay before any subcommand runs. But when this group
    # is invoked directly (tests via CliRunner, or tools that don't go
    # through main), try_load_config doesn't run — so we reload here if
    # --env was supplied and didn't already make it into the singleton.
    if env is not None:
        from .settings import Settings, get_settings, reset_settings, set_settings

        current = get_settings()
        if current.environment != env:
            reset_settings()
            s, _errs = Settings.try_load(env=env)
            if s is not None:
                set_settings(s)


@config.command("show")
@click.option("--json/--text", default=False)
@click.option(
    "--resolved",
    is_flag=True,
    default=False,
    help=(
        "Print the resolved Settings model as a flat table of "
        "(path, value), instead of the raw YAML."
    ),
)
@click.option(
    "--source",
    is_flag=True,
    default=False,
    help=(
        "With --resolved: include a 'source' column showing where each "
        "value came from (init, env, yaml:/path, dotenv, secrets, default)."
    ),
)
def show(json, resolved, source):
    """Prints the configuration used in the environment."""
    if resolved:
        _print_resolved_table(include_source=source)
        return

    cfg = get_config()
    if json:
        console.print_json(cfg.model_dump_json())
    else:
        console.print(cfg.text())


def _print_resolved_table(include_source: bool) -> None:
    """Render the new ``Settings`` model as a (path, value[, source]) table."""
    from rich.table import Table

    settings = get_settings()
    table = Table(show_header=True, header_style="bold")
    table.add_column("path", overflow="fold")
    table.add_column("value", overflow="fold")
    if include_source:
        table.add_column("source", overflow="fold")

    for path, value, label in sorted(iter_field_sources(settings)):
        if include_source:
            table.add_row(path, repr(value), label)
        else:
            table.add_row(path, repr(value))

    console.print(table)


# ---------------------------------------------------------------------------
# gs config dump — deterministic dump for before/after migration diff
# ---------------------------------------------------------------------------


def _canonical_dump(fmt: str) -> str:
    """Return a deterministic dump of the resolved Settings.

    Used for comparing configurations across migrations (env-var rename,
    monolithic → per-env YAML split, etc.). The goal is: if two runs
    resolve to the same effective values, their dumps are byte-identical.

    - Sorted keys top-to-bottom, nested recursively.
    - Excludes ``legacy_web_dict`` (migration-only back-compat mirror)
      so the output is the same whether the YAML used the legacy `web:`
      nested shape or the new flat layout.
    - ``None`` values are preserved (they can distinguish "unset" from
      "explicitly null").
    """
    settings = get_settings()
    data = settings.model_dump(mode="json", exclude={"legacy_web_dict"})
    if fmt == "yaml":
        return yaml.safe_dump(
            data, sort_keys=True, default_flow_style=False, allow_unicode=True
        )
    # JSON (default): stable indent, sorted keys, UTF-8 safe.
    return json.dumps(data, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


@config.command("dump")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "yaml"], case_sensitive=False),
    default="json",
    help="Output format. JSON is the default because it diffs more cleanly.",
)
@click.option(
    "--hash",
    "as_hash",
    is_flag=True,
    default=False,
    help=(
        "Print only a SHA-256 hex digest of the canonical dump. Useful "
        "for quick yes/no comparison ('same config?') without staring "
        "at a diff."
    ),
)
def dump(fmt: str, as_hash: bool):
    """Deterministic dump of the effective configuration.

    Run before and after a migration (env-var rename, monolithic YAML
    split, etc.) and compare the outputs:

    \b
        # Before
        graphsense-cli --env prod config dump > /tmp/before.json
        # Migrate...
        # After
        graphsense-cli --env prod config dump > /tmp/after.json
        diff /tmp/before.json /tmp/after.json   # expect: empty

    Or use --hash for a one-line check:

    \b
        graphsense-cli --env prod config dump --hash
        # → sha256:a3f5…

    The dump reflects the fully resolved Settings (all sources merged,
    defaults filled in) — not raw YAML — so it's stable across the
    YAML shape change.
    """
    body = _canonical_dump(fmt)
    if as_hash:
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        click.echo(f"sha256:{digest}")
        return
    click.echo(body, nl=False)


# ---------------------------------------------------------------------------
# gs config migrate — split a legacy monolithic YAML into the new shape
# ---------------------------------------------------------------------------


def _translate_legacy_env_block(env_block: Dict[str, Any]) -> Dict[str, Any]:
    """Translate one ``environments.<env>`` block to the new per-env shape.

    Legacy:
        cassandra_nodes: [...]
        username, password, readonly_username, readonly_password
        keyspaces: { ... }

    New (per-env overlay file):
        cassandra: { nodes: [...], username, password, ... }
        keyspaces: { ... }
    """
    out: Dict[str, Any] = {}
    cassandra: Dict[str, Any] = {}
    if "cassandra_nodes" in env_block:
        cassandra["nodes"] = env_block["cassandra_nodes"]
    for k in ("username", "password", "readonly_username", "readonly_password"):
        if k in env_block:
            cassandra[k] = env_block[k]
    if cassandra:
        out["cassandra"] = cassandra
    if "keyspaces" in env_block and isinstance(env_block["keyspaces"], dict):
        out["keyspaces"] = env_block["keyspaces"]
    return out


def _dedupe_environments(
    per_env: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """Partition per-env blocks into (common, remaining).

    Fields identical across *all* environments are lifted into the
    returned common dict — the caller merges them into the shared
    base. Only fields that actually differ (or are unique to one env)
    stay in the per-env dicts.

    Two granularity levels are checked:

    1. Top-level keys of each env block: if all envs carry the same
       value for a key, lift it wholesale.
    2. For nested dicts (``cassandra``, ``keyspaces``): compare
       sub-keys individually. Identical sub-keys lift; differing ones
       stay behind.

    The special ``environment`` marker is never lifted — it's the
    per-env identity.
    """
    if not per_env:
        return {}, {}

    env_names = list(per_env.keys())
    blocks = [per_env[n] for n in env_names]

    # All keys that appear anywhere.
    all_keys: set[str] = set()
    for b in blocks:
        all_keys.update(b.keys())
    all_keys.discard("environment")

    common: Dict[str, Any] = {}
    # Sub-field dedup happens separately; track which top-level keys
    # were handled there so we don't also do whole-value dedup on them.
    nested_dedupe_keys = ("cassandra", "keyspaces")

    for key in all_keys:
        if key in nested_dedupe_keys:
            continue
        values = [b.get(key) for b in blocks]
        if all(v is not None for v in values) and all(v == values[0] for v in values):
            common[key] = values[0]

    # For nested dicts, dedupe sub-keys.
    for nkey in nested_dedupe_keys:
        raw_subs = [b.get(nkey) for b in blocks]
        if not all(isinstance(sb, dict) for sb in raw_subs):
            continue
        sub_blocks: List[Dict[str, Any]] = [
            sb for sb in raw_subs if isinstance(sb, dict)
        ]
        shared_sub_keys: set[str] = set(sub_blocks[0].keys())
        for sb in sub_blocks[1:]:
            shared_sub_keys &= set(sb.keys())
        sub_common: Dict[str, Any] = {}
        for sk in shared_sub_keys:
            values = [sb[sk] for sb in sub_blocks]
            if all(v == values[0] for v in values):
                sub_common[sk] = values[0]
        if sub_common:
            common[nkey] = sub_common

    # Build the remaining per-env blocks with common fields removed.
    remaining: Dict[str, Dict[str, Any]] = {}
    for name, block in per_env.items():
        trimmed: Dict[str, Any] = {}
        for k, v in block.items():
            if k == "environment":
                # Only keep the env-name marker if the per-env block has
                # other content; otherwise we'll drop the file entirely.
                trimmed[k] = v
                continue
            if k in nested_dedupe_keys and isinstance(v, dict) and k in common:
                # Drop sub-keys already lifted to common.
                lifted = common[k]
                sub_remaining = {sk: sv for sk, sv in v.items() if sk not in lifted}
                if sub_remaining:
                    trimmed[k] = sub_remaining
                continue
            if k in common and not isinstance(v, dict):
                continue  # scalar lifted whole
            trimmed[k] = v
        remaining[name] = trimmed

    return common, remaining


def _merge_common_into_shared(
    shared: Dict[str, Any], common: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge lifted common fields into the shared base.

    For nested dicts (``cassandra``, ``keyspaces``), the common dict's
    sub-keys are merged into any already-present shared block rather
    than overwriting it. Example: root-level ``cassandra`` in shared
    already holds ``nodes`` (hoisted from ``web.database``); common
    holds ``username`` / ``password`` lifted from identical env creds
    — the result should carry both.
    """
    out = dict(shared)
    for key, value in common.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            merged = dict(value)
            merged.update(out[key])  # existing shared wins over common
            out[key] = merged
        else:
            out.setdefault(key, value)
    return out


def _split_legacy_yaml(
    raw: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[str]]:
    """Split a legacy monolithic YAML into (shared_base, per_env_dict, warnings).

    - ``shared_base``: top-level keys that apply across envs. Legacy
      ``web.database`` is hoisted to root-level ``cassandra``; legacy
      ``web.gs-tagstore`` is hoisted to root-level ``tagstore``.
    - ``per_env_dict``: one ``{env_name: per_env_yaml}`` entry per entry
      in the legacy ``environments:`` block.
    - ``warnings``: human-readable notes about fields that need manual
      review (e.g. a cluster mismatch between ``web.database`` and a
      per-env ``cassandra_nodes``).
    """
    warnings_out: List[str] = []
    shared: Dict[str, Any] = {}
    per_env: Dict[str, Dict[str, Any]] = {}

    # 1. Pass through top-level keys that already live at root in the
    # new model.
    passthrough = (
        "slack_topics",
        "cache_directory",
        "coingecko_api_key",
        "coinmarketcap_api_key",
        "s3_credentials",
        "use_redis_locks",
        "redis_url",
        "mcp",
        "tagstore",  # already new shape
        "cassandra",  # already new shape
        "keyspaces",  # already new shape (unusual at root in legacy, but accept)
    )
    for key in passthrough:
        if key in raw:
            shared[key] = raw[key]

    # 2. Legacy ``web:`` block. Strip ``database`` and ``gs-tagstore``
    # (they hoist to root); keep the rest under ``web:``.
    legacy_web = raw.get("web")
    if isinstance(legacy_web, dict):
        web_scrubbed: Dict[str, Any] = {}
        for k, v in legacy_web.items():
            if k == "database":
                # Hoist REST's Cassandra cluster to root.
                existing_cas = shared.get("cassandra")
                if isinstance(existing_cas, dict) and existing_cas != v:
                    warnings_out.append(
                        "Both `cassandra:` at root and `web.database:` exist "
                        "and differ. Keeping root `cassandra:`; `web.database:` "
                        "is discarded. Review if REST should point at a "
                        "different cluster than ingest."
                    )
                else:
                    shared["cassandra"] = v
            elif k == "gs-tagstore":
                existing_ts = shared.get("tagstore")
                if isinstance(existing_ts, dict) and existing_ts != v:
                    warnings_out.append(
                        "Both `tagstore:` at root and `web.gs-tagstore:` exist "
                        "and differ. Keeping root `tagstore:`."
                    )
                else:
                    shared["tagstore"] = v
            else:
                web_scrubbed[k] = v
        if web_scrubbed:
            shared["web"] = web_scrubbed

    # 3. Per-env split.
    envs = raw.get("environments")
    if isinstance(envs, dict):
        for env_name, env_block in envs.items():
            if not isinstance(env_block, dict):
                continue
            translated = _translate_legacy_env_block(env_block)
            translated["environment"] = env_name
            per_env[env_name] = translated

            # Conflict check: shared root cassandra vs per-env cassandra_nodes.
            shared_cas = shared.get("cassandra")
            if (
                isinstance(shared_cas, dict)
                and "nodes" in shared_cas
                and "cassandra_nodes" in env_block
                and shared_cas["nodes"] != env_block["cassandra_nodes"]
            ):
                warnings_out.append(
                    f"Env `{env_name}` has `cassandra_nodes` different from "
                    "the shared root `cassandra.nodes` (hoisted from "
                    "`web.database`). The per-env overlay will win at "
                    "runtime. If REST and ingest need different clusters, "
                    "point them at different config files via "
                    "`GRAPHSENSE_CONFIG_YAML`."
                )

    # 4. Dedupe: lift fields identical across all envs into shared, and
    # drop any per-env block that ends up substantive-field-empty (only
    # the `environment` marker left). The `--env` flag still propagates
    # the env name at load time, so skipping an empty overlay file is
    # safe.
    common, remaining = _dedupe_environments(per_env)
    if common:
        shared = _merge_common_into_shared(shared, common)

    non_empty: Dict[str, Dict[str, Any]] = {}
    for name, block in remaining.items():
        substantive = {k: v for k, v in block.items() if k != "environment"}
        if substantive:
            non_empty[name] = block

    return shared, non_empty, warnings_out


def _dump_yaml(data: Dict[str, Any]) -> str:
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)


@config.command("migrate")
@click.option(
    "--in",
    "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Legacy YAML to migrate. Defaults to the currently resolved config file.",
)
@click.option(
    "--out-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Write graphsense.yaml + graphsense.<env>.yaml into this directory. "
    "Without this flag, the translated files are printed to stdout with "
    "delimiters.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="With --out-dir, allow overwriting existing files (default: refuse).",
)
def migrate(in_path: Optional[Path], out_dir: Optional[Path], overwrite: bool):
    """Translate a legacy monolithic YAML into the new layered shape.

    Produces:

    \b
    - graphsense.yaml          — shared across environments
    - graphsense.<env>.yaml    — one per entry in the legacy `environments:`
                                 block

    Fields that need manual review (e.g. a REST/ingest cluster mismatch)
    are printed to stderr as warnings. No data is dropped; the legacy
    file is read-only.
    """
    # Resolve input file.
    if in_path is None:
        legacy = get_config()
        resolved = legacy.path() if legacy.is_loaded() else None
        if resolved is None:
            raise click.UsageError(
                "No config file found. Pass --in FILE, or set "
                "GRAPHSENSE_CONFIG_YAML / place a .graphsense.yaml."
            )
        in_path = Path(resolved)

    with in_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise click.UsageError(
            f"{in_path} did not parse to a mapping; nothing to migrate."
        )

    shared, per_env, warnings_out = _split_legacy_yaml(raw)

    for w in warnings_out:
        click.echo(f"warning: {w}", err=True)

    if out_dir is None:
        # Print to stdout with delimiters.
        click.echo("# ===== graphsense.yaml =====")
        click.echo(_dump_yaml(shared), nl=False)
        for env_name, env_data in per_env.items():
            click.echo(f"\n# ===== graphsense.{env_name}.yaml =====")
            click.echo(_dump_yaml(env_data), nl=False)
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    targets: List[Tuple[Path, Dict[str, Any]]] = [(out_dir / "graphsense.yaml", shared)]
    for env_name, env_data in per_env.items():
        targets.append((out_dir / f"graphsense.{env_name}.yaml", env_data))

    if not overwrite:
        existing = [p for p, _ in targets if p.exists()]
        if existing:
            raise click.UsageError(
                "Refusing to overwrite existing file(s): "
                + ", ".join(str(p) for p in existing)
                + " — pass --overwrite to proceed."
            )

    for path, data in targets:
        path.write_text(_dump_yaml(data), encoding="utf-8")
        click.echo(f"wrote {path}")


@config.command("get")
@click.option(
    "--path",
    help="path in the config file sep. is a dot (.)",
    type=str,
    required=True,
    default=False,
)
def get(path):
    """Prints the configuration used in the environment."""
    cfg = get_config()
    console.print(subkey_get(cfg.model_dump(), path.split(".")))


@config.command("path")
def path():
    """Prints the path where the config is loaded from."""
    cfg = get_config()
    console.print(cfg.path())


@config.command("template")
def default():
    """Generates a configuration template."""
    cfg = get_config()
    console.print(cfg.generate_yaml(DEBUG=False))
