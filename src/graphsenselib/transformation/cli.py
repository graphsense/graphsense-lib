"""CLI for PySpark-based Delta Lake → Cassandra transformation."""

import logging

import click
import pyspark  # noqa: F401 — trigger ImportError early if pyspark not installed

from graphsenselib.cli.common import (
    require_currency,
    require_environment,
    spark_profile_option,
)
from graphsenselib.schema import GraphsenseSchemas

logger = logging.getLogger(__name__)

ALPHA_WARNING = (
    "ALPHA command — the interface and behaviour may change and it is not yet "
    "validated in production. Run against isolated keyspaces/paths only."
)


def _warn_alpha(command: str) -> None:
    """Emit a visible alpha warning to stderr when an alpha command is invoked."""
    click.secho(f"⚠  {command}: {ALPHA_WARNING}", fg="yellow", err=True)


@click.group()
def transformation_cli():
    pass


@transformation_cli.group("transformation")
def transformation():
    """Transform Delta Lake tables into Cassandra raw keyspace via PySpark."""
    pass


def _log_startup_banner(
    *,
    env,
    currency,
    delta_lake_path,
    s3_credentials,
    raw_keyspace,
    raw_keyspace_overridden,
    cassandra_nodes,
    start_block,
    end_block,
    top_block,
    local,
    patch=False,
):
    from urllib.parse import urlparse

    from graphsenselib.config import currency_to_schema_type

    # Parse bucket / scheme from delta_lake_path. s3://bucket/path → bucket.
    parsed = urlparse(delta_lake_path)
    if parsed.scheme in ("s3", "s3a"):
        bucket = parsed.netloc or "?"
        delta_loc = f"{parsed.scheme}://{bucket}{parsed.path}"
    else:
        bucket = None
        delta_loc = delta_lake_path
    s3_endpoint = (s3_credentials or {}).get("AWS_ENDPOINT_URL")

    keyspace_label = raw_keyspace + ("  (override)" if raw_keyspace_overridden else "")
    schema_type = currency_to_schema_type.get(currency, "?")

    lines = [
        "=" * 72,
        "PySpark Delta Lake -> Cassandra raw transformation",
        "=" * 72,
        f"  env              : {env}",
        f"  currency         : {currency}  (schema={schema_type})",
        f"  source delta     : {delta_loc}",
    ]
    if bucket is not None:
        lines.append(f"  s3 bucket        : {bucket}")
    if s3_endpoint:
        lines.append(f"  s3 endpoint      : {s3_endpoint}")
    lines += [
        f"  target keyspace  : {keyspace_label}",
        f"  cassandra nodes  : {', '.join(cassandra_nodes)}",
        f"  start block      : {start_block}",
        f"  end block        : {end_block}",
        f"  top block        : {top_block}",
        f"  spark mode       : {'local[*]' if local else 'cluster'}",
        f"  patch mode       : {'on' if patch else 'off'}",
        "=" * 72,
    ]
    logger.info("\n" + "\n".join(lines))


@transformation.command(
    "delta-to-raw",
    short_help="Load the Cassandra raw keyspace from Delta Lake (delta → raw).",
)
@require_environment()
@require_currency()
@click.option(
    "--start-block",
    type=int,
    default=0,
    show_default=True,
    help="Start block (inclusive).",
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help="End block (inclusive). If omitted, auto-detected from Delta Lake.",
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create Cassandra keyspace/tables if they do not exist.",
)
@click.option(
    "--raw-keyspace",
    "raw_keyspace_override",
    type=str,
    default=None,
    help=(
        "Override the target raw Cassandra keyspace name (default: from "
        "graphsense.yaml). Use to write into a fresh/dated keyspace while "
        "continuous ingest and delta-update keep using the YAML name."
    ),
)
@click.option(
    "--delta-lake-path",
    type=str,
    default=None,
    help="Override Delta Lake base path (default: from config).",
)
@click.option(
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help=(
        "Name of the s3_configs entry to use for S3/MinIO credentials. "
        "Required when the Delta Lake path is on s3://."
    ),
)
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode with local[*].",
)
@click.option(
    "--debug-write-audit",
    is_flag=True,
    help=(
        "Before each Cassandra write, run an extra aggregation that logs "
        "per-Spark-partition row counts and partition-key skew. Use to "
        "diagnose stragglers. Adds one shuffle per write."
    ),
)
@click.option(
    "--patch",
    is_flag=True,
    help=(
        "Allow writing into a non-empty target keyspace. Existing rows in the "
        "[start-block, end-block] range are overwritten by PK upsert; rows "
        "outside the range are untouched. Account chains only (eth, trx); "
        "rejected for UTXO chains because their derived spend tables are not "
        "window-local."
    ),
)
@spark_profile_option
def run_transformation(
    env,
    currency,
    start_block,
    end_block,
    create_schema,
    raw_keyspace_override,
    delta_lake_path,
    s3_config_name,
    local,
    debug_write_audit,
    patch,
    spark_profile,
):
    """Run PySpark transformation from Delta Lake to Cassandra raw keyspace.

    For cluster mode, Spark workers must have Python >= 3.10 (matching the driver).
    Install via: uv python install 3.11 on each worker node, then set
    spark.pyspark.python in spark_config.
    \f
    """
    from graphsenselib.config import currency_to_schema_type, get_config

    config = get_config()
    env_config = config.get_environment(env)
    ks_config = config.get_keyspace_config(env, currency)

    raw_keyspace = raw_keyspace_override or ks_config.raw_keyspace_name

    schema_type = currency_to_schema_type.get(currency)
    if patch and schema_type not in ("account", "account_trx"):
        raise click.ClickException(
            f"--patch is only supported for account chains (got {currency}, "
            f"schema_type={schema_type}). UTXO derived tables "
            f"(transaction_spending, transaction_spent_in) are computed across "
            f"the full block range loaded by Spark; a partial rerun would "
            f"silently miss spend links whose two endpoints straddle the "
            f"window boundary. Re-run from a fresh keyspace instead."
        )

    # Resolve delta path from config if not overridden. S3 credentials are
    # selected explicitly via --s3-config (not auto-derived from the sink) so
    # the user picks read-time credentials independently of write-time config.
    if delta_lake_path is None:
        ingest_cfg = ks_config.ingest_config
        if ingest_cfg and ingest_cfg.raw_keyspace_file_sinks:
            delta_sink = ingest_cfg.raw_keyspace_file_sinks.get("delta")
            if delta_sink:
                delta_lake_path = delta_sink.directory
        if delta_lake_path is None:
            raise click.UsageError(
                "No --delta-lake-path provided and no delta sink configured "
                f"for {currency} in environment {env}."
            )

    is_s3_path = delta_lake_path.startswith("s3://") or delta_lake_path.startswith(
        "s3a://"
    )
    if is_s3_path and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        if not available:
            raise click.UsageError(
                f"Delta Lake path {delta_lake_path} is on S3 but no s3_configs "
                "are defined in the graphsense config. Add at least one named "
                "entry under s3_configs and pass --s3-config NAME."
            )
        raise click.UsageError(
            f"Delta Lake path {delta_lake_path} is on S3 but --s3-config was "
            f"not provided. Available s3_configs: {', '.join(available)}."
        )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config(spark_profile)
    spark_packages = config.get_spark_packages()

    # Schema creation runs BEFORE Spark (uses cassandra-driver, no Java needed)
    if create_schema:
        logger.info(f"Creating Cassandra schema for {raw_keyspace} if not exists...")
        GraphsenseSchemas().create_keyspace_if_not_exist(
            env,
            currency,
            keyspace_type="raw",
            keyspace_name_override=raw_keyspace_override,
        )
        GraphsenseSchemas().apply_migrations(
            env,
            currency,
            keyspace_type="raw",
            keyspace_name_override=raw_keyspace_override,
        )
    cassandra_nodes = env_config.cassandra_nodes
    cassandra_username = env_config.username
    cassandra_password = env_config.password

    # Safety check: verify the target keyspace block table is empty
    # to prevent accidental data corruption from mixing sources. Skipped
    # when --patch is set (account chains only — guarded above).
    if not patch:
        from cassandra.cluster import Cluster as CassCluster

        host, _, port = cassandra_nodes[0].partition(":")
        cass_port = int(port) if port else 9042
        auth_provider = None
        if cassandra_username and cassandra_password:
            from cassandra.auth import PlainTextAuthProvider

            auth_provider = PlainTextAuthProvider(
                username=cassandra_username, password=cassandra_password
            )
        with CassCluster(
            [host], port=cass_port, auth_provider=auth_provider
        ) as cluster:
            session = cluster.connect()
            rows = list(
                session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (raw_keyspace,),
                )
            )
            if rows:
                # Keyspace exists — check if block table has data
                block_row = session.execute(
                    f"SELECT block_id FROM {raw_keyspace}.block LIMIT 1"  # noqa: S608
                ).one()
                if block_row is not None:
                    raise click.ClickException(
                        f"Keyspace {raw_keyspace} already contains data "
                        f"(block table is not empty). Use a fresh keyspace, "
                        f"truncate existing tables, or pass --patch to "
                        f"overwrite the requested block range (account "
                        f"chains only)."
                    )

    from graphsenselib.ingest.delta.sink import delta_lake_highest_block
    from graphsenselib.utils.locking import create_lock, delta_ingest_lock_name

    delta_lock_name = delta_ingest_lock_name(delta_lake_path, currency)
    transformed_keyspace = ks_config.transformed_keyspace_name

    # Phase 1: pin a top-block snapshot under the delta-ingest lock so
    # concurrent ingest writes past this boundary cannot tear our read.
    # Block rows are committed last in each ingest batch, so any
    # block_id <= top is guaranteed to have its dependent rows committed.
    with create_lock(delta_lock_name):
        top_block = delta_lake_highest_block(delta_lake_path, s3_credentials)
    if top_block is None:
        raise click.ClickException(
            f"Cannot pin top-block: block Delta table at {delta_lake_path}/block is empty."
        )
    if end_block is None or end_block > top_block:
        end_block = top_block

    _log_startup_banner(
        env=env,
        currency=currency,
        delta_lake_path=delta_lake_path,
        s3_credentials=s3_credentials,
        raw_keyspace=raw_keyspace,
        raw_keyspace_overridden=raw_keyspace_override is not None,
        cassandra_nodes=cassandra_nodes,
        start_block=start_block,
        end_block=end_block,
        top_block=top_block,
        local=local,
        patch=patch,
    )

    # Deferred PySpark import
    from graphsenselib.transformation.factory import run as run_factory

    # Phase 2: hold the transformed-keyspace lock for the Spark run so
    # only one transformation writes to a given transformed keyspace at
    # a time. Ingest is not blocked: the delta-ingest lock from phase 1
    # has already been released.
    with create_lock(transformed_keyspace):
        run_factory(
            env=env,
            currency=currency,
            delta_lake_path=delta_lake_path,
            cassandra_nodes=cassandra_nodes,
            cassandra_username=cassandra_username,
            cassandra_password=cassandra_password,
            raw_keyspace=raw_keyspace,
            start_block=start_block,
            end_block=end_block,
            local=local,
            s3_credentials=s3_credentials,
            spark_config=spark_config,
            spark_packages=spark_packages,
            debug_write_audit=debug_write_audit,
        )


def _log_pubkey_startup_banner(
    *,
    env,
    currency,
    schema_type,
    source_path,
    sink_path,
    sink_type,
    pubkey_keyspace,
    pubkey_table,
    cassandra_nodes,
    s3_credentials,
    start_block,
    end_block,
    local,
):
    """Print a structured banner so it's obvious what this run will do.

    For ``sink_type=cassandra`` we additionally emit a WARNING with the
    fully-qualified target so a misconfigured run can't silently scribble
    over a production keyspace.
    """
    from urllib.parse import urlparse

    def _classify(path):
        """Return (display_path, kind_tag). kind_tag is one of 'local'|'s3'|<scheme>."""
        parsed = urlparse(path)
        if parsed.scheme in ("s3", "s3a"):
            return (
                f"{parsed.scheme}://{parsed.netloc or '?'}{parsed.path}",
                "s3",
            )
        if parsed.scheme in ("", "file"):
            return (path, "local")
        return (path, parsed.scheme)

    def _fmt(path):
        loc, kind = _classify(path)
        return f"{loc}  [{kind}]"

    s3_endpoint = (s3_credentials or {}).get("AWS_ENDPOINT_URL")

    lines = [
        "=" * 72,
        "Cross-chain pubkey → address materialisation",
        "=" * 72,
        f"  env              : {env or '(none — local-only)'}",
        f"  currency         : {currency}  (schema={schema_type})",
        f"  source path      : {_fmt(source_path)}",
        f"  sink path        : {_fmt(sink_path)}",
    ]
    if s3_endpoint:
        lines.append(f"  s3 endpoint      : {s3_endpoint}")
    lines.append(f"  sink type        : {sink_type}")
    if sink_type == "cassandra":
        lines += [
            f"  cassandra ks     : {pubkey_keyspace}",
            f"  cassandra table  : {pubkey_table}",
            f"  cassandra nodes  : {', '.join(cassandra_nodes or [])}",
        ]
    else:
        sink_loc, sink_kind = _classify(sink_path)
        lines.append(f"  delta target     : {sink_loc}/{pubkey_table}  [{sink_kind}]")
    lines += [
        f"  start block      : {start_block if start_block is not None else '(resume from state)'}",
        f"  end block        : {end_block}",
        f"  spark mode       : {'local[*]' if local else 'cluster'}",
        "=" * 72,
    ]
    logger.info("\n" + "\n".join(lines))

    if sink_type == "cassandra":
        nodes_str = ", ".join(cassandra_nodes or []) or "(unset)"
        logger.warning(
            "Cassandra write target: %s.%s on nodes [%s]. "
            "Rows are upserted by `address` PK — any existing row with a "
            "matching address WILL be overwritten with the recomputed pubkey. "
            "Verify env=%s is correct before this run completes; "
            "use --sink-type=delta for a no-write dry run.",
            pubkey_keyspace,
            pubkey_table,
            nodes_str,
            env,
        )


@transformation.command(
    "pubkey-update",
    short_help="[ALPHA] Update cross-chain pubkey → address lookup from Delta Lake.",
)
@require_environment(required=False)
@require_currency()
@click.option(
    "--start-block",
    type=int,
    default=None,
    help=(
        "Start block (exclusive). If omitted, resume from the per-network "
        "last_processed_block stored in the pubkey Delta state table."
    ),
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help="End block (inclusive). If omitted, auto-detected from Delta Lake.",
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create the Cassandra pubkey keyspace/table if it does not exist.",
)
@click.option(
    "--source-path",
    type=str,
    default=None,
    help=(
        "Source Delta Lake base path for this currency (read). Defaults to "
        "the path configured in graphsense.yaml for this env/currency."
    ),
)
@click.option(
    "--sink-path",
    type=str,
    default=None,
    help=(
        "Delta Lake base path for the shared cross-chain pubkey store "
        "(read+write: observed / materialised / state, plus pubkey_by_address "
        "when --sink-type=delta). Same path is used for every chain. "
        "Defaults to environments.<env>.pubkey.sink_path from the config."
    ),
)
@click.option(
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help=(
        "Name of the s3_configs entry to use for S3/MinIO credentials. "
        "Required when either path is on s3://."
    ),
)
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode with local[*].",
)
@click.option(
    "--sink-type",
    type=click.Choice(["cassandra", "delta"]),
    default=None,
    help=(
        "Backend for the final (address, pubkey) rows. 'cassandra' appends "
        "to pubkey.pubkey_by_address; 'delta' writes a Delta table at "
        "<sink-path>/pubkey_by_address (useful for local tests without a "
        "Cassandra cluster). Defaults to environments.<env>.pubkey.sink_type "
        "if configured, else 'cassandra'."
    ),
)
@click.option(
    "--auto-compact",
    type=str,
    default=None,
    help=(
        "After the update, compact the 'observed' table if due. Run-spec "
        "'<period>[;<weekday>]' controls age since last compaction and the day "
        "to run, e.g. '7d;sunday' = compact on Sundays if the last compaction "
        "was more than 7 days ago. Runs only after the update lock is released."
    ),
)
@click.option(
    "--pubkey-keyspace",
    type=str,
    default=None,
    help=(
        "Cassandra keyspace to write the (address, pubkey) rows into "
        "(--sink-type=cassandra). Defaults to environments.<env>.pubkey.keyspace, "
        "else a fresh 'pubkey_v2' keyspace — deliberately NOT the legacy 'pubkey' "
        "table, which an older script may own. The REST reader chooses its source "
        "separately via cross_chain_pubkey_mapping_keyspace."
    ),
)
@click.option(
    "--skip-detect",
    is_flag=True,
    help=(
        "Extract pubkeys and append them to 'observed' (and bump state), but "
        "skip the cross-chain detection/materialisation step. Use on every "
        "invocation of a multi-chain backfill EXCEPT the last, so the full-table "
        "detection groupBy runs once at the end (via the final non-skipped run "
        "or a standalone 'pubkey-detect') instead of once per chain."
    ),
)
@spark_profile_option
def run_pubkey_update(
    env,
    currency,
    start_block,
    end_block,
    create_schema,
    source_path,
    sink_path,
    s3_config_name,
    local,
    sink_type,
    auto_compact,
    pubkey_keyspace,
    skip_detect,
    spark_profile,
):
    """Update cross-chain pubkey → address lookup for ``currency``.

    Reads new transactions from the source Delta Lake for ``currency``,
    extracts signing pubkeys, merges them into a shared cross-chain Delta
    Lake store at ``--sink-path``, and writes derived addresses for any
    pubkey newly observed on 2+ chains to the configured ``--sink-type``
    backend (Cassandra ``pubkey.pubkey_by_address`` table, or a Delta
    table under ``--sink-path``).

    ALPHA: not yet validated in production; the interface may change.
    """
    _warn_alpha("transformation pubkey-update")
    from graphsenselib.config import (
        currency_to_schema_type,
        get_config,
    )
    from graphsenselib.ingest.delta.sink import delta_lake_highest_block
    from graphsenselib.pubkey.factory import run_pubkey, run_pubkey_compact
    from graphsenselib.pubkey.job import (
        ACCOUNT_CURRENCIES,
        PUBKEY_KEYSPACE,
        PUBKEY_TABLE,
        UTXO_CURRENCIES,
    )
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.utils.date import parse_older_than_run_spec
    from graphsenselib.utils.locking import create_lock

    if currency not in UTXO_CURRENCIES and currency not in ACCOUNT_CURRENCIES:
        raise click.UsageError(
            f"Unsupported currency for pubkey update: {currency}. "
            f"Supported: {sorted(UTXO_CURRENCIES | ACCOUNT_CURRENCIES)}"
        )

    config = get_config()
    env_config = config.get_environment(env) if env is not None else None
    ks_config = config.get_keyspace_config(env, currency) if env is not None else None

    # Resolve sink path / type: an explicit CLI flag always wins, otherwise
    # fall back to environments.<env>.pubkey.{sink_path,sink_type}.
    pubkey_cfg = env_config.pubkey if env_config else None
    if sink_path is None and pubkey_cfg is not None:
        sink_path = pubkey_cfg.sink_path
    if sink_type is None:
        sink_type = (pubkey_cfg.sink_type if pubkey_cfg else None) or "cassandra"

    # Resolve the Cassandra write keyspace: CLI flag > env config > fresh
    # default. PUBKEY_KEYSPACE is intentionally a new keyspace, not the legacy
    # 'pubkey' table an older script may own (see pubkey.job).
    if pubkey_keyspace is None:
        pubkey_keyspace = (
            pubkey_cfg.keyspace if pubkey_cfg else None
        ) or PUBKEY_KEYSPACE
    if sink_path is None:
        raise click.UsageError(
            "--sink-path is required (or set environments.<env>.pubkey.sink_path "
            "in the config)."
        )

    # -e is only required when we actually need env-scoped config:
    # writing to Cassandra, or resolving the source path from
    # graphsense.yaml. For a fully-local --sink-type delta run with an
    # explicit --source-path we can skip the lookups entirely.
    if env is None:
        if sink_type != "delta":
            raise click.UsageError("--env is required when --sink-type=cassandra.")
        if source_path is None:
            raise click.UsageError(
                "--source-path is required when --env is omitted "
                "(no env config available to resolve it from)."
            )

    # Resolve source path from config if not overridden — same as
    # `transformation delta-to-raw`.
    if source_path is None:
        assert ks_config is not None  # guarded above: env must be set here
        ingest_cfg = ks_config.ingest_config
        if ingest_cfg and ingest_cfg.raw_keyspace_file_sinks:
            delta_sink = ingest_cfg.raw_keyspace_file_sinks.get("delta")
            if delta_sink:
                source_path = delta_sink.directory
        if source_path is None:
            raise click.UsageError(
                "No --source-path provided and no delta sink configured "
                f"for {currency} in environment {env}."
            )

    is_s3 = any(
        p.startswith("s3://") or p.startswith("s3a://")
        for p in (source_path, sink_path)
    )
    if is_s3 and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        if not available:
            raise click.UsageError(
                "An S3 path was given but no s3_configs are "
                "defined in the graphsense config."
            )
        raise click.UsageError(
            f"An S3 path was given but --s3-config was not "
            f"provided. Available s3_configs: {', '.join(available)}."
        )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config(spark_profile)

    if create_schema:
        if sink_type != "cassandra":
            raise click.UsageError(
                "--create-schema only applies when --sink-type=cassandra."
            )
        logger.info(f"Creating Cassandra keyspace '{pubkey_keyspace}' if not exists...")
        GraphsenseSchemas().create_pubkey_keyspace_if_not_exist(
            env, keyspace_name=pubkey_keyspace
        )

    schema_type = currency_to_schema_type.get(currency)

    if end_block is None:
        top = delta_lake_highest_block(source_path, s3_credentials)
        if top is None:
            raise click.ClickException(
                f"Cannot auto-detect end_block: block Delta table at "
                f"{source_path}/block is empty."
            )
        end_block = top
        logger.info(f"Auto-detected end_block={end_block} from source delta.")

    _log_pubkey_startup_banner(
        env=env,
        currency=currency,
        schema_type=schema_type,
        source_path=source_path,
        sink_path=sink_path,
        sink_type=sink_type,
        pubkey_keyspace=pubkey_keyspace,
        pubkey_table=PUBKEY_TABLE,
        cassandra_nodes=(env_config.cassandra_nodes if env_config else None),
        s3_credentials=s3_credentials,
        start_block=start_block,
        end_block=end_block,
        local=local,
    )

    # Serialise cross-chain detection / sink write across concurrent
    # per-chain invocations sharing the same sink_path. Locked on the stable
    # PUBKEY_KEYSPACE name (not the overridable write keyspace) so it always
    # matches the pubkey-compact lock, which contends on the same Delta store.
    with create_lock(PUBKEY_KEYSPACE):
        run_pubkey(
            env=env or "local",
            currency=currency,
            source_path=source_path,
            sink_path=sink_path,
            cassandra_nodes=env_config.cassandra_nodes if env_config else None,
            cassandra_username=env_config.username if env_config else None,
            cassandra_password=env_config.password if env_config else None,
            start_block=start_block,
            end_block=end_block,
            local=local,
            s3_credentials=s3_credentials,
            spark_config=spark_config,
            pubkey_keyspace=pubkey_keyspace,
            sink_type=sink_type,
            skip_detect=skip_detect,
        )

    # Auto-compaction runs AFTER the update lock is released: run_pubkey_compact
    # re-acquires the same (non-reentrant) pubkey lock, so it must not nest.
    if auto_compact:
        last_compaction = _pubkey_last_compaction_time(sink_path, s3_credentials)
        if parse_older_than_run_spec(auto_compact, last_compaction):
            logger.info(
                f"Auto-compact conditions met (last compaction: {last_compaction}); "
                "compacting observed."
            )
            run_pubkey_compact(
                env=env or "local",
                sink_path=sink_path,
                local=local,
                s3_credentials=s3_credentials,
                spark_config=spark_config,
            )
        else:
            logger.info(
                f"Auto-compact conditions not met (last compaction: "
                f"{last_compaction}); skipping."
            )


@transformation.command(
    "pubkey-load",
    short_help="[ALPHA] Load the Delta pubkey_by_address dataset into Cassandra.",
)
@require_environment(required=True)
@click.option(
    "--sink-path",
    type=str,
    default=None,
    help=(
        "Delta Lake base path of the cross-chain pubkey store written by a "
        "sink_type=delta pubkey-update run (reads <sink-path>/pubkey_by_address). "
        "Defaults to environments.<env>.pubkey.sink_path."
    ),
)
@click.option(
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help="Name of the s3_configs entry for S3/MinIO creds. Required if sink-path is s3://.",
)
@click.option("--local", is_flag=True, help="Run Spark in local mode with local[*].")
@click.option(
    "--pubkey-keyspace",
    type=str,
    default=None,
    help=(
        "Cassandra keyspace to load into. Defaults to "
        "environments.<env>.pubkey.keyspace, else 'pubkey_v2'."
    ),
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create the Cassandra pubkey keyspace/table if it does not exist.",
)
@spark_profile_option
def run_pubkey_load_cmd(
    env, sink_path, s3_config_name, local, pubkey_keyspace, create_schema, spark_profile
):
    """Load the cross-chain ``pubkey_by_address`` Delta table into Cassandra.

    The Cassandra-write half of the decoupled flow: run ``pubkey-update`` with
    ``--sink-type delta`` for every chain (heavy compute, never touches
    production Cassandra), inspect the resulting Delta dataset, then load it
    here into the isolated keyspace when it looks good.

    ALPHA: not yet validated in production; the interface may change.
    """
    _warn_alpha("transformation pubkey-load")
    from graphsenselib.config import get_config
    from graphsenselib.pubkey.factory import run_pubkey_load
    from graphsenselib.pubkey.job import PUBKEY_KEYSPACE, PUBKEY_TABLE
    from graphsenselib.schema.schema import GraphsenseSchemas

    config = get_config()
    env_config = config.get_environment(env)
    pubkey_cfg = env_config.pubkey

    if sink_path is None and pubkey_cfg is not None:
        sink_path = pubkey_cfg.sink_path
    if sink_path is None:
        raise click.UsageError(
            "--sink-path is required (or set environments.<env>.pubkey.sink_path)."
        )
    if pubkey_keyspace is None:
        pubkey_keyspace = (
            pubkey_cfg.keyspace if pubkey_cfg else None
        ) or PUBKEY_KEYSPACE

    is_s3 = sink_path.startswith("s3://") or sink_path.startswith("s3a://")
    if is_s3 and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        raise click.UsageError(
            "sink-path is on S3 but --s3-config was not provided. "
            f"Available s3_configs: {', '.join(available) or 'none'}."
        )
    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config(spark_profile)

    if create_schema:
        logger.info(f"Creating Cassandra keyspace '{pubkey_keyspace}' if not exists...")
        GraphsenseSchemas().create_pubkey_keyspace_if_not_exist(
            env, keyspace_name=pubkey_keyspace
        )

    logger.info(
        f"Loading {PUBKEY_TABLE} from {sink_path} into "
        f"{pubkey_keyspace}.{PUBKEY_TABLE} (env={env})"
    )
    run_pubkey_load(
        env=env,
        sink_path=sink_path,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        pubkey_keyspace=pubkey_keyspace,
        local=local,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
    )


def _pubkey_last_compaction_time(sink_path, s3_credentials):
    """Timestamp of the last `observed` compaction, or None.

    Reads the Delta history via delta-rs (no Spark). A compaction is marked by
    the overwrite commit written by ``compact_observed`` (always present), so
    this is reliable even when the trailing OPTIMIZE is a no-op. Also matches
    OPTIMIZE commits for robustness.
    """
    from datetime import datetime

    import deltalake

    observed_path = sink_path.rstrip("/") + "/observed"
    storage_options = {}
    if s3_credentials:
        storage_options = {
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "false",
            "AWS_CONDITIONAL_PUT": "etag",
            **s3_credentials,
        }
    try:
        history = deltalake.DeltaTable(
            observed_path, storage_options=storage_options
        ).history()
    except Exception:
        # Table does not exist yet → treat as "never compacted".
        return None

    timestamps = [
        h["timestamp"]
        for h in history
        if h.get("operation") == "OPTIMIZE"
        or (
            h.get("operation") == "WRITE"
            and h.get("operationParameters", {}).get("mode") == "Overwrite"
        )
    ]
    if not timestamps:
        return None
    return datetime.fromtimestamp(max(timestamps) // 1000)


@transformation.command(
    "pubkey-compact",
    short_help="[ALPHA] Deduplicate/compact the cross-chain pubkey 'observed' table.",
)
@require_environment(required=False)
@click.option(
    "--sink-path",
    type=str,
    default=None,
    help=(
        "Delta Lake base path of the shared cross-chain pubkey store. "
        "Defaults to environments.<env>.pubkey.sink_path from the config."
    ),
)
@click.option(
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help="Name of the s3_configs entry to use when the sink path is on s3://.",
)
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode.",
)
@spark_profile_option
def run_pubkey_compact_command(env, sink_path, s3_config_name, local, spark_profile):
    """Rewrite ``<sink-path>/observed`` as distinct (pubkey, network) and OPTIMIZE.

    ``pubkey-update`` appends to ``observed`` without deduplicating, so
    re-observed hot keys accumulate duplicate rows. Detection stays correct,
    but this periodically shrinks the table and bin-packs small files. Safe to
    schedule between update runs; it takes the same pubkey lock so it won't
    race a concurrent update.

    ALPHA: not yet validated in production; the interface may change.
    """
    _warn_alpha("transformation pubkey-compact")
    from graphsenselib.config import get_config
    from graphsenselib.pubkey.factory import run_pubkey_compact
    from graphsenselib.pubkey.job import PUBKEY_KEYSPACE
    from graphsenselib.utils.locking import create_lock

    config = get_config()
    env_config = config.get_environment(env) if env is not None else None

    pubkey_cfg = env_config.pubkey if env_config else None
    if sink_path is None and pubkey_cfg is not None:
        sink_path = pubkey_cfg.sink_path
    if sink_path is None:
        raise click.UsageError(
            "--sink-path is required (or set environments.<env>.pubkey.sink_path "
            "in the config)."
        )

    is_s3 = sink_path.startswith("s3://") or sink_path.startswith("s3a://")
    if is_s3 and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        raise click.UsageError(
            "An S3 sink path was given but --s3-config was not provided. "
            f"Available s3_configs: {', '.join(available) or '(none)'}."
        )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config(spark_profile)

    with create_lock(PUBKEY_KEYSPACE):
        run_pubkey_compact(
            env=env or "local",
            sink_path=sink_path,
            local=local,
            s3_credentials=s3_credentials,
            spark_config=spark_config,
        )


@transformation.command(
    "pubkey-detect",
    short_help="[ALPHA] Run cross-chain pubkey detection/materialisation once.",
)
@require_environment(required=False)
@click.option(
    "--sink-path",
    type=str,
    default=None,
    help=(
        "Delta Lake base path of the shared cross-chain pubkey store. "
        "Defaults to environments.<env>.pubkey.sink_path from the config."
    ),
)
@click.option(
    "--s3-config",
    "s3_config_name",
    type=str,
    default=None,
    help="Name of the s3_configs entry to use when the sink path is on s3://.",
)
@click.option("--local", is_flag=True, help="Run Spark in local mode.")
@click.option(
    "--sink-type",
    type=click.Choice(["cassandra", "delta"]),
    default=None,
    help=(
        "Backend for the derived (address, pubkey) rows; must match the "
        "pubkey-update runs that fed 'observed'. Defaults to "
        "environments.<env>.pubkey.sink_type, else 'cassandra'."
    ),
)
@click.option(
    "--pubkey-keyspace",
    type=str,
    default=None,
    help=(
        "Cassandra keyspace to write into (--sink-type=cassandra). Defaults to "
        "environments.<env>.pubkey.keyspace, else 'pubkey_v2'."
    ),
)
@click.option(
    "--create-schema",
    is_flag=True,
    help="Create the Cassandra pubkey keyspace/table if it does not exist.",
)
@spark_profile_option
def run_pubkey_detect_command(
    env,
    sink_path,
    s3_config_name,
    local,
    sink_type,
    pubkey_keyspace,
    create_schema,
    spark_profile,
):
    """Run cross-chain detection + materialisation once over ``<sink-path>``.

    The deferred half of ``pubkey-update --skip-detect``: append every chain to
    ``observed`` with detection skipped, then run this once so the full-table
    detection ``groupBy`` executes a single time instead of once per chain.
    Currency-agnostic; idempotent (anti-joins the ``materialised`` set). Takes
    the same pubkey lock as update/compact so it won't race them.

    ALPHA: not yet validated in production; the interface may change.
    """
    _warn_alpha("transformation pubkey-detect")
    from graphsenselib.config import get_config
    from graphsenselib.pubkey.factory import run_pubkey_detect
    from graphsenselib.pubkey.job import PUBKEY_KEYSPACE
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.utils.locking import create_lock

    config = get_config()
    env_config = config.get_environment(env) if env is not None else None
    pubkey_cfg = env_config.pubkey if env_config else None

    if sink_path is None and pubkey_cfg is not None:
        sink_path = pubkey_cfg.sink_path
    if sink_type is None:
        sink_type = (pubkey_cfg.sink_type if pubkey_cfg else None) or "cassandra"
    if pubkey_keyspace is None:
        pubkey_keyspace = (
            pubkey_cfg.keyspace if pubkey_cfg else None
        ) or PUBKEY_KEYSPACE
    if sink_path is None:
        raise click.UsageError(
            "--sink-path is required (or set environments.<env>.pubkey.sink_path "
            "in the config)."
        )

    # -e is only needed to resolve Cassandra coordinates; a delta-sink detect
    # can run with just an explicit --sink-path.
    if env is None and sink_type != "delta":
        raise click.UsageError("--env is required when --sink-type=cassandra.")

    is_s3 = sink_path.startswith("s3://") or sink_path.startswith("s3a://")
    if is_s3 and s3_config_name is None:
        available = sorted(config.s3_configs.keys())
        raise click.UsageError(
            "An S3 sink path was given but --s3-config was not provided. "
            f"Available s3_configs: {', '.join(available) or '(none)'}."
        )

    s3_credentials = config.get_s3_credentials(s3_config_name)
    spark_config = config.get_spark_config(spark_profile)

    if create_schema:
        if sink_type != "cassandra":
            raise click.UsageError(
                "--create-schema only applies when --sink-type=cassandra."
            )
        logger.info(f"Creating Cassandra keyspace '{pubkey_keyspace}' if not exists...")
        GraphsenseSchemas().create_pubkey_keyspace_if_not_exist(
            env, keyspace_name=pubkey_keyspace
        )

    with create_lock(PUBKEY_KEYSPACE):
        run_pubkey_detect(
            env=env or "local",
            sink_path=sink_path,
            cassandra_nodes=env_config.cassandra_nodes if env_config else None,
            cassandra_username=env_config.username if env_config else None,
            cassandra_password=env_config.password if env_config else None,
            pubkey_keyspace=pubkey_keyspace,
            sink_type=sink_type,
            local=local,
            s3_credentials=s3_credentials,
            spark_config=spark_config,
        )


def _expected_transformed_ks(currency, suffix, no_date):
    """Transformed keyspace name a fresh full transform would create.

    Mirrors GraphsenseSchemas.create_new_transformed_ks_if_not_exist so
    --dry-run can show the target without creating the keyspace.
    """
    from datetime import datetime

    name = f"{currency}_transformed"
    if not no_date:
        name = f"{name}_{datetime.now().strftime('%Y%m%d')}"
    if suffix is not None:
        name = f"{name}_{suffix}"
    return name


@transformation.command(
    "raw-to-transformed",
    short_help="[ALPHA] Transform the Cassandra raw keyspace into a transformed keyspace (raw → transformed, graphsense-spark job).",
)
@require_environment()
@require_currency()
@click.option(
    "--suffix",
    default=None,
    help="Suffix for the new transformed keyspace (default: today's date).",
)
@click.option(
    "--no-date",
    is_flag=True,
    help="Omit the date in the new transformed keyspace name.",
)
@click.option(
    "--raw-keyspace",
    "raw_keyspace_override",
    default=None,
    help="Override the source raw keyspace (default: from config).",
)
@click.option(
    "--target-keyspace",
    "target_keyspace_override",
    default=None,
    help=(
        "Write into this existing transformed keyspace instead of creating a "
        "fresh dated one (created if missing)."
    ),
)
@click.option(
    "--version",
    "version_override",
    default=None,
    help=(
        "graphsense-spark release tag to run, or 'latest' for the newest stable "
        "release (default: from config; resolves latest if unset)."
    ),
)
@click.option(
    "--artifact",
    type=click.Choice(["fat", "slim"]),
    default=None,
    help="Which release jar to use (default: from config).",
)
@click.option(
    "--backend",
    "backend_override",
    type=click.Choice(["scala", "pyspark"]),
    default=None,
    help="Implementation backend (default: from config; currently 'scala').",
)
@click.option(
    "--writer",
    type=click.Choice(["cassandra", "sidecar"]),
    default=None,
    help="Cassandra write path (default: from config sidecar.enabled).",
)
@click.option(
    "--spark-home",
    envvar="SPARK_HOME",
    default=None,
    help="Spark install dir (or set SPARK_HOME). Falls back to spark-submit on PATH.",
)
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark locally (master=local[*]) for testing.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print the spark-submit command and exit; creates no keyspace.",
)
@click.argument("extra_jar_args", nargs=-1, type=click.UNPROCESSED)
@spark_profile_option
def run_full_transform(
    env,
    currency,
    suffix,
    no_date,
    raw_keyspace_override,
    target_keyspace_override,
    version_override,
    artifact,
    backend_override,
    writer,
    spark_home,
    local,
    dry_run,
    extra_jar_args,
    spark_profile,
):
    """Run the raw → transformed full transform.

    Creates a fresh transformed keyspace, then runs the external graphsense-spark
    Scala job via spark-submit (jar pulled from a public GitHub Release and
    cached). Backend-neutral by design: a future native-PySpark implementation
    can be selected with `backend: pyspark` without changing how this is invoked.

    Extra args after `--` are passed through to the job, e.g.
    `... raw-to-transformed -e prod -c btc -- --debug 1`.

    ALPHA: not yet validated in production; the interface may change.
    \f
    """
    _warn_alpha("transformation raw-to-transformed")
    from graphsenselib.config import get_config
    from graphsenselib.transformation.spark_jar import (
        apply_sidecar,
        build_spark_submit,
        fetch_release_jar,
        resolve_latest_release,
        run_spark_submit,
    )

    config = get_config()
    env_config = config.get_environment(env)
    ks_config = config.get_keyspace_config(env, currency)
    fta = config.get_full_transform_args()

    backend = backend_override or fta.backend
    if backend != "scala":
        raise click.UsageError(
            f"Backend '{backend}' is not implemented yet; only 'scala' "
            f"(external graphsense-spark job) is currently available."
        )

    schemas = GraphsenseSchemas()

    # 1. Resolve / create the target transformed keyspace.
    if target_keyspace_override:
        target_keyspace = target_keyspace_override
        if not dry_run:
            schemas.create_keyspace_if_not_exist(
                env,
                currency,
                "transformed",
                keyspace_name_override=target_keyspace,
            )
    elif dry_run:
        target_keyspace = _expected_transformed_ks(currency, suffix, no_date)
    else:
        created = schemas.create_new_transformed_ks_if_not_exist(
            env, currency, suffix, no_date
        )
        if created is None:
            raise click.ClickException(
                "Target transformed keyspace already exists. Pass a fresh "
                "--suffix, or --target-keyspace to write into an existing one."
            )
        target_keyspace = created

    raw_keyspace = raw_keyspace_override or ks_config.raw_keyspace_name

    # 2. Spark properties from the per-currency profile + cassandra coordinates.
    #    An explicit --spark-profile overrides the per-currency config default.
    spark_props = dict(
        config.get_spark_config(spark_profile or fta.profile_for(currency))
    )
    if local:
        spark_props["spark.master"] = "local[*]"
    if "spark.master" not in spark_props:
        raise click.UsageError(
            "No spark.master configured. Set it in the spark_config profile "
            "(baseline or the per-currency profile) or pass --local."
        )
    spark_props.setdefault(
        "spark.cassandra.connection.host",
        ",".join(n.split(":")[0] for n in env_config.cassandra_nodes),
    )
    ports = {n.partition(":")[2] for n in env_config.cassandra_nodes if ":" in n}
    if len(ports) == 1:
        spark_props.setdefault("spark.cassandra.connection.port", ports.pop())
    if env_config.username:
        spark_props.setdefault("spark.cassandra.auth.username", env_config.username)
    if env_config.password:
        spark_props.setdefault("spark.cassandra.auth.password", env_config.password)

    # 3. Jar + packages (fat is self-contained; slim needs the package list).
    version = version_override or fta.version_for(currency)
    if not version or version == "latest":
        version = resolve_latest_release(fta.repo)
        logger.info(f"Using latest stable graphsense-spark release: {version}")
    artifact = artifact or fta.artifact
    packages = [] if artifact == "fat" else list(fta.packages)

    # 4. Job args: base + per-currency extras + CLI passthrough.
    jar_args = [
        "--network",
        currency,
        "--raw-keyspace",
        raw_keyspace,
        "--target-keyspace",
        target_keyspace,
        *fta.jar_args.get(currency, []),
        *extra_jar_args,
    ]

    # 5. Optional sidecar bulk-write path (config flag or --writer sidecar).
    use_sidecar = writer == "sidecar" or (writer is None and fta.sidecar.enabled)
    if use_sidecar:
        spark_props, packages, jar_args = apply_sidecar(
            spark_props,
            packages,
            jar_args,
            contact_points=fta.sidecar.contact_points,
            local_dc=fta.sidecar.local_dc,
            consistency_level=fta.sidecar.consistency_level,
        )

    jar_path = fetch_release_jar(fta.repo, version, artifact, config.cache_directory)

    cmd = build_spark_submit(
        spark_home=spark_home,
        jar_path=jar_path,
        main_class=fta.main_class,
        spark_props=spark_props,
        packages=packages,
        repositories=fta.repositories,
        jar_args=jar_args,
        extra_submit_args=fta.extra_submit_args,
    )

    logger.info(
        "\n".join(
            [
                "=" * 72,
                "Full transform (raw -> transformed)",
                "=" * 72,
                f"  env             : {env}",
                f"  currency        : {currency}",
                f"  backend         : {backend}",
                f"  raw keyspace    : {raw_keyspace}",
                f"  target keyspace : {target_keyspace}",
                f"  jar             : {fta.repo}@{version} ({artifact})",
                f"  writer          : {'sidecar' if use_sidecar else 'cassandra'}",
                f"  spark.master    : {spark_props.get('spark.master')}",
                "=" * 72,
            ]
        )
    )

    if dry_run:
        click.echo("# Resolved Spark configuration:")
        for key in sorted(spark_props):
            click.echo(f"#   {key}={spark_props[key]}")
        click.echo("# spark-submit command:")
        click.echo(" ".join(cmd))
        return

    rc = run_spark_submit(cmd)
    if rc != 0:
        raise click.ClickException(f"spark-submit exited with code {rc}")
    logger.info(f"Full transform complete: {currency} -> {target_keyspace}")


@transformation.command("cluster", short_help="Run one-off UTXO address clustering.")
@require_environment()
@require_currency()
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode (local[*]) instead of submitting to the cluster.",
)
@click.option(
    "--read-partitions",
    type=int,
    default=None,
    help=(
        "Partitions the edge-set DataFrame is coalesced to before streaming to the "
        "driver (one Arrow blob each). Raise it if a partition exceeds "
        "spark.driver.maxResultSize or executor memory is tight on large chains "
        "(default 64). Does NOT control join parallelism."
    ),
)
@click.option(
    "--end-block",
    type=int,
    default=None,
    help=(
        "Cluster the chain only up to this block (inclusive); transactions in "
        "later blocks are ignored. Omit to cluster the whole transaction table. "
        "There is no start bound — clustering is transitive over full history."
    ),
)
def run_clustering(env, currency, local, read_partitions, end_block):
    """Run one-off UTXO address clustering with PySpark.

    Bulk-reads raw.transaction and address_ids_by_address_prefix via parallel
    token-range scans, clusters multi-input transactions with the Rust Union-Find,
    and bulk-writes fresh_address_cluster / fresh_cluster_addresses /
    fresh_cluster_stats via the Spark Cassandra connector. Clusters the whole
    transaction table, or only blocks up to --end-block when given.

    The transformed keyspace must already be seeded (Scala transformation or a
    prior run) so summary_statistics.no_addresses is populated.
    \f
    """
    from graphsenselib.config import get_config, is_fresh_clustering_enabled
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import run_clustering_spark
    from graphsenselib.transformation.spark import create_spark_session
    from graphsenselib.utils.locking import create_lock

    if not is_fresh_clustering_enabled():
        raise click.ClickException(
            "Fresh clustering is disabled. Set "
            "GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true to enable."
        )

    GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="transformed")

    with DbFactory().from_config(env, currency) as db:
        config = get_config()
        env_config = config.get_environment(env)
        ks_config = config.get_keyspace_config(env, currency)
        raw_keyspace = ks_config.raw_keyspace_name
        transformed_keyspace = ks_config.transformed_keyspace_name

        stats = db.transformed.get_summary_statistics()
        if stats is None or getattr(stats, "no_addresses", None) is None:
            raise click.ClickException(
                f"{transformed_keyspace}.summary_statistics.no_addresses is "
                "missing — seed the transformed keyspace before clustering."
            )
        max_address_id = int(stats.no_addresses)

        logger.info(
            f"Starting Spark clustering: env={env}, currency={currency}, "
            f"raw={raw_keyspace}, transformed={transformed_keyspace}"
        )
        with create_lock(transformed_keyspace):
            # Clear the fresh_* tables first so a re-run is idempotent. Membership
            # and stats are append/upsert-written and cluster_id == min(address_id)
            # can SHRINK across re-runs (a smaller address_id joins/merges a cluster),
            # so stale cluster_id-keyed rows in fresh_cluster_addresses /
            # fresh_cluster_stats would otherwise survive as phantom clusters
            # (fresh_address_cluster self-heals per-address, but is truncated too
            # so a shrunk keyspace never keeps orphaned member rows).
            db.transformed.execute_raw_cql("TRUNCATE fresh_address_cluster")
            db.transformed.execute_raw_cql("TRUNCATE fresh_cluster_addresses")
            db.transformed.execute_raw_cql("TRUNCATE fresh_cluster_stats")
            spark_session = create_spark_session(
                app_name=f"graphsense-clustering-{currency}-{env}",
                local=local,
                cassandra_nodes=env_config.cassandra_nodes,
                cassandra_username=env_config.username,
                cassandra_password=env_config.password,
                spark_config=config.get_spark_config(),
                spark_packages=config.get_spark_packages(),
            )
            try:
                spark_kwargs = {}
                if read_partitions is not None:
                    spark_kwargs["read_partitions"] = read_partitions
                run_clustering_spark(
                    spark_session,
                    raw_keyspace=raw_keyspace,
                    transformed_keyspace=transformed_keyspace,
                    max_address_id=max_address_id,
                    bucket_size=db.transformed.get_cluster_id_bucket_size(),
                    end_block=end_block,
                    **spark_kwargs,
                )
            finally:
                spark_session.stop()
                logger.info("SparkSession stopped.")
        logger.info("One-off clustering complete.")


@transformation.command(
    "recompute-cluster-stats",
    short_help="Recompute fresh_cluster_stats from address-level tables.",
)
@require_environment()
@require_currency()
@click.option(
    "--local",
    is_flag=True,
    help="Run Spark in local mode (local[*]) instead of submitting to the cluster.",
)
def recompute_cluster_stats(env, currency, local):
    """Recompute the full ``fresh_cluster_stats`` from the address-level tables.

    Aggregates ``address`` + ``address_incoming/outgoing_relations`` through the
    fresh ``address -> cluster`` membership into per-cluster size, totals,
    first/last tx, degrees and tx-counts, and rewrites ``fresh_cluster_stats``.
    Membership (``fresh_address_cluster`` / ``fresh_cluster_addresses``) is NOT
    touched. The delta loop keeps only size + root live, so the full
    cluster-level stats lag behind and this job refreshes them in one pass.

    Holds the transformed-keyspace lock (the same lock the delta updater takes) so
    it never races a delta merge, and truncates ``fresh_cluster_stats`` first for a
    clean, self-healing rebuild (clears rows of clusters merged away since the last
    run). ``total_received_adj`` / ``total_spent_adj`` are the cluster totals
    minus intra-cluster flows (summed external-relation ``estimated_value``);
    validate against a real keyspace before REST reads them.
    \f
    """
    from graphsenselib.config import get_config, is_fresh_clustering_enabled
    from graphsenselib.db.factory import DbFactory
    from graphsenselib.schema.schema import GraphsenseSchemas
    from graphsenselib.transformation.clustering import recompute_fresh_cluster_stats
    from graphsenselib.transformation.spark import create_spark_session
    from graphsenselib.utils.locking import create_lock

    if not is_fresh_clustering_enabled():
        raise click.ClickException(
            "Fresh clustering is disabled. Set "
            "GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true to enable."
        )

    GraphsenseSchemas().apply_migrations(env, currency, keyspace_type="transformed")

    with DbFactory().from_config(env, currency) as db:
        config = get_config()
        env_config = config.get_environment(env)
        transformed_keyspace = db.transformed.get_keyspace()

        logger.info(
            f"Recomputing cluster stats: env={env}, currency={currency}, "
            f"transformed={transformed_keyspace} (acquiring keyspace lock)"
        )
        with create_lock(transformed_keyspace):
            db.transformed.execute_raw_cql("TRUNCATE fresh_cluster_stats")
            spark_session = create_spark_session(
                app_name=f"graphsense-cluster-stats-{currency}-{env}",
                local=local,
                cassandra_nodes=env_config.cassandra_nodes,
                cassandra_username=env_config.username,
                cassandra_password=env_config.password,
                spark_config=config.get_spark_config(),
                spark_packages=config.get_spark_packages(),
            )
            try:
                n = recompute_fresh_cluster_stats(
                    spark_session,
                    transformed_keyspace,
                    db.transformed.get_cluster_id_bucket_size(),
                )
            finally:
                spark_session.stop()
                logger.info("SparkSession stopped.")
        logger.info(f"Cluster-stat recompute complete: {n} clusters.")
