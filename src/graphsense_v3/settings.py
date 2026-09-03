"""Run settings, read from ``graphsense.yaml``.

Everything a backfill needs already lives in the gslib config -- the lake root,
the S3 credentials, the Cassandra contact points, the Spark profile -- so this
reads them rather than asking for them again on the command line. Nothing here
writes to the config or changes it.

**The v3 keyspace names are constructed, never supplied.** A v3 run must not be
able to touch a v2 keyspace even by typo, so the name is derived from the
network and checked against a pattern no existing keyspace matches, and then
cross-checked against every keyspace name the config mentions. The same pattern
is enforced one level down in :func:`graphsense_v3.spark.writer.write`, so no
code path in this package can write outside it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from graphsense_v3.config import NetworkConfig, config_for
from graphsense_v3.schema import Kind
from graphsense_v3.spark.profile import resolve, warnings

#: The marker that makes a v3 name unmistakable. v2 keyspaces are
#: ``<currency>_<kind>_<env>`` or ``<currency>_<kind>_<YYYYMMDD>``; none of them
#: carry a ``_v3`` segment, and none of the configured environment names is
#: ``v3`` (``config.py:18``). The label, when given, keeps successive runs apart.
V3_MARKER = "v3"
V3_KEYSPACE = re.compile(r"^[a-z]{3,4}_(raw|derived)_v3(_[a-z0-9]+)?$")


class UnsafeKeyspace(RuntimeError):
    """Raised for any keyspace name a v3 job must not write to."""


def v3_keyspace(network: str, kind: Kind, label: Optional[str] = None) -> str:
    """The v3 keyspace name for a network. Constructed, not configurable."""
    if label is not None and not re.fullmatch(r"[a-z0-9]+", label):
        raise ValueError(f"label must be lowercase alphanumeric, got {label!r}")
    suffix = f"_{label}" if label else ""
    name = f"{network.lower()}_{kind.value}_{V3_MARKER}{suffix}"
    assert_v3_keyspace(name)
    return name


def assert_v3_keyspace(name: str) -> None:
    """Raise unless ``name`` is unmistakably a v3 keyspace.

    The structural half of the guarantee: whatever else goes wrong, a write
    cannot land in ``btc_transformed_20260828``.
    """
    if not V3_KEYSPACE.match(name or ""):
        raise UnsafeKeyspace(
            f"{name!r} is not a v3 keyspace name. v3 writes only to "
            f"<network>_<raw|derived>_v3[_<label>], which no existing "
            "keyspace matches -- this is what keeps a v3 run off the live data."
        )


def configured_keyspaces(config: Any) -> set[str]:
    """Every keyspace name the config mentions, across all environments."""
    names: set[str] = set()
    for environment in (getattr(config, "environments", None) or {}).values():
        for keyspace in (getattr(environment, "keyspaces", None) or {}).values():
            names.add(keyspace.raw_keyspace_name)
            names.add(keyspace.transformed_keyspace_name)
    return {n for n in names if n}


def effective_lake_root(root: str) -> str:
    """The path Spark will actually read.

    The config stores ``s3://``, which the deltalake/duckdb readers accept, but
    Hadoop 3 dropped that scheme and Spark fails with "No FileSystem for scheme:
    s3". Resolved here rather than only in the reader so that `plan` cannot
    print a path different from the one that gets read. Idempotent: an
    ``s3a://``, ``hdfs://`` or local path passes through.
    """
    return root.rstrip("/").replace("s3://", "s3a://")


@dataclass(frozen=True)
class RunSettings:
    """What one backfill needs, resolved from the config."""

    network: str
    env: str
    lake_root: str
    raw_keyspace: str
    derived_keyspace: str
    #: The *existing* keyspace holding ``exchange_rates``. Read-only: rates are
    #: not in the lake and the gslib exchange-rates path owns that table.
    rates_keyspace: str
    cassandra_nodes: list[str]
    username: Optional[str] = None
    password: Optional[str] = None
    s3_credentials: Optional[dict] = None
    spark_config: dict[str, str] = field(default_factory=dict)
    spark_packages: dict[str, str] = field(default_factory=dict)
    spark_profile: Optional[str] = None
    #: Keyword arguments for the sidecar bulk writer, or None for the CQL path.
    sidecar: Optional[dict] = None

    @property
    def config(self) -> NetworkConfig:
        return config_for(self.network)

    def with_sidecar(self) -> "RunSettings":
        """The same run, bulk-writing through the Cassandra Sidecar.

        Contact points and local DC come from the config's existing
        ``full_transform_args.sidecar`` block, so there is one place that
        records where the sidecars are. The Spark properties the bulk writer
        needs must be set before the session exists, so they are folded into
        ``spark_config`` here rather than applied later.
        """
        from dataclasses import replace

        from graphsenselib.config import get_config

        from graphsense_v3.spark import sidecar as bulk

        args = get_config().full_transform_args
        cfg = getattr(args, "sidecar", None) if args is not None else None
        if cfg is None or not getattr(cfg, "contact_points", None):
            raise KeyError(
                "sidecar writes need full_transform_args.sidecar.contact_points "
                "in graphsense.yaml"
            )
        contact_points = list(cfg.contact_points)
        local_dc = getattr(cfg, "local_dc", None) or "DC1"
        return replace(
            self,
            spark_config=bulk.session_config(
                self.spark_config,
                contact_points,
                local_dc,
                list(getattr(args, "repositories", None) or []),
            ),
            sidecar={
                "contact_points": contact_points,
                "local_dc": local_dc,
                "consistency_level": getattr(cfg, "consistency_level", None)
                or "LOCAL_QUORUM",
            },
        )

    def describe(self) -> str:
        """What the run will do, in the form a person can check before firing."""
        return "\n".join(
            [
                f"network            {self.network}",
                f"environment        {self.env}",
                f"lake               {self.lake_root}",
                f"raw keyspace       {self.raw_keyspace}      (created, written)",
                f"derived keyspace   {self.derived_keyspace}      (created, written)",
                f"rates keyspace     {self.rates_keyspace}      (READ ONLY)",
                f"cassandra          {', '.join(self.cassandra_nodes) or '(none)'}",
                f"spark profile      {self.spark_profile or '(baseline)'}",
                f"cassandra writes   {'sidecar bulk writer' if self.sidecar else 'connector CQL path'}",
                *(f"WARNING            {w}" for w in warnings(self.spark_config)),
            ]
        )


def from_config(
    env: str,
    network: str,
    *,
    label: Optional[str] = None,
    spark_profile: Optional[str] = None,
    config: Any = None,
) -> RunSettings:
    """Resolve a run from ``graphsense.yaml``.

    Raises rather than guesses: a missing lake sink or an unconfigured network
    is a setup problem, and finding it here costs seconds where finding it on
    the cluster costs a run.
    """
    from graphsenselib.config import get_config

    cfg = config if config is not None else get_config()
    network = network.lower()

    environment = cfg.get_environment(env)
    if network not in (environment.keyspaces or {}):
        available = sorted(environment.keyspaces or {})
        raise KeyError(
            f"network {network!r} is not configured in environment {env!r}; "
            f"available: {', '.join(available) or '(none)'}"
        )
    keyspaces = cfg.get_keyspace_config(env, network)

    lake = cfg.get_deltaupdater_config(env, network)
    if lake is None or lake.delta_sink is None:
        raise KeyError(
            f"no delta lake sink configured for {network!r} in {env!r} "
            "(environments.<env>.keyspaces.<network>.ingest_config."
            "raw_keyspace_file_sinks.delta.directory)"
        )

    # The per-currency Spark profile the full-transform command already uses
    # (`config.py:513`, FullTransformArgs.profile_for). Reusing it means the v3
    # run gets the resources that network was already sized for, rather than a
    # second set of numbers to keep in step.
    profile = spark_profile
    if profile is None and cfg.full_transform_args is not None:
        profile = cfg.full_transform_args.profile_for(network)

    settings = RunSettings(
        network=network,
        env=env,
        lake_root=effective_lake_root(lake.delta_sink.directory),
        raw_keyspace=v3_keyspace(network, Kind.RAW, label),
        derived_keyspace=v3_keyspace(network, Kind.DERIVED, label),
        rates_keyspace=keyspaces.raw_keyspace_name,
        cassandra_nodes=list(environment.cassandra_nodes),
        username=environment.username,
        password=environment.password,
        s3_credentials=lake.s3_credentials,
        spark_config=resolve(cfg.get_spark_config(profile)),
        spark_packages=dict(cfg.spark_packages or {}),
        spark_profile=profile,
    )
    assert_no_conflict(settings, cfg)
    return settings


def assert_no_conflict(settings: RunSettings, config: Any) -> None:
    """The second half of the guarantee: no v3 target names a configured keyspace.

    The pattern check alone would not catch an environment literally named
    ``v3``, which would make ``btc_raw_v3`` a real v2 keyspace. This does.
    """
    existing = configured_keyspaces(config)
    for name in (settings.raw_keyspace, settings.derived_keyspace):
        assert_v3_keyspace(name)
        if name in existing:
            raise UnsafeKeyspace(
                f"{name!r} is already a keyspace in graphsense.yaml. Pass a "
                "--label to move the v3 run out of its way; nothing here will "
                "write to a configured keyspace."
            )
    if settings.rates_keyspace in (
        settings.raw_keyspace,
        settings.derived_keyspace,
    ):
        raise UnsafeKeyspace("the read-only rates keyspace cannot be a write target")
