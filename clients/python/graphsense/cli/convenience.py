"""Hand-written flat commands: `graphsense lookup-address`, `graphsense lookup-tx`, ..."""

from __future__ import annotations

from collections import OrderedDict
from typing import Optional

import click

from graphsense.ext.client import _looks_like_date

from graphsense.cli.context import CliContext
from graphsense.ext import bulk as bulk_mod
from graphsense.ext import io as io_mod
from graphsense.ext import output as out_mod


def register_convenience_commands(group: click.Group) -> None:
    group.add_command(lookup_address)
    group.add_command(lookup_cluster)
    group.add_command(lookup_tx)
    group.add_command(search)
    group.add_command(statistics)
    group.add_command(exchange_rates)
    group.add_command(block)
    group.add_command(tags_for)
    group.add_command(actor)


pass_ctx = click.make_pass_decorator(CliContext)


def _collect_ids(ctx: CliContext, positional: tuple[str, ...]) -> list[str]:
    if positional:
        return list(positional)
    text = ctx.read_input_text()
    if text is None or text.strip() == "":
        return []
    return io_mod.parse_input(
        text,
        input_format=ctx.input_format,
        jq=ctx.address_jq,
        col=ctx.address_col,
    )


def _collect_id_network_pairs(
    ctx: CliContext,
    positional: tuple[str, ...],
    default_network: str,
) -> list[tuple[str, str]]:
    """Return `[(network, id), ...]` using per-row network selectors if set.

    Positional ids always use `default_network` (the subcommand's CURRENCY
    argument). Input-driven ids consult `--network-jq` / `--network-col`
    and fall back to `default_network` when empty.
    """
    if positional:
        return [(default_network, p) for p in positional]
    text = ctx.read_input_text()
    if text is None or text.strip() == "":
        return []
    raw = io_mod.parse_input_with_network(
        text,
        input_format=ctx.input_format,
        jq=ctx.address_jq,
        col=ctx.address_col,
        network_jq=ctx.network_jq,
        network_col=ctx.network_col,
        default_network=default_network,
    )
    # `default_network` is always a non-empty string in this path, so narrow.
    return [(net or default_network, i) for net, i in raw]


def _group_by_network(
    pairs: list[tuple[str, str]],
) -> "OrderedDict[str, list[str]]":
    """Preserve first-seen order; within a group, preserve input order."""
    out: "OrderedDict[str, list[str]]" = OrderedDict()
    for net, i in pairs:
        out.setdefault(net, []).append(i)
    return out


def _write(ctx: CliContext, payload) -> None:
    out_mod.write(
        payload,
        output=ctx.output,
        directory=ctx.directory,
        format=ctx.format,
        color=ctx.color,
    )


# --------------------------------------------------------------------- address
@click.command(name="lookup-address")
@click.argument("currency")
@click.argument("addresses", nargs=-1)
@click.option("--with-tags", is_flag=True, default=False)
@click.option("--with-cluster", is_flag=True, default=False)
@click.option("--with-tag-summary", is_flag=True, default=False)
@click.option("--include-actors/--no-include-actors", default=True)
@pass_ctx
def lookup_address(
    ctx: CliContext,
    currency: str,
    addresses: tuple[str, ...],
    with_tags: bool,
    with_cluster: bool,
    with_tag_summary: bool,
    include_actors: bool,
) -> None:
    """Look up one or more addresses, optionally bundling tags/cluster/summary."""
    pairs = _collect_id_network_pairs(ctx, addresses, default_network=currency)
    if not pairs:
        raise click.UsageError(
            "no addresses provided — pass positionals, --input FILE, or pipe stdin"
        )
    gs = ctx.gs()
    groups = _group_by_network(pairs)

    # Fast path: one group → preserve current single-vs-list output shape.
    if len(groups) == 1:
        net, ids = next(iter(groups.items()))
        results = _dispatch_address(
            ctx,
            gs,
            net,
            ids,
            with_tags,
            with_cluster,
            with_tag_summary,
            include_actors,
        )
        _write(ctx, results)
        return

    # Mixed networks → always a list, dispatch per group and concatenate.
    collected: list = []
    for net, ids in groups.items():
        r = _dispatch_address(
            ctx,
            gs,
            net,
            ids,
            with_tags,
            with_cluster,
            with_tag_summary,
            include_actors,
        )
        if isinstance(r, list):
            collected.extend(r)
        else:
            collected.append(r)
    _write(ctx, collected)


def _dispatch_address(
    ctx: CliContext,
    gs,
    currency: str,
    ids: list[str],
    with_tags: bool,
    with_cluster: bool,
    with_tag_summary: bool,
    include_actors: bool,
):
    use_bulk = bulk_mod.should_bulk(
        len(ids), threshold=ctx.bulk_threshold, override=ctx.bulk
    )
    if use_bulk and not (with_tags or with_cluster or with_tag_summary):
        # Plain address fetch via /bulk is the big win.
        if not ctx.quiet:
            bulk_mod.announce_switch()
        fmt = (ctx.format or "json").lower()
        if fmt == "csv":
            return gs.bulk("get_address", ids, currency=currency, format="csv")
        return gs.bulk("get_address", ids, currency=currency, format="json")

    # Per-item bundles (parallel). When multiple ids, output is a list.
    def one(addr: str):
        return gs.lookup_address(
            addr,
            currency=currency,
            with_tags=with_tags,
            with_cluster=with_cluster,
            with_tag_summary=with_tag_summary,
            include_actors=include_actors,
        ).to_dict()

    if len(ids) == 1:
        return one(ids[0])
    return bulk_mod.run_parallel(one, ids, max_workers=gs.max_workers)


# --------------------------------------------------------------------- cluster
@click.command(name="lookup-cluster")
@click.argument("currency")
@click.argument("cluster_ids", nargs=-1)
@click.option("--with-tag-summary", is_flag=True, default=False)
@click.option("--with-top-addresses", is_flag=True, default=False)
@pass_ctx
def lookup_cluster(
    ctx: CliContext,
    currency: str,
    cluster_ids: tuple[str, ...],
    with_tag_summary: bool,
    with_top_addresses: bool,
) -> None:
    """Look up one or more clusters by id."""
    pairs = _collect_id_network_pairs(ctx, cluster_ids, default_network=currency)
    if not pairs:
        raise click.UsageError("no cluster ids provided")
    gs = ctx.gs()
    groups = _group_by_network(pairs)

    def one_for(net: str):
        def _one(cid: str):
            return gs.lookup_cluster(
                cid,
                currency=net,
                with_tag_summary=with_tag_summary,
                with_top_addresses=with_top_addresses,
            ).to_dict()

        return _one

    if len(groups) == 1 and len(pairs) == 1:
        net, ids = next(iter(groups.items()))
        _write(ctx, one_for(net)(ids[0]))
        return

    collected: list = []
    for net, ids in groups.items():
        collected.extend(
            bulk_mod.run_parallel(one_for(net), ids, max_workers=gs.max_workers)
        )
    _write(ctx, collected)


# -------------------------------------------------------------------------- tx
@click.command(name="lookup-tx")
@click.argument("currency")
@click.argument("tx_hashes", nargs=-1)
@click.option(
    "--with-io",
    is_flag=True,
    default=False,
    help="UTXO-only: fetch /inputs and /outputs. Silently skipped on "
    "account-model chains (eth, trx, ...).",
)
@click.option(
    "--with-flows",
    is_flag=True,
    default=False,
    help="Account-model only: fetch /flows. Silently skipped on UTXO "
    "chains (btc, ltc, ...).",
)
@click.option(
    "--with-upstream",
    is_flag=True,
    default=False,
    help="UTXO-only: fetch the txs that funded each input.",
)
@click.option(
    "--with-downstream",
    is_flag=True,
    default=False,
    help="UTXO-only: fetch the txs that spent each output.",
)
@click.option(
    "--with-heuristics",
    is_flag=True,
    default=False,
    help="UTXO-only: compute every available heuristic (change, "
    "coinjoin, etc.) and return them on the base tx.",
)
@pass_ctx
def lookup_tx(
    ctx: CliContext,
    currency: str,
    tx_hashes: tuple[str, ...],
    with_io: bool,
    with_flows: bool,
    with_upstream: bool,
    with_downstream: bool,
    with_heuristics: bool,
) -> None:
    """Look up one or more transactions."""
    pairs = _collect_id_network_pairs(ctx, tx_hashes, default_network=currency)
    if not pairs:
        raise click.UsageError("no tx hashes provided")
    gs = ctx.gs()
    groups = _group_by_network(pairs)

    def one_for(net: str):
        def _one(h: str):
            return gs.lookup_tx(
                h,
                currency=net,
                with_io=with_io,
                with_flows=with_flows,
                with_upstream=with_upstream,
                with_downstream=with_downstream,
                with_heuristics=with_heuristics,
            ).to_dict()

        return _one

    if len(groups) == 1 and len(pairs) == 1:
        net, ids = next(iter(groups.items()))
        _write(ctx, one_for(net)(ids[0]))
        return

    collected: list = []
    for net, ids in groups.items():
        collected.extend(
            bulk_mod.run_parallel(one_for(net), ids, max_workers=gs.max_workers)
        )
    _write(ctx, collected)


# ----------------------------------------------------------------- passthrough
@click.command(name="search")
@click.argument("query")
@click.option("--currency", default=None)
@pass_ctx
def search(ctx: CliContext, query: str, currency: Optional[str]) -> None:
    """Disambiguate an identifier across networks."""
    gs = ctx.gs()
    result = gs.search(query, currency=currency)
    _write(ctx, result)


@click.command(name="statistics")
@pass_ctx
def statistics(ctx: CliContext) -> None:
    """Indexer freshness / network coverage."""
    _write(ctx, ctx.gs().statistics())


def _parse_height_or_date(value: str, *, param_name: str) -> int | str:
    """Accept either an integer height or an ISO 8601 date / datetime string.

    Date forms passed to the server are parsed with `dateutil.parser.parse`,
    so we mirror that here. Anything else gets a click usage error
    (instead of leaking a `ValueError` traceback from a downstream `int()`).
    """
    try:
        return int(value)
    except ValueError:
        pass
    if _looks_like_date(value):
        return value
    raise click.BadParameter(
        f"{value!r} is not a valid block height or ISO 8601 date/datetime "
        "(e.g. 825000, 2024-01-15, or 2024-01-15T12:34:56Z)",
        param_hint=param_name,
    )


@click.command(name="exchange-rates")
@click.argument("currency")
@click.argument("height_or_date")
@pass_ctx
def exchange_rates(ctx: CliContext, currency: str, height_or_date: str) -> None:
    """Fiat exchange rates at a block height or at a `YYYY-MM-DD` date."""
    _write(
        ctx,
        ctx.gs().exchange_rates(
            _parse_height_or_date(height_or_date, param_name="HEIGHT_OR_DATE"),
            currency=currency,
        ),
    )


@click.command(name="block")
@click.argument("currency")
@click.argument("height_or_date")
@pass_ctx
def block(ctx: CliContext, currency: str, height_or_date: str) -> None:
    """Look up a block by height or by `YYYY-MM-DD` date (closest block ≤ date)."""
    _write(
        ctx,
        ctx.gs().block(
            _parse_height_or_date(height_or_date, param_name="HEIGHT_OR_DATE"),
            currency=currency,
        ),
    )


@click.command(name="tags-for")
@click.argument("currency")
@click.argument("address")
@click.option(
    "--include-best-cluster-tag/--no-include-best-cluster-tag",
    default=True,
    help="Inherit the best cluster tag down to the address level (default on).",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of tags to return. Default: unlimited (walk all pages).",
)
@click.option(
    "--page-size",
    type=int,
    default=100,
    help="Server page size for the underlying paginated request (default 100).",
)
@pass_ctx
def tags_for(
    ctx: CliContext,
    currency: str,
    address: str,
    include_best_cluster_tag: bool,
    limit: Optional[int],
    page_size: int,
) -> None:
    """List attribution tags for an address (auto-paginates)."""
    _write(
        ctx,
        ctx.gs().tags_for(
            address,
            currency=currency,
            include_best_cluster_tag=include_best_cluster_tag,
            limit=limit,
            page_size=page_size,
        ),
    )


@click.command(name="actor")
@click.argument("actor_id")
@pass_ctx
def actor(ctx: CliContext, actor_id: str) -> None:
    _write(ctx, ctx.gs().actor(actor_id))
