# Command reference

Every global option (`--api-key`, `--host`, `--currency`, `--format`, ...)
must be passed *before* the subcommand: `graphsense --format jsonl lookup-address ...`.

## lookup-address

```
graphsense lookup-address CURRENCY [ADDRESSES...]
  [--with-tags] [--with-cluster] [--with-tag-summary]
  [--include-actors/--no-include-actors]
```

Look up one or more addresses. Bundles related data in parallel. Without
`--with-*` flags and above `--bulk-threshold`, automatically switches to
the bulk endpoint.

```sh
# single address, tag + cluster bundled
graphsense lookup-address btc 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa \
   --with-tags --with-cluster

# 50 addresses from CSV → one bulk POST
cat addrs.csv | graphsense --address-col address lookup-address btc
```

## lookup-cluster

```
graphsense lookup-cluster CURRENCY [CLUSTER_IDS...]
  [--with-tag-summary] [--with-top-addresses]
```

```sh
graphsense lookup-cluster btc 17 --with-top-addresses --with-tag-summary
```

## lookup-tx

```
graphsense lookup-tx CURRENCY [TX_HASHES...]
  [--with-io] [--with-upstream] [--with-downstream] [--with-heuristics]
  [--with-flows]
```

Some flags only apply to one of the two transaction models. They are
silently skipped on the other model so the same invocation works across
mixed inputs:

| Flag                | UTXO chains (btc, ltc, ...) | Account chains (eth, trx, ...) |
| ------------------- | --------------------------- | ------------------------------ |
| `--with-io`         | ✅ fetches `/inputs` + `/outputs` | skipped                  |
| `--with-upstream`   | ✅ backward trace (`/spending`)   | skipped                  |
| `--with-downstream` | ✅ forward trace (`/spent_in`)    | skipped                  |
| `--with-heuristics` | ✅ computes every heuristic (change, coinjoin, ...) | skipped |
| `--with-flows`      | skipped                     | ✅ fetches `/flows`            |

`--with-upstream` is the *backward* trace (GraphSense's underlying
`/spending` endpoint — counter-intuitive naming); `--with-downstream` is
the forward trace.

`--with-heuristics` always asks the server for *all* heuristics
(`include_heuristics=all`) — there's no per-heuristic toggle on the flat
command. Use `graphsense raw txs get-tx --include-heuristics ...` if you
need to pass a specific subset.

```sh
# UTXO transaction
graphsense lookup-tx btc a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d \
   --with-io --with-upstream --with-downstream --with-heuristics

# Account transaction
graphsense lookup-tx eth 0x... --with-flows
```

## search

```
graphsense search QUERY [--currency CCY]
```

Disambiguate an identifier across networks. Handy as the first step when
all you have is a hex string and no idea what chain it belongs to.

```sh
graphsense search "binance"
graphsense search 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa --currency btc
```

## statistics

```
graphsense statistics
```

Returns indexer freshness and network coverage. No currency argument.

## exchange-rates

```
graphsense exchange-rates CURRENCY HEIGHT_OR_DATE
```

Accepts either a block height (`825000`) or any ISO 8601 date / datetime
the server's parser understands — `2024-01-15`, `2024-01-15T12:34:56Z`,
`"2024-01-15 12:34"`, etc. Date queries first resolve the closest block
via `get_block_by_date` and then fetch rates at that height.

## block

```
graphsense block CURRENCY HEIGHT_OR_DATE
```

Accepts either a block height or an ISO 8601 date / datetime. Date queries
return the block at or before the moment (via `get_block_by_date`).

## tags-for

```
graphsense tags-for CURRENCY ADDRESS
                    [--include-best-cluster-tag/--no-include-best-cluster-tag]
                    [--limit N]
                    [--page-size N]
```

Lists attribution tags for an address. By default it walks all pages of
the underlying paginated endpoint (`--page-size 100`) and inherits the
best cluster tag down to the address level
(`--include-best-cluster-tag`, default on). Pass `--limit N` to cap the
total number of tags; the response then carries a non-null `next_page`
token you can hand back via
`graphsense raw addresses list-tags-by-address ... --page <token>` to
resume.

## actor

```
graphsense actor ACTOR_ID
```

## gs

Read GraphSense `.gs` save files written by the Pathfinder / Graph
dashboards. The extraction commands (`txs`, `addresses`) emit a uniform
`{network, id}` shape that pipes directly into `lookup-tx` /
`lookup-address` via the standard
`--address-jq '[].id' --network-jq '[].network'` selectors.

```
graphsense gs txs       FILE [--dedupe/--no-dedupe]
graphsense gs addresses FILE [--dedupe/--no-dedupe]
graphsense gs decode    FILE [--raw]
graphsense gs summary   FILE
```

| Command     | Pathfinder file | Graph file                  | Output records              |
| ----------- | --------------- | --------------------------- | --------------------------- |
| `txs`       | tx hashes       | empty (graphs have no txs)  | `{"network", "id"}`         |
| `addresses` | addresses       | addresses                   | `{"network", "id"}`         |
| `decode`    | structured JSON | structured JSON             | typed dataclasses → JSON; `--raw` emits the underlying payload as-is |
| `summary`   | counts + kind   | counts + kind               | dict                        |

Cluster references in Graph files (rare; legacy format) are not exposed
as a dedicated subcommand — fetch them via `graphsense gs decode FILE`
and pull from the `entities` array if needed.

Records are deduped (by `(network, id)`) by default, preserving first-seen
order. Pass `--no-dedupe` to keep repeats.

`.gs` files cover multiple networks in one file — the per-row `network`
column carries the right network into the downstream lookup. The
positional `CURRENCY` on `lookup-tx` / `lookup-address` is the fallback
for rows where the selector resolves to empty.

```sh
# every tx referenced in graph.gs, looked up
graphsense gs txs graph.gs \
  | graphsense --address-jq '[].id' --network-jq '[].network' lookup-tx btc

# every address, bundled with tag summaries; honor each address's own network
graphsense gs addresses graph.gs \
  | graphsense --address-jq '[].id' --network-jq '[].network' \
       lookup-address btc --with-tag-summary

# inspect a file without touching the API
graphsense gs summary graph.gs
graphsense -f json gs decode graph.gs | jq '.addresses | length'
```
