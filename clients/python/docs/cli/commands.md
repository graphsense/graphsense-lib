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
  [--with-io] [--with-flows] [--with-upstream] [--with-downstream]
```

`--with-upstream` is the *backward* trace (GraphSense's underlying
`/spending` endpoint — counter-intuitive naming); `--with-downstream` is
the forward trace.

```sh
graphsense lookup-tx btc a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d \
   --with-io --with-upstream --with-downstream
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
graphsense exchange-rates CURRENCY HEIGHT
```

## block

```
graphsense block CURRENCY HEIGHT
```

## tags-for

```
graphsense tags-for CURRENCY ADDRESS
```

Pagination: use `graphsense raw addresses list-tags-by-address <currency> <addr> --page <token> --pagesize 100`
if you need to page explicitly.

## actor

```
graphsense actor ACTOR_ID
```
