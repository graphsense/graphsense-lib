# Recipes

Real pipelines. Copy, paste, adapt.

## 1. Enrich a customer CSV with attribution

```sh
cat customers.csv \
  | graphsense --address-col btc_address -f jsonl \
       lookup-address btc --with-tags --with-tag-summary \
  | jq -c 'select(.tag_summary.tag_count > 0) | {addr: .address, label: .tag_summary.best_label}'
```

## 2. Export a cluster's whole activity to CSV

```sh
graphsense -f csv raw clusters list-cluster-txs btc 1234 --pagesize 1000 \
  -o cluster_1234_txs.csv
```

## 3. Bulk-enrich transactions streamed from jq

```sh
jq -r '.txs[].hash' suspicious.json \
  | graphsense -f csv bulk get_tx btc --key-field tx_hash \
  > suspicious_txs.csv
```

## 4. Per-address JSON files for downstream tools

```sh
cat watchlist.txt \
  | graphsense --bulk-threshold 1000 lookup-address btc --with-tags \
       -d addresses_out/
# → addresses_out/1A1z....json, addresses_out/1B2y....json, ...
```

## 5. Slice a deep response with jmespath

```sh
graphsense lookup-address btc 1A1z... --with-cluster -f json \
  | jq '.cluster.best_address_tag.label'
```

## 6. Use the escape hatch for a brand-new endpoint

If graphsense-python was regenerated against a newer OpenAPI spec that
added `addresses.get_something_new`, you get it for free:

```sh
graphsense raw addresses get-something-new btc 1A1z...
```

No code changes, no new release of this package.

## 7. Run against a staging host

```sh
graphsense --host https://staging.graphsense.io --api-key $STAGING_KEY statistics
```

## 8. Re-hydrate a `.gs` save file from the dashboard

A `.gs` file from the Pathfinder / Graph dashboards is just a list of
references (addresses, txs, cluster ids) — to do anything analytical with
it you typically want to fetch the full records. The `graphsense gs`
group emits `{network, id}` records that pipe straight into the lookup
commands:

```sh
# Fetch every transaction referenced in the save file
graphsense gs txs graph.gs \
  | graphsense --address-jq '[].id' --network-jq '[].network' \
       -f jsonl lookup-tx btc \
  > txs.jsonl

# Same, but for addresses, with tag summaries bundled in
graphsense gs addresses graph.gs \
  | graphsense --address-jq '[].id' --network-jq '[].network' \
       -f jsonl lookup-address btc --with-tag-summary \
  > addresses.jsonl

# Diff two saves: which addresses did v2 add over v1?
comm -13 \
  <(graphsense gs addresses v1.gs -f jsonl | jq -r '.id' | sort) \
  <(graphsense gs addresses v2.gs -f jsonl | jq -r '.id' | sort)
```

`.gs` files routinely mix networks (BTC + ETH in one graph). The per-row
`--network-jq '[].network'` selector carries the right network into each
lookup — the positional `btc` is only the fallback for rows where the
selector returns empty.
