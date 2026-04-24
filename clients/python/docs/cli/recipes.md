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
