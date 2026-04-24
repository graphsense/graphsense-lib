# A tour of `graphsense`

A single page with end-to-end examples of what the CLI does. Copy, paste,
adapt. For the per-command flag reference see [`commands.md`](commands.md);
for deeper guides on inputs, outputs, bulk and the `raw` escape hatch, see
the sibling pages.

Every example assumes one of `GRAPHSENSE_API_KEY`, `IKNAIO_API_KEY`,
`GS_API_KEY`, or `API_KEY` is set (see [global options](index.md)).

## Single lookups

```sh
# Just the address
graphsense lookup-address btc 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa

# Bundle related data in one call (tags + cluster + tag summary run in parallel)
graphsense lookup-address btc 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa \
   --with-tags --with-cluster --with-tag-summary

# A transaction with upstream + downstream traces
graphsense lookup-tx btc a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d \
   --with-io --with-upstream --with-downstream

# A cluster with its top addresses
graphsense lookup-cluster btc 17 --with-top-addresses

# Don't know what you're holding?
graphsense search "binance"
```

## Stdin pipelines — `jq`-style & `sed`-style selectors

```sh
# JSON in, pick a field with jmespath
echo '[{"addr":"1A1z..."},{"addr":"1B2y..."}]' \
  | graphsense --address-jq '[].addr' lookup-address btc

# CSV in, pick a column
cat customers.csv \
  | graphsense --address-col btc_address lookup-address btc --with-tags

# Plain lines — one id per line; blanks and `#` comments ignored
printf '1A1z...\n# satoshi\n1B2y...\n' \
  | graphsense lookup-address btc
```

## Output control

```sh
# JSON for single, JSONL for lists — default adapts
graphsense lookup-address btc 1A1z...                    # pretty JSON
graphsense lookup-address btc 1A1z... 1B2y... 1C3x...    # 3 JSONL lines

# Force a format
graphsense -f csv lookup-address btc 1A1z... 1B2y... -o out.csv

# Format inferred from extension
graphsense lookup-address btc 1A1z... -o wallet.json           # JSON
graphsense lookup-address btc 1A1z... 1B2y... -o wallet.jsonl  # JSONL
graphsense lookup-address btc 1A1z... 1B2y... -o wallet.csv    # CSV (dotted-key flattening)

# One file per record, named by primary id
cat watchlist.txt | graphsense lookup-address btc --with-tags -d addresses_out/
# → addresses_out/1A1z....json, addresses_out/1B2y....json, ...
```

## Auto-bulk vs per-item

```sh
# 20 addresses → single POST to /btc/bulk.json/get_address (threshold=10)
cat many_addrs.txt | graphsense lookup-address btc

# Force the bulk endpoint even for small lists
graphsense --bulk lookup-address btc 1A1z... 1B2y...

# Force per-item calls — needed with --with-tags etc. (bulk returns flat rows,
# not typed models that carry auxiliary data)
cat many_addrs.txt \
  | graphsense --no-bulk lookup-address btc --with-tags --format jsonl

# Direct bulk call, CSV straight back from the server
printf '1A...\n1B...\n' \
  | graphsense -f csv bulk get_address btc \
  > bulk_result.csv
```

## The `raw` escape hatch — full generated API

```sh
# Every generated method is a subcommand — auto-mirrored from the OpenAPI spec
graphsense raw addresses get-address btc 1A1z...
graphsense raw addresses list-address-txs btc 1A1z... --pagesize 20
graphsense raw clusters list-cluster-neighbors btc 17 --direction out --pagesize 50
graphsense raw txs list-tx-flows btc a1075db5...
graphsense raw general get-statistics
```

## Composed pipelines

```sh
# Enrich a CSV, keep only tagged addresses, extract label
cat customers.csv \
  | graphsense --address-col btc_address -f jsonl \
       lookup-address btc --with-tag-summary \
  | jq -c 'select(.tag_summary.tag_count > 0) | {addr: .address, label: .tag_summary.best_label}'

# Pull tx hashes from a report, bulk-enrich to CSV
jq -r '.incidents[].tx_hash' report.json \
  | graphsense -f csv bulk get_tx btc --key-field tx_hash \
  > report_txs.csv
```

## Mixed-network batches (per-row network extraction)

```sh
# CSV with a network column → per-row dispatch
cat mixed.csv
# network,address
# btc,1A1z...
# eth,0x1234...

cat mixed.csv \
  | graphsense --address-col address --network-col network \
       lookup-address btc        # positional is the fallback network

# Same idea for JSON
echo '[{"net":"btc","a":"1A..."},{"net":"eth","a":"0x1..."}]' \
  | graphsense --address-jq '[].a' --network-jq '[].net' lookup-address btc
```

The positional `CURRENCY` remains required and is used when a row's network
cell is empty or the selector is not set. Batches are grouped by network
internally so bulk can still kick in per group.

## Configuration tricks

```sh
# Per-invocation override
graphsense --host https://staging.iknaio.com --api-key $STAGING_KEY statistics

# Set once in your shell profile (any alias works)
export GRAPHSENSE_API_KEY=...
export GRAPHSENSE_HOST=https://api.iknaio.com
```

## Color & quiet

```sh
# Pretty colors on a TTY, auto-disabled in pipes — no flags needed
graphsense lookup-address btc 1A1z...

# Force colors through a pager
graphsense --color always lookup-address btc 1A1z... | less -R

# Force off (also honors $NO_COLOR)
graphsense --no-color lookup-address btc 1A1z...

# Silence deprecation / bulk-switch notices on stderr
graphsense -q lookup-address btc 1A1z...
```

## Python equivalent

Anything the CLI does, `graphsense.ext` does in Python — no `[cli]` extra needed:

```python
from graphsense.ext import GraphSense

gs = GraphSense(currency="btc")  # reads api_key from env

addr = gs.lookup_address("1A1z...", with_tags=True, with_cluster=True)
print(addr.data.address, addr.data.cluster, addr.tag_summary)

# Escape hatch to the generated API
for tx in gs.raw.addresses.list_address_txs("btc", "1A1z...").address_txs:
    print(tx.tx_hash, tx.value)
```

See [`README_EXT.md`](../../README_EXT.md) for the wrapper overview and
[`docs/ext/GraphSense.md`](../ext/GraphSense.md) for the full reference.
