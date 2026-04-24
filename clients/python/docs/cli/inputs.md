# Inputs

`graphsense` commands accept ids one of four ways:

1. Positional arguments (`graphsense lookup-address btc 1A... 1B...`).
2. From a file via `-i / --input FILE`.
3. From stdin (piped).
4. None — for single-id commands where the id is the only argument.

## Formats

| `--input-format` | When to use                                                      |
| ---------------- | ---------------------------------------------------------------- |
| `auto` (default) | Sniff the first non-whitespace byte: `{` or `[` → JSON; first line has a comma → CSV; otherwise lines. |
| `json`           | Array of strings, array of objects, or a single object with `--address-jq`. |
| `csv`            | With optional header. Pick the id column via `--address-col`.             |
| `lines`          | One id per line; blanks and leading-`#` comments ignored.         |

Explicit `--input-format` always beats auto-detect.

## Selectors

### `--address-jq EXPR` (JSON)

Uses **jmespath**, not jq. Same muscle memory for simple projections:

```sh
echo '[{"address":"1A..."},{"address":"1B..."}]' \
  | graphsense --address-jq '[].address' lookup-address btc
```

Nested projections work too:

```sh
cat wallet.json | graphsense --address-jq 'wallets[].accounts[].address' lookup-address btc
```

### `--address-col NAME_OR_INDEX` (CSV)

Pick a single column by name or 0-based index:

```sh
cat addrs.csv | graphsense --address-col address lookup-address btc --format jsonl
cat addrs.tsv | graphsense --address-col 0       lookup-address btc --format jsonl
```

If the CSV has only one column, `--address-col` is optional.

### Lines

No selector needed:

```sh
printf '1A1z...\n# skip me\n1B2y...\n' | graphsense lookup-address btc
```

## Per-row network / currency

By default the subcommand's positional `CURRENCY` argument is used for
every row. For mixed-network batches, pair the id selector with a network
selector:

| Flag             | Effect                                                          |
| ---------------- | --------------------------------------------------------------- |
| `--network-jq EXPR` | Parallel jmespath that yields the network for each JSON row. |
| `--network-col NAME_OR_IDX` | Parallel CSV column for the network.               |

Rows where the network cell is empty or missing fall back to the positional
`CURRENCY`. The network list must produce the same number of values as the
id list — the CLI exits with a usage error otherwise.

```sh
# CSV — network column + address column
cat mixed.csv \
  | graphsense --address-col address --network-col network \
       lookup-address btc          # fallback network = btc

# JSON — parallel jmespath
echo '[{"net":"btc","a":"1A"},{"net":"eth","a":"0x1"}]' \
  | graphsense --address-jq '[].a' --network-jq '[].net' lookup-address btc
```

Internally the batch is grouped by network and dispatched per group so the
bulk endpoint can still be used within each group.

> **Terminology note**: we prefer "network" for `btc`/`eth`/`trx`/... in
> new flags and code. The REST API still calls this parameter `currency`
> for backward compatibility — both names refer to the same thing.

## Worked example

```sh
# Addresses from a spreadsheet export, one bundle per address:
cat customers.csv | graphsense --address-col btc_address -f jsonl \
    lookup-address btc --with-tags --with-cluster \
    > enriched.jsonl
```
