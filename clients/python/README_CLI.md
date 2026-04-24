# `graphsense` — GraphSense CLI (shell)

Query blockchain analytics from the command line. Pipe-friendly, handles
single values, CSV, JSON, and stdin.

```sh
pip install graphsense-python[cli]

# Single lookup
graphsense lookup-address btc 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa \
   --with-tags --with-cluster

# CSV in → enriched JSONL out
cat customers.csv \
  | graphsense --address-col btc_address -f jsonl \
       lookup-address btc --with-tags \
  > enriched.jsonl

# Full auto-mirrored generated API, no hand-written commands needed
graphsense raw addresses list-address-txs btc 1A1z... --pagesize 20
```

## Features

- **Flat convenience commands** (`lookup-address`, `lookup-cluster`,
  `lookup-tx`, `search`, `statistics`, ...) for the common cases.
- **`graphsense raw`** auto-mirrors *every* method of *every* generated `*Api`
  class. Survives regeneration for free.
- **Input**: positionals, `-i FILE`, or stdin; auto-detects JSON / CSV /
  plain lines. `--address-jq '[].address'` projects from JSON; `--address-col address`
  projects from CSV.
- **Output**: JSON / JSONL / CSV. `-o FILE` infers format from extension;
  `-d DIR` writes one file per record.
- **Auto-bulk**: for N ≥ `--bulk-threshold` (default 10), switches to
  `/bulk.json` or `/bulk.csv` for a single efficient POST. `--bulk` /
  `--no-bulk` override.
- **Deprecation-aware**: RFC 8594 `Deprecation` / `Sunset` headers from
  the server trigger a one-line stderr warning (silence with `--quiet`).
- **Non-deprecated endpoints only** on the flat commands. Deprecated APIs
  (`EntitiesApi`) are hidden under `graphsense raw` unless
  `GS_SHOW_DEPRECATED=1` is set.

## Full docs

- **[cli/tour.md](docs/cli/tour.md)** — start here: copy-paste examples.
- [cli/index.md](docs/cli/index.md) — synopsis and global options
- [cli/inputs.md](docs/cli/inputs.md) — input formats, selectors (jmespath / CSV columns)
- [cli/outputs.md](docs/cli/outputs.md) — JSON / JSONL / CSV, `-o` / `-d`
- [cli/bulk.md](docs/cli/bulk.md) — auto-bulk heuristic, `graphsense bulk` direct
- [cli/commands.md](docs/cli/commands.md) — per-command reference
- [cli/raw.md](docs/cli/raw.md) — `graphsense raw` auto-mirror, how it tracks the generated API
- [cli/recipes.md](docs/cli/recipes.md) — end-to-end pipelines
