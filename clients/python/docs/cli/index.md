# `graphsense` — GraphSense CLI

Query blockchain analytics from the shell. Pipe-friendly (`jq`/`sed`/`mlr`),
reads single values, lists, CSV, JSON, or stdin.

## Install

```sh
pip install graphsense-python[cli]
```

This installs the optional `click` + `jmespath` dependencies and the `graphsense`
command. Without the `[cli]` extra the Python package still works, just
without the `graphsense` entry point.

## Synopsis

```
graphsense [GLOBAL OPTIONS] COMMAND [ARGS]
```

## Global options

| Option                       | Purpose                                                               |
| ---------------------------- | --------------------------------------------------------------------- |
| `--api-key KEY`              | Env fallback, first match wins: `GRAPHSENSE_API_KEY` → `IKNAIO_API_KEY` → `GS_API_KEY` → `API_KEY`. |
| `--host URL`                 | Env fallback, first match wins: `GRAPHSENSE_HOST` → `IKNAIO_HOST` → `GS_HOST`. Otherwise uses the baked-in default. |
| `-f/--format json|jsonl|csv` | Output format. See [outputs](outputs.md).                              |
| `-o/--output FILE`           | Write to FILE instead of stdout.                                       |
| `-d/--directory DIR`         | One file per record, named by the primary id.                          |
| `-i/--input FILE`            | Read ids from FILE instead of positionals / stdin.                     |
| `--input-format auto|json|csv|lines` | Force an input format; default is sniff.                      |
| `--address-jq EXPR`                  | jmespath selector for JSON input (e.g. `[].address`).                  |
| `--address-col NAME_OR_IDX`          | CSV column to pull ids from.                                           |
| `--network-jq EXPR`          | Parallel jmespath for per-row network/currency extraction.             |
| `--network-col NAME_OR_IDX`  | Parallel CSV column for per-row network/currency extraction.           |
| `--bulk` / `--no-bulk`       | Force/disable the `/bulk` endpoint; default is threshold-based.        |
| `--bulk-threshold N`         | Switch to bulk at N ids (default 10).                                  |
| `--color auto|always|never`  | Colorize JSON output. Default `auto` enables colors only on a TTY stdout and disables when `NO_COLOR` is set. File output (`-o`/`-d`) and CSV are never colored. |
| `--no-color`                 | Shorthand for `--color never`.                                         |
| `-q/--quiet`                 | Suppress stderr notices (including deprecation warnings).              |
| `-v/--verbose`               | Increase verbosity (repeat for more).                                  |

## Commands at a glance

- [`graphsense lookup-address`](commands.md#lookup-address) — one-shot address lookup with optional tag/cluster/summary bundling
- [`graphsense lookup-cluster`](commands.md#lookup-cluster)
- [`graphsense lookup-tx`](commands.md#lookup-tx)
- [`graphsense search`](commands.md#search) — disambiguate across networks
- [`graphsense statistics`](commands.md#statistics)
- [`graphsense exchange-rates`](commands.md#exchange-rates)
- [`graphsense block`](commands.md#block)
- [`graphsense tags-for`](commands.md#tags-for)
- [`graphsense actor`](commands.md#actor)
- [`graphsense bulk`](bulk.md) — direct access to `/bulk.json` / `/bulk.csv`
- [`graphsense raw <group> <method> ...`](raw.md) — full auto-mirrored generated API

## See also

- **[Tour](tour.md)** — start here: end-to-end copy-paste examples.
- [Inputs](inputs.md)
- [Outputs](outputs.md)
- [Bulk guide](bulk.md)
- [Recipes](recipes.md)
- [Python wrapper](../ext/index.md) (`graphsense.ext.GraphSense`)
