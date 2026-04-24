# Outputs

By default `graphsense` writes to stdout. Use `-o FILE` for a single file or
`-d DIR` for one file per record.

## Format resolution

| Explicit `--format` | `-o` extension | Input is list? | Effective format |
| ------------------- | -------------- | -------------- | ---------------- |
| set                 | any            | any            | `--format` wins  |
| unset               | `.csv`         | any            | `csv`            |
| unset               | `.jsonl`/`.ndjson` | any        | `jsonl`          |
| unset               | `.json`        | any            | `json`           |
| unset               | none (stdout)  | single record  | `json`           |
| unset               | none (stdout)  | list / stream  | `jsonl`          |

## Formats

### `json`

Pretty-printed UTF-8 JSON. For list results, a single JSON array. Good for
single-result commands (`graphsense lookup-address btc 1A...`).

### `jsonl`

One JSON record per line. Streams as records arrive. Default for list
outputs — pipes cleanly into `jq -c '...'` or `mlr --j2c cat`.

### `csv`

`csv.DictWriter` output. Columns come from the first record's keys in
insertion order; nested dicts are flattened with dotted keys
(`balance.value`). Subsequent records with extra keys append new columns
deterministically.

> Note: the CSV flattening rule is lossy for deeply nested structures. If
> you need full fidelity, prefer `jsonl` and let `jq`/`mlr` handle the
> transformation.

## `-o FILE`

Write to the given path. The file is truncated and re-created. Format is
inferred from the extension unless `--format` is given.

## `-d DIR`

Useful when every record is a heavy payload (say, a full address with tags
and flows) and you want them in separate files for subsequent tools. The
directory is created if missing. Each record lands in `<id>.json` where
`<id>` is the primary key (`address`, `tx_hash`, etc.).

## Colorized output

JSON and JSONL output to **stdout on a TTY** is syntax-highlighted (via
pygments) by default. File output (`-o FILE` / `-d DIR`) and CSV are
never colored — so pipes into `jq`, `mlr`, redirects to disk, or CI logs
stay clean.

Toggles:

| Invocation                  | Behaviour                                    |
| --------------------------- | -------------------------------------------- |
| `--color auto` (default)    | TTY-only, disabled if `NO_COLOR` env is set. |
| `--color always`            | Force colors even when piping.               |
| `--color never` / `--no-color` | Force plain text.                         |

Environment variables honoured (standard conventions):

- `NO_COLOR=<anything>` → disables colors
- `CLICK_COLOR=0` / `CLICK_COLOR=1` → disables / enables colors

## Exit codes

| Code | Meaning                                                  |
| ---- | -------------------------------------------------------- |
| 0    | Success.                                                 |
| 2    | Bad CLI usage (click's built-in). See `--help`.          |
| non-0 from HTTP | ApiException propagates as a Python traceback. Pipe stderr somewhere if you want to grep it. |
