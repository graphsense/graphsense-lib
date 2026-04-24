# Bulk

GraphSense exposes streaming bulk endpoints at `/{currency}/bulk.json/<op>`
and `/{currency}/bulk.csv/<op>`, which accept a list of ids in one POST and
flatten the results. The CLI exposes this two ways:

## Auto-bulk on `lookup-*`

When you pass many ids to a `lookup-*` command with **no `--with-*` flags**,
`graphsense` switches to the bulk endpoint automatically once the id count reaches
`--bulk-threshold` (default 10).

```sh
# 20 addresses → single POST /btc/bulk.json/get_address
cat lots_of_addrs.txt | graphsense lookup-address btc
```

Flags:

| Flag                   | Effect                                                      |
| ---------------------- | ----------------------------------------------------------- |
| `--bulk`               | Force the bulk endpoint even for 2 ids.                     |
| `--no-bulk`            | Never use the bulk endpoint; fan out per-item calls instead. |
| `--bulk-threshold N`   | Change the auto-switch threshold.                            |

When auto-switching happens, `graphsense` emits a one-line stderr notice the first
time (`notice: switching to bulk endpoint …`). Silence with `--quiet`.

> **Response-shape caveat.** Bulk endpoints return *flat rows* — not the
> typed model you get from the per-item endpoint. Fields are pre-joined by
> the server and suitable for CSV export. Use `--no-bulk` when you need
> typed responses (e.g. with `--with-tags`).

## Explicit `graphsense bulk`

For direct control:

```sh
graphsense bulk <operation> <currency> [keys...]
graphsense bulk get_address btc 1A1z... 1B2y...

# from stdin / file
printf '1A...\n1B...\n' | graphsense bulk get_address btc
```

### Options

| Option          | Default     | Purpose                                            |
| --------------- | ----------- | -------------------------------------------------- |
| `--key-field`   | `address`   | Name of the field the operation expects in the body (`tx_hash`, `cluster`, ...). |
| `--num-pages`   | `1`         | Number of paged results to stream per key.          |

### Output format

- `--format json` / default → one JSON doc with the server's list of rows
  (or `jsonl` when writing to stdout and the server returns a list).
- `--format csv` → passes the server's CSV stream straight through; stream
  starts as soon as the first row arrives.

### Supported operations

The full list is derived from the OpenAPI spec and surfaced at
`/openapi.json` on the live server. Typical ones:

- `get_address` (key_field `address`)
- `get_cluster` (key_field `cluster`)
- `get_tx` (key_field `tx_hash`)
- `list_tags_by_address` (key_field `address`)
