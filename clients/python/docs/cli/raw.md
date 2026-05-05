# `graphsense raw` — full auto-mirrored API

`graphsense raw <group> <method> [args...]` exposes every method of every
non-deprecated `*Api` class in `graphsense.api` as a CLI subcommand. The
tree is built at CLI startup by introspecting the generated classes — no
hand-written mapping, no stale command list.

## Layout

```
graphsense raw addresses get-address btc 1A1z...
graphsense raw clusters  list-cluster-neighbors btc 123 --direction out
graphsense raw txs       get-tx btc a1b2c3...
graphsense raw general   search satoshi
```

Group names are the API class name lowercased with `Api` stripped
(`AddressesApi` → `addresses`). Method names are converted snake_case →
`dashed-case`. Both are always discoverable via `--help` at each level.

## Why it survives regeneration

The group and command list are built from `dir(graphsense)` and
`inspect.getmembers(cls, predicate=inspect.isfunction)` at CLI startup. New
endpoints added by the OpenAPI generator become new subcommands on the
next import — no changes in this package needed.

The regression test `tests/test_cli_raw_mirror.py` pins the current tree
shape: if the generator renames a method or drops one we depend on, a test
fails and points at the specific mismatch.

## Parameter mapping

| Method parameter        | CLI surface                                    |
| ----------------------- | ---------------------------------------------- |
| Required (`str`/`int`)  | Positional argument                            |
| Optional scalar         | `--flag VALUE`                                 |
| `bool`                  | `--flag / --no-flag`                           |
| `List[str]`             | Repeatable `--flag VALUE`                      |
| `Dict[str, Any]` / body | `--flag '<json>'` or `--flag @file.json`        |

Private params prefixed with `_` (`_request_timeout`, `_headers`, ...) are
omitted. The `*_with_http_info` and `*_without_preload_content` variants
are also filtered out.

## Deprecated endpoints

Deprecated API classes (currently `EntitiesApi`) are hidden by default.
Set the environment variable `GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1` to expose them:

```sh
GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1 graphsense raw entities --help
```

Even when hidden, calls that return RFC 8594 `Deprecation` / `Sunset`
headers from the server emit a one-line stderr warning (unless `--quiet`).
