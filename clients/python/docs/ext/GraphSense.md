# `graphsense.ext.GraphSense`

The single facade class. Instantiating it once gives you a configured
`ApiClient`, the high-level bundlers (`lookup_*`, `search`, `statistics`,
`block`, `exchange_rates`, `actor`, `tags_for`, `bulk`), and raw access to
every non-deprecated generated API through `.raw`.

## Constructor

```python
GraphSense(
    api_key: str | None = None,
    host: str | None = None,
    currency: str | None = None,
    *,
    api_client: ApiClient | None = None,
    quiet_deprecation: bool = False,
    show_deprecated: bool = False,
    max_workers: int = 8,
)
```

| Arg                  | Purpose                                                                   |
| -------------------- | ------------------------------------------------------------------------- |
| `api_key`            | GraphSense API key. If omitted, tries `GRAPHSENSE_API_KEY`, `IKNAIO_API_KEY`, `GS_API_KEY`, `API_KEY` (in that order). |
| `host`               | Base URL. If omitted, tries `GRAPHSENSE_HOST`, `IKNAIO_HOST`, `GS_HOST`; otherwise the default baked into `Configuration`. |
| `currency`           | Default currency for calls that take one. Can be overridden per call.      |
| `api_client`         | Provide an existing `ApiClient` (useful for tests/mocks).                  |
| `quiet_deprecation`  | Suppress the stderr warning installed by the deprecation hook.             |
| `show_deprecated`    | Expose deprecated API groups on `.raw` (default: hidden).                  |
| `max_workers`        | Parallelism for bundlers.                                                  |

## Bundlers

All bundlers accept the primary identifier plus a default-False `with_*` flag
per auxiliary call and return a `Bundle`:

- `lookup_address(address, currency=None, *, with_tags=False, with_cluster=False, with_tag_summary=False, include_actors=True)`
- `lookup_cluster(cluster_id, currency=None, *, with_tag_summary=False, with_top_addresses=False)`
- `lookup_tx(tx_hash, currency=None, *, with_io=False, with_flows=False, with_upstream=False, with_downstream=False)`

## Passthroughs

- `search(query, currency=None)`
- `statistics()`
- `block(height, currency=None)`
- `exchange_rates(height, currency=None)`
- `actor(actor_id)`
- `tags_for(address, currency=None)`

## Bulk

```python
gs.bulk(operation, keys, currency=None, *, format="json", num_pages=1, key_field="address")
```

Hits `/bulk.json/<operation>` (or `/bulk.csv/<operation>`). `key_field` must
match the parameter name the operation expects in its request body
(`address`, `tx_hash`, `cluster`, ...). See
[the CLI bulk guide](../cli/bulk.md) for the shape of the response.

## `.raw`

Auto-populated namespace of every `*Api` class in `graphsense.api`. The
attribute name is the API group name lowercased, with the `Api` suffix
stripped:

| `.raw` attribute | Class              |
| ---------------- | ------------------ |
| `addresses`      | `AddressesApi`     |
| `blocks`         | `BlocksApi`        |
| `bulk`           | `BulkApi`          |
| `clusters`       | `ClustersApi`      |
| `general`        | `GeneralApi`       |
| `rates`          | `RatesApi`         |
| `tags`           | `TagsApi`          |
| `tokens`         | `TokensApi`        |
| `txs`            | `TxsApi`           |
| `entities` *     | `EntitiesApi` (deprecated; only visible with `show_deprecated=True`) |

Because `.raw` is built by introspection, every future API class added by
the generator appears automatically — no code changes here.

## Context manager

```python
with GraphSense(api_key="...") as gs:
    addr = gs.lookup_address("btc", "1A...")
# underlying ApiClient closed here
```

## Deprecation hook

At construction time a wrapper is installed on `api_client.call_api` that
inspects `RESTResponse.headers` for RFC 8594 `Deprecation` / `Sunset`
headers. When present, a one-line warning is printed to `stderr` once per
unique `METHOD + path`.
