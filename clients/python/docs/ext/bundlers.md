# Bundler matrix

Each `with_*` flag on a `GraphSense.lookup_*` call maps to exactly one
underlying REST endpoint from the generated client. All auxiliary calls
run in parallel via a `ThreadPoolExecutor`; the primary call runs first
so its result can feed cluster id lookups when needed.

## `lookup_address`

| Flag                | Underlying call                                     | Bundle attr   |
| ------------------- | --------------------------------------------------- | ------------- |
| *(always)*          | `AddressesApi.get_address(currency, address)`        | `data`        |
| `with_tags=True`    | `AddressesApi.list_tags_by_address(currency, addr)`  | `tags`        |
| `with_tag_summary=True` | `AddressesApi.get_tag_summary_by_address(...)`   | `tag_summary` |
| `with_cluster=True` | `ClustersApi.get_cluster(currency, <base.cluster>)`  | `cluster`     |

Note: the cluster bundle uses the **non-deprecated** `ClustersApi.get_cluster`
route, keyed off the primary Address's `cluster` field — *not* the deprecated
`AddressesApi.get_address_entity`.

## `lookup_cluster`

| Flag                     | Underlying call                                      | Bundle attr       |
| ------------------------ | ---------------------------------------------------- | ----------------- |
| *(always)*               | `ClustersApi.get_cluster(currency, cluster_id)`       | `data`            |
| `with_tag_summary=True`  | `ClustersApi.get_tag_summary_by_cluster(...)` (if present) | `tag_summary` |
| `with_top_addresses=True`| `ClustersApi.list_cluster_addresses(...)`             | `top_addresses`   |

## `lookup_tx`

| Flag                     | Underlying call                      | Bundle attr   |
| ------------------------ | ------------------------------------ | ------------- |
| *(always)*               | `TxsApi.get_tx(currency, tx_hash)`    | `data`        |
| `with_io=True`           | `TxsApi.get_tx_io(...)`               | `io`          |
| `with_flows=True`        | `TxsApi.list_tx_flows(...)`           | `flows`       |
| `with_upstream=True`     | `TxsApi.get_spending_txs(...)`        | `upstream`    |
| `with_downstream=True`   | `TxsApi.get_spent_in_txs(...)`        | `downstream`  |

Note: the `/spending` endpoint is the *backward* trace (counter-intuitive naming
in the underlying REST API).

## Response contract

Every `Bundle.data` is a typed, generated pydantic model. Every auxiliary
attribute is either the typed model returned by the corresponding API or
`None` if the flag wasn't set.

`Bundle.to_dict()` flattens `data` and merges each non-`None` auxiliary as
top-level keys (`tags`, `cluster`, `tag_summary`, ...). Use it when you want
JSON output; prefer attribute access when you need typed introspection.
