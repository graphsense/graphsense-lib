# Graph export format (v0, experimental)

`POST /graph/convert` and `graphsense-cli convert gs-files convert` export a
Pathfinder graph to other graph formats. The code is
`src/graphsenselib/convert/graph_convert/`. This page describes the data
model behind every format, and how GraphML writes it.

**Status: v0, unstable.** Attribute names, ids and rules may still change
without a deprecation cycle. Every document says which version it uses
(`format_version`, currently `0`), and GraphML attribute names carry it
(`gs:v0:*`).

## Principles

- **One model, many formats.** A writer only serializes the model; it
  computes nothing. Every format gets the same ids, units and rules.
- **Lossless by default.** `tx_mode=node` keeps every transaction and every
  input/output that touches the graph. `tx_mode=aggregate` is computed from
  that, never stored.
- **Chain data and vendor assessments stay apart.** Attribution comes in its
  own block (`attribution.*`) with its source. Clusters are a heuristic and
  are never exported.
- **Visuals are optional.** Positions, colors and annotations come from the
  Pathfinder file; consumers may ignore them.

## Conventions

| What | Representation |
| --- | --- |
| Network | GraphSense code (`btc`, `eth`, …) in `network`; ids use CAIP-2 |
| Address node id | CAIP-10: `<caip2>:<address>` |
| Tx node id | `<caip2>:tx:<identifier>` (not a CAIP standard) |
| Address spelling | EVM: lowercase hex, no EIP-55 casing. Everything else, Tron base58 included, as GraphSense spells it |
| Amount | Integer in the asset's base units, **as a decimal string**. No floats; no fiat |
| Asset | `asset`: GraphSense code (`btc`, `eth`, `usdt`); `asset_id`: CAIP-19, native assets only for now |
| Timestamp | UTC, ISO 8601, second precision, `Z` suffix: `2023-07-22T04:26:40Z` |
| UTXO output | `outpoint` = `<tx_hash>:<vout>` |
| Account-model sub-transfer | its own tx node; `identifier` = `<hash>_T<n>` (token) / `<hash>_I<n>` (internal), `sub_index` = `T<n>` / `I<n>` |

CAIP-2 ids in use:

| network | CAIP-2 |
| --- | --- |
| btc | `bip122:000000000019d6689c085ae165831e93` |
| bch | `bip122:000000000000000000651ef99cb9fcbe` (fork block 478559, since BCH shares Bitcoin's genesis) |
| ltc | `bip122:12a765e31ffd4059bada1e25190f6e98` |
| zec | `bip122:00040fe8ec8471911baa1db1266ea15d` |
| eth | `eip155:1` |
| anything else (e.g. trx) | `graphsense:<code>`: unique, but not portable yet |

## Graph

| Attribute | Type | |
| --- | --- | --- |
| `format_version` | string | `0` |
| `name` | string | graph name from the file |
| `source` | string | `graphsense-pathfinder` |
| `tx_mode` | string | `node` or `aggregate` |
| `warnings` | string | one per line: lookups that failed, txs left out of the aggregate view |

## Nodes

### `kind=address`

| Attribute | Type | |
| --- | --- | --- |
| `network`, `address` | string | |
| `starting_point` | boolean | marked as a starting point in Pathfinder |
| `annotation`, `color` | string | user label; `#rrggbbaa` |
| `x`, `y` | double | Pathfinder position |
| `attribution.*` | | only with `include=tags`, see below |

### `kind=tx` (`tx_mode=node` only)

| Attribute | Type | |
| --- | --- | --- |
| `network`, `tx_hash`, `identifier`, `sub_index` | string | |
| `resolved` | boolean | false: the backend didn't know the tx or the lookup failed. Then only identity and visuals are set, and the tx has no flow edges |
| `tx_model` | string | `utxo` or `account` |
| `height` | long | |
| `timestamp` | string | |
| `asset`, `asset_id` | string | |
| `value` | string | account model: the amount transferred |
| `total_input`, `total_output` | string | UTXO |
| `no_inputs`, `no_outputs` | long | UTXO, over the whole tx |
| `input_address_count` | long | UTXO: distinct sender addresses over all inputs. An input with no address, or with several, counts as one sender of its own |
| `coinbase` | boolean | UTXO |
| `starting_point`, `annotation`, `color`, `x`, `y` | | as for addresses |

A tx none of whose inputs or outputs touches an address of the graph is
still exported, as a node without edges.

### Attribution (`include=tags`)

An address's own tags: the tag summary of
`GET /{network}/addresses/{address}/tag_summary`, without cluster tags. It is
absent when the address has no tags.

| Attribute | Type | |
| --- | --- | --- |
| `attribution.source` | string | `graphsense-tagstore` |
| `attribution.label` | string | best label |
| `attribution.actor` | string | best actor id |
| `attribution.category` | string | broad category |
| `attribution.confidence` | double | the best label's confidence, 0..1 |
| `attribution.concepts` | string | the best label's concepts, space-separated |
| `attribution.tag_count` | long | |

## Edges

### `tx_mode=node`

| kind | source → target | Attributes |
| --- | --- | --- |
| `input` | address → tx | `network`, `tx`, `value`, `asset`, `asset_id`, `io_index` (vin), `address_count` |
| `output` | tx → address | the same, plus `outpoint`; `io_index` is the vout |
| `agg` | address → address | `network`, `directed=false`, `txs` (space-separated tx node ids) |
| `conversion` | input-leg tx → output-leg tx | `conversion_type` (`dex_swap`, `bridge_tx`), `input_tx`, `output_tx`, `from_address`, `to_address` |

- There is one input or output edge per input or output, not per address:
  an address that spends two inputs gets two edges.
- Only inputs and outputs that touch an address of the graph become edges.
  The tx node's totals and counts cover the rest.
- An input or output that names several addresses (bare multisig) gives
  one edge per address on the graph. Each edge carries the **full** value
  and `address_count`, so summing such edges counts the value twice.
- Inputs carry no outpoint: the tx body does not say which output an input
  spends.
- `agg` edges are the address-to-address edges drawn in Pathfinder.
  Pathfinder does not store their direction, so `source`/`target` are only
  the order of the file.
- A `conversion` edge's `from_address` is the first output of the input leg
  and its `to_address` the first input of the output leg, as Pathfinder
  draws them. It is exported only when both legs and both addresses are on
  the graph.

### `tx_mode=aggregate`

Tx nodes and input/output edges are replaced by `kind=flow`. There is one
flow edge per (network, sender, receiver, asset): every pair of an on-graph
input address and an on-graph output address of a tx. Values are never
summed across assets or networks.

| Attribute | Type | |
| --- | --- | --- |
| `network`, `asset`, `asset_id` | string | |
| `value` | string | total sent from sender to receiver, **only when exact** |
| `value_determined` | boolean | whether `value` is present |
| `tx_count` | long | |
| `first_timestamp`, `last_timestamp` | string | |
| `txs` | string | space-separated tx node ids |

- **Values are exact or absent, never estimated.** A UTXO tx does not say
  which input paid which output. Its payment from one sender to one
  receiver is therefore known only when the tx has a single sender
  (`input_address_count = 1`); then it is the sum of that receiver's
  outputs. An account-model transfer is always exact. A pair's `value` is
  set only when every one of its txs is exact.
- A sender paying itself (UTXO change, account self-transfer) is no flow
  edge. A tx that connects no two different addresses of the graph drops
  out, and a graph warning says how many did. `tx_mode=node` keeps them.
- `agg` edges are kept unchanged. `conversion` edges run from `from_address`
  to `to_address`.

## GraphML

- An attribute `<name>` becomes a `<key>` with `attr.name="gs:v0:<name>"`
  and its key id is `<n|e|g>.gs.v0.<name>`. Types: string, boolean, long,
  double.
- Nodes also carry a plain `label` key (the annotation, or the address or
  tx id), which yEd, Gephi and Cytoscape show by default.
- The graph is directed (`edgedefault="directed"`), and `agg` edges are
  too, with `gs:v0:directed=false`. GraphML allows mixed graphs, but
  networkx, and with it many importers, rejects them.
- Only the keys a document uses are declared, in first-use order, so a
  given graph always gives the same bytes.
- Parallel edges are normal (two inputs from one address). networkx reads
  the document as a `MultiDiGraph`.
- Media type `application/graphml+xml`, extension `.graphml`.

## Not (yet) in the model

- Inputs and outputs of addresses that are not on the graph, except as tx
  totals.
- CAIP-19 ids of tokens, which need the token contract address.
- A CAIP-2 id for Tron (`graphsense:trx` for now).
- Cluster data of any kind.
- Fiat values.
