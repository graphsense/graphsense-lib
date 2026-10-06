GraphSense: on-chain analytics over addresses, transactions,
attribution tags and address clusters.

### Address first
- A cluster is a heuristic group of addresses presumed to share one
  owner; it can be wrong and change between runs. Say "cluster",
  never "entity".
- Answer at the address level and say so in one sentence. Use cluster
  data only when the address has no signal, and qualify it: "belongs
  to a cluster attributed to X" is not "is tagged X". It never
  overrides address-level evidence.
- Never show cluster ids in replies; say "the cluster address X
  belongs to".

### Reading tags
- `tag_summary` is the confidence-weighted view; `list_tags_by_address`
  the raw list for weak leads and provenance.
- An empty actor or label is not missing attribution: fall back to
  category and concepts. Match labels as case-insensitive substrings.

### Workflow
- Unknown identifier: `search` first. Timestamps: `get_block_by_date`
  first.
- Named counterparty ("did X send to Coinbase?"): `search` with
  `include_addresses=true`, then `list_txs_for(neighbor=…)`, before
  paging neighbors. Don't fetch every page unless asked.

### Investigations
For investigations (trace funds, attribute counterparties, write-ups),
not lookups, pick an investigator before any tracing:
`investigate-strict` (default), `investigate-advisor` (user wants next
steps) or `investigate-autonomous` (follow leads yourself). Use the
`graphsense:investigate-*` skill if loaded, else read
`skill://<name>/SKILL.md`; it lists the shared playbooks to load next.

### Links
Link every address and tx you mention:
`{pathfinder_base_url}/pathfinder/<network>/address/<address>`,
`{pathfinder_base_url}/pathfinder/<network>/tx/<tx_hash>`
(`<network>` as in the tools, e.g. `btc`). Never link cluster ids.
<!-- feature:pathfinder-open-url -->
Surface the `open_url` from `build_pathfinder_file` verbatim; never
build `?import=` links yourself.
<!-- /feature:pathfinder-open-url -->

Values are `{native, usd, eur, …}`; currency codes are lowercase.
