---
name: trace-funds
description: "How to trace funds hop by hop and attribute counterparties with the GraphSense tools without closing a trace on a guess - tags vs transfers, empty and truncated results, block windows, swaps, consolidation, and what counts as a terminal. Use when following the money from an address or transaction, or when attributing where funds went."
metadata:
  graphsense-tools: search get_statistics lookup_address lookup_cluster lookup_tx_details list_neighbors list_txs_for list_tx_flows list_tags_by_address get_actor get_block_by_date
---

# Tracing funds with GraphSense

## Tools

- `get_statistics` lists the indexed networks; `search` disambiguates an
  identifier or finds a named entity's addresses
- `lookup_address` / `lookup_cluster` for activity, balance and attribution
- `list_neighbors` / `list_txs_for` / `list_tx_flows` to trace fund movement
- `list_tags_by_address` / `get_actor` for per-tag provenance and actor detail
- `lookup_tx_details` for a single transaction
- `get_block_by_date` to turn a date into a block height

Follow the GraphSense server instructions for network slugs, pagination and
cluster discipline; they override this skill where they are more specific.
Omit optional parameters entirely rather than sending empty strings.

## Method

1. Identify the input type and network, and say which tool(s) you'll use.
2. Call tools; never invent balances, tags, or counterparties.
3. Summarize: balance and activity, first/last seen, top counterparties,
   entity attribution, notable risk indicators.
4. For a trace, REPORT THE CHAIN HOP BY HOP: for each hop the receiving
   address, amount and asset, date or block, tx hash where you have it, the
   tool call that produced it, and why the hop carries the traced funds
   (amount match, timing after the deposit). A terminal with no chain
   behind it is not a result, however confident you are about the entity.
5. If a tool errors or returns nothing, say so plainly - do not guess.

## Rules

- TAGS INFORM, TRANSFERS PROVE. Tags and clusters tell you WHO an address is
  and where to look next. They do not tell you WHERE THE FUNDS WENT, and they
  never discharge you from tracing. A tag on the address you were handed
  describes the subject, not the destination; it is a lead to confirm with
  transfers, never a terminal.
- AN EMPTY RESULT IS A FAILED QUERY, NOT A FINDING. If a transfer query
  returns nothing for an address that has activity, your parameters are
  wrong. Work the ladder: drop the block window; check you omitted unused
  filters; raise the page size; try the other listing (`list_neighbors`
  vs `list_txs_for`). Only when the unscoped query is also empty may you
  report no outgoing activity, and then name the queries you ran.
- NEVER CLOSE A TRACE ON AN UNQUERIED NEGATIVE. "No further outflows" or
  "the trail ends here" may only be said about an address whose outgoing
  transfers you actually fetched - name the query and what it returned.
  Otherwise call the hop UNRESOLVED. An honest "not followed further" is a
  finding; a fabricated dead end is a fabrication.
- DO IT NOW, DO NOT PROPOSE IT. If you can name the query that would resolve
  a hop of the asked task, run it before you answer. A query you named but
  did not run is the work itself, left undone. (How far to go *beyond* the
  asked task is set by the investigator's automation level.)
- Do not invent tool limitations. If unsure whether a tool can do
  something, call it and report what comes back.
- Every attribution you state must come from a call you made in THIS
  conversation. Inventing a plausible tag string is as serious as inventing
  a hash.
- Follow the SPECIFIC funds, not the busiest pipe. At every hop, pick the
  outgoing transfer(s) that match the traced amount and occur AFTER it
  (allowing for fees and splits) - never simply the largest aggregate
  outflow. A high-volume counterparty is usually unrelated traffic.
- Ignore dust and fee change: an output under roughly 1% of the traced
  amount is not the funds.
- Consolidation addresses do not end the trace and do not widen it to the
  address's whole activity: list its outgoing transfers dated after your
  funds arrived and follow the one(s) large enough to carry them. An
  unattributed recipient is still the next hop.
- Tags are not endpoints. A tagged address that sent the funds onward is a
  hop, not a terminal.
- A swap is a hop, never a terminus. Continue tracing the output asset from
  the same wallet; never report a DEX pool, router or token contract as the
  terminal.
- The terminal is where the traced party loses control of the value: an
  exchange DEPOSIT address (name it, not the hot wallet it sweeps to), a
  seized or sanctioned wallet, or a genuine, queried dead end. If several
  branches carry the funds, follow the dominant one and name the smaller
  branches you did not pursue.
- If the trail goes cold, say at which address and why, and cite the
  outgoing-transfer query you ran there and what it returned. Do not
  promote the last tagged service you saw to "terminal".
- A PAGE IS NOT THE RECORD. A result carrying `next_page`, or whose last row
  is older than the period asked about, is incomplete. Page through it or
  scope the query before saying anything about that period. Absence in
  page 1 is not evidence of absence.
- A BLOCK WINDOW IS A TRAP AT ITS EDGES. `get_block_by_date` maps a date to
  the block around midnight, so using it directly as an upper bound cuts off
  that whole final day - and funds usually move mid-day. Prefer unscoped
  queries filtered by row timestamps; when you must scope, take the block
  for the day AFTER your end date. When a scoped query comes back empty,
  re-run it unscoped first.
