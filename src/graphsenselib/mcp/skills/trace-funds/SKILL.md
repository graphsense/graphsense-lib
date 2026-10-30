---
name: trace-funds
description: "How to follow funds hop by hop and attribute counterparties with the GraphSense tools without ending a trace on a guess - tags vs transfers, empty and partial results, date-to-block edges, swaps, consolidation, and what counts as an endpoint. Not a starting point: investigators load it. To trace funds or attribute where they went, pick an investigator first (investigate-strict by default)."
metadata:
  graphsense-tools: search get_statistics lookup_address lookup_cluster lookup_tx_details list_neighbors list_txs_for list_tx_flows list_tags_by_address get_actor get_block_by_date
---

# Tracing funds with GraphSense

The GraphSense server instructions cover network slugs, pagination and
cluster discipline. Where they say something more specific than this skill,
follow them. In particular, trace at the address level unless the user asks
about the cluster.

## Which tool

| need | tool |
| --- | --- |
| which networks are indexed | `get_statistics` |
| resolve an identifier, or find a named service's addresses | `search` |
| balance, activity and attribution of an address or cluster | `lookup_address`, `lookup_cluster` |
| one transaction in detail | `lookup_tx_details` |
| counterparties and the transfers between them | `list_neighbors`, `list_txs_for` |
| internal transfers inside an EVM transaction | `list_tx_flows` |
| individual tags and where they came from | `list_tags_by_address`, `get_actor` |
| a date as a block height | `get_block_by_date` |

Leave out an optional parameter you don't need. Don't send it as an empty
string.

## Workflow

1. Work out the input type and network, and tell the user which tools you
   will start with.
2. Get every figure, tag and counterparty from a tool call. When a call fails
   or returns nothing, say so; don't fill the gap.
3. For a lookup, cover balance and activity, first and last activity, main
   counterparties, attribution, and any tag categories that signal risk. Name
   the source of each tag.
4. For a trace, write out the path from the start to each endpoint. For every
   hop give the date or block, the asset and amount, where it went, the tx
   hash if you have it, and the tool call that produced it. Add one line on
   why this transfer carries the traced funds, such as a matching amount or
   timing after the incoming transfer. An endpoint without a path to it is not
   a trace result.

## Attribution is not movement

A tag or cluster label says who probably controls an address. It says nothing
about where a given amount went next. Look tags up whenever they help, but
establish every hop from transfers.

- A tag on the starting address describes the subject. "The subject has used
  a mixer" is a lead to check against transfers. It is not a destination.
- A tagged address that forwarded the funds is a hop, whatever the tag says.
- Every tag or label you report must come from a call in this conversation.
  A made-up tag is as bad as a made-up hash.

## Picking the next hop

- Match the traced amount, not the traffic. Choose the outgoing transfers
  that follow the incoming one in time and add up to about its value, net of
  fees and splits. The address's largest counterparty is usually unrelated.
- Skip outputs worth less than about 1% of the traced amount. They are change
  or dust.
- On a consolidation address with many unrelated depositors, take only the
  outflows after your funds arrived, and follow the ones large enough to
  contain them. Don't widen the trace to everything the address ever did.
- An unlabelled recipient is still a hop. Keep following it.
- For a swap through a DEX pool, router or token contract, keep tracing the
  asset that came out, from the same wallet. The pool, router or contract is
  never the endpoint.
- If the funds split, follow the largest branch and list the branches you did
  not follow.
- If you can name the call that would resolve a hop within the asked task,
  make that call before you answer. Don't write it up as a suggestion. How
  far to go *beyond* the task is set by the investigator's automation level.
- Don't claim a tool can't do something without trying it. Call it and
  report what it returns.

## Where a trace legitimately ends

A trace ends where the traced party stops controlling the funds:

- an exchange **deposit** address (report that address, not the hot wallet
  it is swept into),
- a seized or sanctioned wallet, or
- an address whose outgoing transfers you fetched and found empty, naming the
  call.

Any other stopping point is **unresolved**, and you must label it that way.
"Not followed further" is an honest finding. A dead end you never queried is
a fabrication, and so is promoting the last tagged service you saw to an
endpoint.

## An empty or partial result proves nothing yet

If a transfer listing comes back empty for an address that has activity,
suspect the query first:

1. remove any block or date bounds,
2. check that no unused filter was sent,
3. raise `pagesize`,
4. try the other listing (`list_neighbors` vs `list_txs_for`).

You may report that an address sent nothing onward only when the
unbounded query is empty too. List the calls behind that statement.

A result with a `next_page` cursor, or whose oldest row is still newer than
the period in question, is partial. You don't need every page. Fetch the
pages a claim about that period depends on, or narrow the query to it,
before making the claim.

## Date bounds

`get_block_by_date` returns the block at a point in time, and a bare date
means midnight at the start of that day. Passed as `max_height`, it drops
that whole day, which is often when the funds moved. Prefer unbounded queries
and filter on row timestamps. If you must bound by block, look up the next
day's block and use that as `max_height`. If a bounded query returns nothing, rerun it
unbounded before you conclude anything.
