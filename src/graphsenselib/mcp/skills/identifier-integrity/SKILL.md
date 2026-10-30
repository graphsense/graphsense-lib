---
name: identifier-integrity
description: "Hard rules for writing crypto addresses, transaction hashes, xpubs and export links into answers without fabricating or corrupting them. Investigators load it; in an investigation, pick an investigator first (investigate-strict by default). Outside one, use whenever an answer, figure or export will contain an on-chain identifier taken from GraphSense tool results."
metadata:
  graphsense-tools: list_txs_for build_pathfinder_file
---

# Identifier integrity

An investigator will paste every identifier you write into another tool. One
wrong character points them at an address that has nothing to do with the
case, so a corrupted identifier does more damage than any other mistake. The
rules below have no exceptions.

## Where an identifier may come from

Only two sources count: a tool result in this conversation, or the user's own
message. Anything else - recall, training data, a pattern that "looks right" -
is off limits.

- Missing a transaction hash you want to cite? Look it up first (for example
  with `list_txs_for`). If no tool returns it, leave it out.
- Do not put cluster ids in replies at all. Refer to a cluster through an
  address that belongs to it, as the GraphSense server instructions require.

## Abbreviated identifiers

Never expand an abbreviated form such as `0x3f5c...9a1e` into a full value,
and never correct what looks like a typo. Fetch the full value with a tool. If
that fails, describe the item in words and say its identifier could not be
retrieved.

## Copying

Take each identifier from the tool result it appeared in, at the point where
you use it. Across a long trace, text you retype from memory or rebuild from a
shortened form you wrote earlier drifts, and a drifted value looks exactly like
an invented one.

## Checking the format

Before an identifier goes into an answer or an export, check its length and
alphabet against its network's format. On EVM chains, for example:

| kind | shape | length |
| --- | --- | --- |
| address | `0x` + 40 hex digits | 42 |
| tx hash | `0x` + 64 hex digits | 66 |

Hex digits are `0-9a-f`. Upper-case `A-F` appears only in checksummed
addresses. A value one character short or long is a different identifier, not
a near miss. UTXO addresses (base58, bech32) have no fixed length, so the
copying rule above is the safeguard there.

## When you cannot reproduce it exactly

Leave it out. Put "[tx hash omitted]" or "[address omitted]" in its place and
describe the transfer by date, direction, asset and amount. A gap is acceptable; a
guess is not. Never pad, round or complete an identifier.

## Links

- A `download_url` or `open_url` returned by `build_pathfinder_file` is an
  identifier. Copy it exactly as returned and never assemble one yourself.
- Address and transaction deep links follow the URL pattern in the GraphSense
  server instructions, filled with identifiers that meet the rules above.

## Before an export

Check every identifier in a Pathfinder export against the tool results in this
conversation. Remove anything you cannot trace to a result, or keep it only as
a description without the identifier.
