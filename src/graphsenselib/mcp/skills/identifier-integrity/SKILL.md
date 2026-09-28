---
name: identifier-integrity
description: "Hard rules for writing crypto addresses, transaction hashes, cluster ids, xpubs and links into answers without fabricating or corrupting them. Use whenever an answer, figure or export will contain an on-chain identifier taken from GraphSense tool results."
---

# Identifier integrity

These are hard rules; never break them. A malformed identifier sends an
investigator to an address that does not exist, and it is the single worst
error you can make.

- Every address, transaction hash, cluster id, or xpub you write must be
  copied character for character from a tool result in THIS conversation or
  from the user's own message. Never write an identifier from memory or
  training data.
- Never complete, expand, or "fix" a truncated identifier. Do NOT turn
  0xabc...123 into a full hash. If you only have a truncated form, fetch the
  full identifier with a tool. If no tool returns it, describe the item in
  words and say the identifier could not be retrieved.
- Need a tx hash you do not have? Get it from a tool first (e.g.
  `list_txs_for`). No tool result, no hash.
- COUNT THE CHARACTERS. An EVM address is 0x + exactly 40 hex digits (42
  total); a tx hash is 0x + exactly 64 (66 total). Hex is 0-9 and a-f only,
  with no "..." in the middle. A 39-, 41- or 65-character string is not a
  typo, it is a different address that does not exist.
- COPY AT THE MOMENT YOU READ IT. Paste each identifier straight from the
  tool result that produced it. Do not carry it in your head across a long
  trace and retype it later, and never rebuild one from a shortened form
  you wrote earlier - transcription drifts the further you get from the
  tool call.
- AN OMITTED HASH IS FINE, A WRONG ONE IS NOT. If you cannot reproduce an
  identifier exactly, write "hash not retained" (or "address not retained")
  and describe the hop in words - amount, asset, date, direction. Never
  approximate, never fill in a plausible tail.
- Links are identifiers too. Copy a `download_url` or `open_url` verbatim
  from the tool result; never construct one.
- Before building any export, re-check every identifier in it against the
  tool results in this conversation. Drop anything you cannot source, or
  mention it descriptively without an identifier.
