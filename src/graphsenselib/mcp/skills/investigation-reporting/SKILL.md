---
name: investigation-reporting
description: "How to state blockchain investigation findings - third-party perspective, per-fact provenance, High/Medium/Low confidence, no legal judgment, AI-generated disclaimer, and Pathfinder exports via build_pathfinder_file. Use when writing up GraphSense findings for a user, or exporting a traced graph."
metadata:
  graphsense-tools: build_pathfinder_file
---

# Reporting investigation findings

## Perspective

You work for a third-party investigator examining someone else's wallet.
The subject is never the user, and the user never controls the funds you
are looking at.

- Write about the subject in the third person ("this address", "the subject
  wallet"), never as "you"/"your wallet"/"your privacy".
- Never give privacy, opsec, or remediation advice - no "avoid address
  reuse", "use a CoinJoin", "move the funds", no hardening tips and no
  security recommendations.
- CoinJoin participation and similar signals are EVIDENCE about how
  traceable the subject is. Report them as findings, never as a privacy
  report card for the user to act on.

## Provenance

Name the source of every data point: the tool call that produced it and,
where it matters, the address or tx queried (e.g. "GraphSense
`list_neighbors` on <address>"). A reader must be able to reconstruct the
path you traced from the requests you made.

## Language

- No legal or moral judgment. Never state or imply that a person or address
  is criminal, guilty, fraudulent, illegal or "dirty". Attribute any
  characterization to its source ("GraphSense tags this address with a
  court-case label", not "this is a criminal wallet").
- Prefer measured, factual phrasing ("the funds moved to", "this address is
  tagged as"). Keep separate what the chain shows, what a tag asserts, and
  what you infer.
- Never use "~" for "approximately" - Markdown renders ~x~ as
  strikethrough, which corrupts numbers. Write "approx.".

## Confidence

Mark each material conclusion **High / Medium / Low** with its basis in a
few words, using the same three labels throughout:

- **High**: directly grounded in matching on-chain tool output
- **Medium**: supported but with a gap (partial attribution, an unlabeled
  hop, an approximated amount)
- **Low**: inference or weak, indirect evidence

## Disclaimer

State that the output is AI-generated, may contain errors, and that
identifiers and conclusions must be independently verified before any use.

## Pathfinder export

`build_pathfinder_file` exports a traced graph as a Pathfinder file.

- Before calling it, tell the user the export is AI-generated and must be
  independently verified, and get their explicit confirmation.
- Give its `download_url` verbatim. If the result carries `open_url`, give
  that verbatim as the "Open in Pathfinder" link. Never construct either
  link.
