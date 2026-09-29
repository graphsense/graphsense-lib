---
name: investigation-reporting
description: "How to write up blockchain investigation findings - the subject is a third party, every fact carries its source, High/Medium/Low confidence, no legal judgment, an AI-generated notice, and Pathfinder exports via build_pathfinder_file. Use when presenting GraphSense findings to a user, or exporting a traced graph."
metadata:
  graphsense-tools: build_pathfinder_file
---

# Reporting investigation findings

## Whose wallet this is

The reader is an investigator, and the wallet belongs to someone else. The
user does not control the funds.

- Refer to the subject as "the address", "the subject" or "the wallet", never
  as "you" or "your".
- Give no privacy, security or remediation advice. Anything about how easy
  the subject is to trace, such as CoinJoin use, is a finding about the
  subject, not a recommendation to the reader.

## Sourcing

Tie every fact to the tool call that produced it and, where it matters, to
what was queried, for example "`list_neighbors` on <address>". Someone
reading the write-up must be able to redo each call and arrive at the same
path.

## Wording

- Describe what the data shows and leave legal or moral conclusions to the
  reader. Don't call a person or address criminal, fraudulent, illicit or
  similar. Attribute a characterisation to its source: "tagged in GraphSense
  with a court-case label" rather than "a criminal's wallet".
- Keep three things visibly apart: what the chain records, what a tag
  claims, and what you conclude from them.
- Use plain, measured verbs such as "sent", "received", "is tagged as".
- Write "approx." rather than "~", which Markdown can turn into
  strikethrough.

## Confidence

Give each material conclusion one of three levels and a short reason. Use
the same scale throughout an answer.

| level | means |
| --- | --- |
| **High** | tool output shows it directly, with matching amounts and times |
| **Medium** | well supported, with one gap: an unlabelled hop, partial attribution, an estimated amount |
| **Low** | inferred, or resting on indirect evidence |

## AI-generated notice

Close every final answer with a short notice: it was produced by an AI,
may be wrong, and its identifiers and conclusions need independent checking
before anyone relies on them.

## Pathfinder export

`build_pathfinder_file` turns the traced graph into a file the user can open
in Pathfinder.

1. Before calling it, tell the user the export is AI-generated and must be
   checked independently, and wait for them to agree.
2. Include only identifiers that satisfy `identifier-integrity`.
3. Pass on `download_url` exactly as returned. If there is an `open_url`,
   give it exactly as returned as the "Open in Pathfinder" link. Don't build
   either link yourself.
