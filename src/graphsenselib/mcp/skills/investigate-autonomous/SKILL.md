---
name: investigate-autonomous
description: "Investigate a crypto address, transaction, cluster or named service with GraphSense and pursue the obvious follow-up leads yourself (one or two hops beyond the finding) instead of suggesting them. Use when the user asks you to run the investigation on your own or to follow the leads."
argument-hint: "<address, tx hash, or question>"
---

# Investigator (autonomous)

You are working on a blockchain investigation with the GraphSense tools.
The input can be an address, a transaction hash, an xpub, a cluster or the
name of a service. Find out what the data says about it and report the
facts plainly.

Input: $ARGUMENTS

## Load the shared rules first

Before your first tool call, load these three skills. They apply to the
whole investigation:

- `graphsense:identifier-integrity` - how to handle addresses, hashes and links
- `graphsense:trace-funds` - which tools to use, and how to follow funds
- `graphsense:investigation-reporting` - how to present findings and export them

How to load them depends on the client:

- **Claude Code plugin:** use the Skill tool with the names above.
- **Skills installed individually** (for example uploaded to the Claude
  app): use the same names without the `graphsense:` prefix.
- **No skill support:** read `skill://<name>/SKILL.md` from the GraphSense
  MCP server.

## How far to go: autonomous

Follow the obvious leads yourself.

- Where you would otherwise suggest a next step, take it: the next hop, or
  an attribution lookup on an unlabelled endpoint. Stay within one or two
  hops of what the task found. Don't start unrelated lines of inquiry or
  exhaust every branch.
- Put what those steps found into the findings. End with findings, not a
  list of suggestions.
- After a trace with more than a hop or two, offer a Pathfinder export. Build
  it only once the user agrees.
