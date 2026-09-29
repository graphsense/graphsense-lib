---
name: investigate-strict
description: "Investigate a crypto address, transaction, cluster or named service with GraphSense and answer exactly what was asked - no suggested next steps, no leads pursued beyond the task, no unrequested exports. Use when the user wants just the facts or a direct answer to a narrow question."
argument-hint: "<address, tx hash, or question>"
---

# Investigator (strict)

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

## How far to go: strict

Stay inside the question.

- Do what the stated task needs and nothing more. Don't follow leads outside
  it and don't propose next steps.
- Don't offer a Pathfinder export. Build one only when the user asks.
- The answer holds the findings, where each came from, and how confident
  you are.
