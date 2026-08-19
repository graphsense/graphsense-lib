# Security Policy

## Reporting a vulnerability

Please report security issues privately, **not** as a public GitHub issue.

- Preferred: [GitHub private vulnerability reporting](https://github.com/graphsense/graphsense-lib/security/advisories/new)
  on this repository.
- Alternative: email `contact@iknaio.com` with `SECURITY` in the subject.

Please include enough detail to reproduce: affected version or commit, the
component (library, REST API, CLI, MCP server, Python client), a proof of
concept, and the impact you believe it has. If you have a suggested fix, we are
glad to see it, but a clear report is far more valuable than a patch.

### What to expect

| Stage | Target |
| --- | --- |
| Acknowledgement of your report | 5 working days |
| Initial assessment (accepted / declined, with reasoning) | 10 working days |
| Fix released for an accepted High or Critical finding | 30 days |
| Fix released for an accepted Low or Medium finding | next scheduled release |

We publish a GitHub Security Advisory for every accepted finding, request a CVE
where one applies, and credit the reporter unless they ask us not to. We will
tell you before an advisory goes public. If a fix will take longer than the
targets above, we will say so rather than let the report go quiet.

We do not operate a paid bug bounty.

## Supported versions

Security fixes land on the latest released minor version of the `2.x` line and
are published to PyPI as `graphsense-lib`. Older minor versions do not receive
backports; please upgrade before reporting an issue against one.

## Scope

In scope:

- The Python library and CLI (`graphsenselib`).
- The REST API application (`graphsenselib.web`) and the MCP server.
- The published Docker images and their default entrypoints.
- The generated Python client under `clients/python/`.

Out of scope — these are expected, documented behaviours rather than defects:

- **Authentication and rate limiting are delegated to the API gateway.**
  `graphsenselib.web.security.get_api_key` extracts the `Authorization` header
  but deliberately does not validate it; a deployment is expected to front the
  application with a gateway (we use APISIX) that enforces credentials, quotas
  and per-client rate limits. Reports that an endpoint is reachable without a
  valid API key when the application is run bare are not treated as
  vulnerabilities on their own — but reports of an endpoint whose *cost* is
  disproportionate to the request that triggers it are, whether authenticated
  or not.
- Findings that require access to the Cassandra cluster, the tagstore database,
  or the configuration file. Those are trusted inputs; anyone holding them
  already holds the data.
- Vulnerabilities in third-party dependencies without a demonstrated impact on
  this project. Please report those upstream; we track dependency advisories
  through Dependabot.
- Anything that requires a user to run an untrusted `graphsense.yaml`, tagpack
  or plugin. Configuration and plugins are code, and are trusted as such.

## Hardening a deployment

The stock container is safe to run as shipped, but a production deployment
should additionally:

- Terminate TLS and enforce API keys and rate limits at the gateway.
- Keep the gateway's own request body limit at or below the application's
  `max_request_body_bytes` (default 8 MiB).
- Lower `max_bulk_items` (default 10,000) if the deployment serves untrusted
  callers and does not need long bulk requests.
- Run the REST API with a memory limit and more than one worker, so a single
  expensive request cannot take the service down.
