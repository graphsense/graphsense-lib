# Regression tests

Regression suites for graphsense-lib. Two families:

- **Ingest / pipeline suites** (`tests/cassandra`, `tests/deltalake`,
  `tests/sink_consistency`, `tests/sink_catchup`, `tests/continuation`,
  `tests/transformation`, `tests/clustering`, `tests/delta_update`) — spin up
  testcontainers and compare current code against a reference release or a
  second code path. See `make help` for the `test-ingest-*` targets.
- **REST API suites** (`tests/rest`) — compare two API servers
  endpoint-by-endpoint.

This directory is its own uv project; run everything from here.

## Quick start (REST)

```bash
make install

# compare production against your working tree, quick pass
make rest REF=api.iknaio.com CUR=local DEPTH=quick
```

`make rest` without arguments prints the full help, including what `REF`/`CUR`
accept (git tag/branch/commit, `local`, `api.iknaio.com`, `api.test.iknaio.com`,
any http(s) URL) and expected runtimes per depth:

| DEPTH | Suites | Calls | Runtime |
|-------|--------|-------|---------|
| `quick` | manual (hand-written edge cases) | ~40 | ~1–2 min |
| `standard` | quick + fuzz (endpoint family sweep) | ~85 | ~3–5 min |
| `full` | standard + loki (replayed production requests, `LOKI_WORKERS` parallel, default 8) | ~14k | ~15–30 min |

Sides given as git refs or `local` are built into Docker images
(`gslib-rest:<sha>`) and served locally on ports 19100/19101; images and
running servers are reused across runs. `*.iknaio.com` sides need `GS_API_KEY`
in the environment. Stop local servers with `make rest-stop`.

`make generate-loki` regenerates `tests/rest/test_loki_generated.py` from
production Loki logs (requires `LOKI_URL`); the file is gitignored, so a
fresh checkout needs this once before `DEPTH=full`. To run a single suite
directly, invoke pytest with the `CURRENT_SERVER`/`BASELINE_SERVER` env vars
described below.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `REST_CONFIG_FILE` | `../../instance/config.yaml` | Config mounted into locally served sides |
| `REST_CURRENT_PORT` | 19100 | Port for a locally served current side |
| `REST_BASELINE_PORT` | 19101 | Port for a locally served reference side |
| `GS_API_KEY` | — | API key for hosted `*.iknaio.com` sides |
| `TAGSTORE_URL` | — | Used to derive the tagstore DSN for local sides |
| `REBUILD` | — | `1` forces a docker rebuild of git-ref/local sides |

The pytest layer itself is driven by `CURRENT_SERVER` / `BASELINE_SERVER`
(plus `*_AUTH` and `*_HEADERS` as JSON) — `scripts/rest_suite.sh` sets these
for you; export them yourself only when bypassing it.

Deployment-context caveats when one side is a hosted deployment (gateway
headers, obfuscation plugin, timezone, tagstore DSN) are handled by the
runner / `instance/config.yaml`; details in
`tests/rest/test_baseline_regression.py` comments.

## Reports

Every REST run writes `reports/regression_timing_report.json` with
per-endpoint timing comparisons, speedup/slowdown analysis, and
pattern-based grouping.
