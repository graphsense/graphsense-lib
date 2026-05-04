# Copilot Instructions for `graphsense-lib`

## Build, test, lint

- Use `uv` consistently for dependency management and command execution in this repository (e.g., `uv sync`, `uv run ...`).
- Install dependencies for local development:
  - `make dev` (installs deps and pre-commit hook)
  - or `make install-dev` (deps only)
- Build package: `make build`
- Lint: `make lint`
- Type-check: `make type-check`
- Format: `make format`
- Fast pre-commit gate: `make pre-commit`
- Test suite:
  - Local default: `make test`
  - CI-like (no slow tests): `make test-ci`
  - Base-dependency matrix variant: `make test-with-base-dependencies-ci`
- Run a single test:
  - `uv run --exact --all-extras pytest tests/path/to/test_file.py::test_name -x -vv`

## High-level architecture

- `src/graphsenselib/cli/main.py` is the main Click command collection (`graphsense-cli`), wiring subcommands from db, ingest, schema, delta update, rates, monitoring, watch, config, convert, plus optional tagpack/tagstore/web CLIs.
- Configuration is centralized in `src/graphsenselib/config/config.py` (`AppConfig`), with environment/keyspace models shared across CLI, ingestion, delta update, DB access, and web.
- Data access is split into raw/transformed keyspaces; `src/graphsenselib/db/factory.py` maps schema type (`utxo`, `account`, `account_trx`) to concrete DB/address/tx classes and returns an `AnalyticsDb`.
- Ingestion modules under `src/graphsenselib/ingest/` load blockchain data into raw keyspaces; delta update modules under `src/graphsenselib/deltaupdate/` derive transformed state from raw data.
- REST API is in `src/graphsenselib/web/`; `web/app.py` builds the FastAPI app, wires middleware/routes/services, and integrates async DB + tagstore dependencies.
- Tag metadata tooling is split between `src/graphsenselib/tagstore/` (database + web/admin components) and `src/graphsenselib/tagpack/` (import/validation/quality tooling), both exposed via CLI modules.
- Python client generation is tied to OpenAPI versioning via `clients/python/` and root `Makefile` targets.

## Key conventions specific to this repository

- Optional feature surfaces are dependency-gated: CLI modules for `web`, `tagpack`, and `tagstore` are conditionally imported and only registered when extras are installed.
- Keep schema type aligned with currency semantics (`eth -> account`, `trx -> account_trx`, others mostly `utxo`) and preserve raw/transformed keyspace prefix consistency (validated in config models).
- Web OpenAPI output is intentionally post-processed in `web/app.py` for backward compatibility (snake_case schema names, named union schemas, parameter example promotion) because client generation depends on it.
- REST app config resolution order is intentional: explicit `config_file` parameter > `CONFIG_FILE` env var > `./instance/config.yaml` > `.graphsense.yaml` `web` key > env-only configuration.
- For settings management, prefer Pydantic Settings models (`pydantic_settings.BaseSettings`) with typed fields and defaults, following existing patterns in `src/graphsenselib/web/config.py` and other config modules.
- Tests rely on Testcontainers (`CassandraContainer`, `PostgresContainer`) from `tests/conftest.py`; accelerated mode uses `DANGEROUSLY_ACCELERATE_TESTS=1` with prebuilt `graphsense/cassandra-test:4.1.4`.
- Release/version workflow uses two version tracks in `Makefile`: `RELEASESEM` (library tag `vX.Y.Z`, `vX.Y.Z-rc.N`, or optionally `vX.Y.Z-dev.N`) and `WEBAPISEM` (API/client tag `webapi-vA.B.C`), with dedicated sync/check targets for client version alignment.
- Library package version is dynamic via `setuptools_scm` (`version_scheme = "release-branch-semver"`, `local_scheme = "node-and-date"`):
  - on a tag, the wheel version is the tag verbatim (PEP 440 normalized): `vX.Y.Z -> X.Y.Z`, `vX.Y.Z-rc.N -> X.Y.ZrcN`, `vX.Y.Z-dev.N -> X.Y.Z.devN`;
  - off-tag commits get a computed dev version with a local segment: `X.Y+1.0.devN+g<sha>.d<date>` on `master`/`develop`/`main`, `X.Y.Z+1.devN+g<sha>.d<date>` on `release/X.Y.x` branches, and patch-bump (same as a release branch) on other feature branches;
  - dirty workspaces append `.dirty` to the local segment, so `make build` from uncommitted changes is honest about what it contains.
- Dev builds use **branch-pushed Docker images**, not hand-minted git tags: every push to `master`, `develop`, or `feature/**` produces ghcr.io images tagged with the branch slug (rolling) and short SHA (immutable, pin-friendly). The `develop` branch also gets the rolling `dev` alias. Operators pin to the SHA tag; developers track the branch tag.
- Mint a `vX.Y.Z-dev.N` git tag only when you want a clean, named ref (e.g. for cross-repo Python pinning or a milestone snapshot). Use plain SemVer pre-release form — do not use `+<branch>.N` build metadata. For routine dev work, branch-pushed Docker images are sufficient.
- When stabilizing a minor (backporting fixes to `2.12.x` after `2.13.0` ships), cut a `release/2.12.x` branch — `release-branch-semver` then computes patch-bump dev versions on that branch instead of a minor bump.
- Major-version bumps (e.g. `3.0.0`) require an explicit tag — `release-branch-semver` does not auto-detect majors.
- CI trigger behavior is intentional:
  - `vX.Y.Z` tag: creates GitHub Release, publishes Python library package, and publishes Docker images (`vX.Y.Z` + rolling `latest`).
  - `webapi-vA.B.C` tag: publishes Python client package.
  - `vX.Y.Z-rc.N` tag: publishes Docker images only (`vX.Y.Z-rc.N` + rolling `rc`); no GitHub Release, no Python package publish.
  - `vX.Y.Z-dev.N` tag: publishes Docker image with the exact tag only (no rolling alias, since branch tags now serve that purpose); no GitHub Release, no Python package publish.
  - Branch push to `master` / `develop` / `feature/**`: publishes Docker image tagged with branch slug + short SHA; `develop` additionally publishes the rolling `dev` alias.
