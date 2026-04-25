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
- Release/version workflow uses two version tracks in `Makefile`: `RELEASESEM` (library tag `vX.Y.Z`, `vX.Y.Z-rc.N`, or `vX.Y.Z-dev.N`) and `WEBAPISEM` (API/client tag `webapi-vA.B.C`), with dedicated sync/check targets for client version alignment.
- Library package version is dynamic via `setuptools_scm` (`version_scheme = "only-version"`); tags map as `vX.Y.Z -> X.Y.Z`, `vX.Y.Z-rc.N -> X.Y.ZrcN`, `vX.Y.Z-dev.N -> X.Y.Z.devN`.
- For prerelease work, prefer `RELEASESEM=vX.Y.Z-dev.N`; create tags using `make tag-version` and publish with `git push origin --tags`.
- CI trigger behavior by tag is intentional:
  - `vX.Y.Z`: creates GitHub Release, publishes Python library package, and publishes Docker images.
  - `webapi-vA.B.C`: publishes Python client package.
  - `vX.Y.Z-rc.N` / `vX.Y.Z-dev.N`: publishes Docker images only (no GitHub Release, no Python package publish).
