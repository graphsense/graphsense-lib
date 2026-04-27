# Config consolidation — migration guide

## Why

`graphsenselib` as it exists today is the merger of several previously-standalone repositories — the ingest pipeline, the REST API (`graphsense-rest` / `gsrest`), the tagstore (`tagpack-tool` / `gs-tagstore`), the MCP server, and the async Cassandra client — each absorbed into this monorepo over time. Every one of those repos arrived with its *own* configuration loader, its *own* env prefix, and its *own* idea of what a YAML file should look like. Merging them gave us a single importable library, but the config surfaces were left side-by-side. That's how we ended up with **six independent configuration systems**:

| Legacy system | Origin | Loader | Env prefix |
|---|---|---|---|
| `AppConfig` | original graphsense-lib | `goodconf` | `GRAPHSENSE_` |
| `CassandraConfig` | async Cassandra client (`gs-cassandra`) | pydantic-settings | `GS_CASSANDRA_ASYNC_` |
| `TagStoreReaderConfig` | tagstore read path | pydantic-settings | `GRAPHSENSE_TAGSTORE_READ_` |
| `TagstoreSettings` | tagstore server (schema / writes) | pydantic-settings + `.env` | `gs_tagstore_` |
| `GSRestConfig` | `graphsense-rest` / `gsrest` | pydantic-settings | `GSREST_` |
| `GSMCPConfig` | MCP server | pydantic-settings | `GS_MCP_` |

Plus per-keyspace metadata stored in Cassandra `configuration` tables — unchanged here, since that one was never a problem.

**This sprawl was an integration artifact, not a design choice.** Each system made sense in its own original repo. Pulling them into one codebase without unifying the config layer is what left us unable to answer "where did this value come from?" without grepping across five prefixes and four loaders. The consolidation catches up with the merges: **one** model, **one** env prefix, **one** YAML loader, **one** debug command.

## The new shape

```
Settings (BaseSettings, env_prefix=GRAPHSENSE_, env_nested_delimiter=__)
├── cassandra: CassandraSettings         # single source of truth
├── tagstore:  TagStoreSettings          # single source of truth (url + pool)
├── web:       WebSettings               # REST-only fields; reads cassandra/tagstore from root
├── mcp:       MCPSettings               # MCP-only fields; reads cassandra/tagstore from root
├── keyspaces: dict[str, KeyspaceSettings]  # per-currency ingest topology for the loaded env
├── environment: str | None              # name of currently loaded env, informational
├── slack_topics, cache_directory, coingecko_api_key, …
└── legacy_web_dict                      # raw YAML `web:` block, for the loose fallback path
```

**No duplication, no environments dict.** Two structural changes from the legacy shape:

1. REST and MCP don't each carry their own `CassandraSettings` / `TagStoreSettings` — they share the root instance. (Before: `GSRestConfig.database` / `GSRestConfig.tagstore` nested copies.)
2. There is no `environments: {prod: {...}, dev: {...}}` dict. A `Settings` instance represents **one** environment. Switching environments is done at YAML-load time by loading a different overlay file — see *Layered YAML* below.

Using the new API:

```python
from graphsenselib.config import get_settings

s = get_settings()
s.cassandra.nodes            # List[str]
s.tagstore.url               # str
s.mcp.path                   # str
s.web.ALLOWED_ORIGINS        # str | List[str]
s.keyspaces["btc"]           # KeyspaceSettings (per-currency ingest topology)
s.environment                # currently-loaded env name (e.g. "prod")
```

## Migration tables

### Env variables

Every legacy env var still works during the deprecation window and emits a one-shot `DeprecationWarning`. Update when convenient.

| Legacy env | New env |
|---|---|
| `GS_CASSANDRA_ASYNC_NODES` | `GRAPHSENSE_CASSANDRA__NODES` |
| `GS_CASSANDRA_ASYNC_PORT` | `GRAPHSENSE_CASSANDRA__PORT` |
| `GS_CASSANDRA_ASYNC_USERNAME` | `GRAPHSENSE_CASSANDRA__USERNAME` |
| `GS_CASSANDRA_ASYNC_<anything>` | `GRAPHSENSE_CASSANDRA__<anything>` |
| `GRAPHSENSE_TAGSTORE_READ_URL` | `GRAPHSENSE_TAGSTORE__URL` |
| `GRAPHSENSE_TAGSTORE_READ_POOL_SIZE` | `GRAPHSENSE_TAGSTORE__POOL_SIZE` |
| `GRAPHSENSE_TAGSTORE_READ_<anything>` | `GRAPHSENSE_TAGSTORE__<anything>` |
| `GS_TAGSTORE_DB_URL` | `GRAPHSENSE_TAGSTORE__URL` *(field rename)* |
| `GSREST_HIDE_PRIVATE_TAGS` | `GRAPHSENSE_WEB__HIDE_PRIVATE_TAGS` |
| `GSREST_ALLOWED_ORIGINS` | `GRAPHSENSE_WEB__ALLOWED_ORIGINS` |
| `GSREST_<anything>` | `GRAPHSENSE_WEB__<anything>` |
| `GS_MCP_PATH` | `GRAPHSENSE_MCP__PATH` |
| `GS_MCP_ENABLED` | `GRAPHSENSE_MCP__ENABLED` |
| `GS_MCP_<anything>` | `GRAPHSENSE_MCP__<anything>` |

Special note on `GS_TAGSTORE_DB_URL`: both the legacy reader URL (`GRAPHSENSE_TAGSTORE_READ_URL`) and the legacy server-side URL (`GS_TAGSTORE_DB_URL`) collapse into `GRAPHSENSE_TAGSTORE__URL`. They were always the same Postgres database in practice; now the config reflects that.

**When both a legacy and the new env var are set, the new prefix wins.** No overrides needed for phased migration.

### Python imports / classes

| Legacy import | New path |
|---|---|
| `from graphsenselib.config.cassandra_async_config import CassandraConfig` | `get_settings().cassandra` |
| `from graphsenselib.config.tagstore_config import TagStoreReaderConfig` | `get_settings().tagstore` |
| `from graphsenselib.tagstore.config import TagstoreSettings` | `get_settings().tagstore` |
| `from graphsenselib.web.config import GSRestConfig` | `get_settings().web` (+ `.cassandra` / `.tagstore` at root) |
| `from graphsenselib.mcp.config import GSMCPConfig` | `get_settings().mcp` |
| `from graphsenselib.config import get_config` (`AppConfig`) | `get_settings()` (`Settings`) |

### YAML structure

The new Settings accepts **both the legacy and the new layouts**. Legacy monolithic YAMLs (with a top-level `environments:` dict) continue to load — the selected environment's block is automatically lifted to root at load time, with a deprecation warning nudging toward the per-env file split. The new preferred shape splits configuration into a shared base file plus one overlay file per environment.

**Legacy layout (keeps working, emits deprecation warning):**

```yaml
default_environment: prod
environments:
  prod:
    cassandra_nodes: [cassandra-prod:9042]
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
        ingest_config: { node_reference: http://btc-node:8332 }
web:                          # loose Optional[Dict] in legacy AppConfig
  database:
    nodes: [cassandra-rest:9042]
  gs-tagstore:
    url: postgresql://tagstore
  ALLOWED_ORIGINS: "*"
slack_topics:
  exceptions:
    hooks: [https://hooks.slack.com/…]
```

**New layout — shared base + per-env overlay:**

`graphsense.yaml` holds everything that's the same across environments — credentials-in-env aside, things like Slack hooks, cache paths, API keys, REST feature flags. A per-env overlay file (`graphsense.prod.yaml`, `graphsense.dev.yaml`, etc.) holds the environment-specific parts: Cassandra nodes, keyspace topology, any env-specific REST/MCP overrides.

```yaml
# graphsense.yaml  — shared defaults
tagstore:
  url: postgresql://tagstore/tagstore
  pool_size: 50
web:
  ALLOWED_ORIGINS: "*"
  hide_private_tags: false
mcp:
  enabled: true
  path: /mcp
slack_topics:
  exceptions:
    hooks: [https://hooks.slack.com/…]
coingecko_api_key: ${GRAPHSENSE_COINGECKO_API_KEY}   # or set via env var only
```

```yaml
# graphsense.prod.yaml  — production overlay
cassandra:
  nodes: [cassandra-prod-1:9042, cassandra-prod-2:9042]
  port: 9042
keyspaces:
  btc:
    raw_keyspace_name: btc_raw
    transformed_keyspace_name: btc_transformed
    schema_type: utxo
    ingest_config: { node_reference: http://btc-node:8332 }
  eth:
    raw_keyspace_name: eth_raw
    transformed_keyspace_name: eth_transformed
    schema_type: account
    ingest_config: { node_reference: http://eth-node:8545 }
```

```yaml
# graphsense.dev.yaml  — dev overlay
cassandra:
  nodes: [localhost:9042]
keyspaces:
  btc:
    raw_keyspace_name: btc_raw_dev
    transformed_keyspace_name: btc_transformed_dev
    schema_type: utxo
    ingest_config: { node_reference: http://btc-dev-node:8332 }
```

### Layered YAML loader

- The base file is resolved in order:
  1. `--config-file` CLI flag
  2. `$GRAPHSENSE_CONFIG_YAML` env var
  3. `$CONFIG_FILE` env var *(legacy — REST/gsrest deployment convention, still honored)*
  4. `./.graphsense.yaml`
  5. `./instance/config.yaml` *(REST Docker convention)*
  6. `~/.graphsense.yaml`
- When `--env <name>` is passed on the CLI, the loader looks for `<base-stem>.<env>.<ext>` next to the base file (e.g. `graphsense.yaml` + `--env prod` → `graphsense.prod.yaml`).
- The overlay is **deep-merged** onto the base: dicts merge recursively, overlay wins on scalar and list conflicts.
- Without `--env`, only the base file is loaded. With `--env X` and no matching overlay file, the base is used alone.

```
$ graphsense-cli --env prod <command>
   → loads graphsense.yaml           (base: shared)
   → loads graphsense.prod.yaml      (overlay: prod-specific)
   → deep-merge, prod wins
```

### REST and ingest share a cluster

In the new model, `Settings.cassandra` is the single Cassandra cluster for the loaded environment — REST, MCP, and ingest all point at it. Before, REST could carry a separate `GSRestConfig.database` block for its own cluster. If a deployment genuinely has two clusters (e.g. a read replica for REST, writes via ingest to another), run the two processes with different base files — e.g. `graphsense-rest.yaml` on the REST host, `graphsense.yaml` on the ingest host, each pointed at via `GRAPHSENSE_CONFIG_YAML`.

### `./instance/config.yaml` — REST deployment convention

REST deployments that ship with `./instance/config.yaml` (the existing Docker convention) continue to work unchanged: that path is in the default lookup tuple, so neither `GRAPHSENSE_CONFIG_YAML` nor `CONFIG_FILE` needs to be set. The file can contain just the REST-relevant sections (`cassandra:`, `tagstore:`, `web:`).

Deployments that set the legacy `CONFIG_FILE` env var (the old gsrest convention for pointing at a non-default path) also keep working without changes — it's picked up by the new loader at lookup-step 3, just below `GRAPHSENSE_CONFIG_YAML` in precedence. When both are set, the new `GRAPHSENSE_CONFIG_YAML` wins.

## Source precedence

A single, explicit chain for every field. Highest priority first:

1. `init` — kwargs passed to `Settings(...)`
2. `env` — environment variables (new prefix, or alias-promoted legacy)
3. `dotenv` — `.env` file
4. `yaml:<path>` — the YAML file (base and overlay deep-merged, with overlay winning)
5. `secrets` — pydantic `file_secret_settings` (mount-based secrets)
6. `default` — pydantic field default

`gs config show --resolved --source` prints this source for every field.

## Debugging: where did this value come from?

```bash
# List every resolved field and its value
graphsense-cli config show --resolved

# Same, with the source layer that produced each value
graphsense-cli config show --resolved --source
```

Output (abridged):

```
path                    value                        source
cassandra.nodes         ['prod.example:9042']        yaml:/home/me/.graphsense.yaml
cassandra.port          9042                         default
mcp.path                '/custom-mcp'                env
tagstore.url            'postgresql://tagstore'      yaml:/home/me/.graphsense.yaml
web.ALLOWED_ORIGINS     '*'                          default
web.hide_private_tags   True                         env
…
```

This is the fastest way to answer "is this coming from my YAML, my env, or a default?"

## Verifying a migration didn't change the effective config

When migrating a live system — renaming env vars, splitting a monolithic YAML into per-env files, or both — you want a way to prove the new layout resolves to exactly the same effective values as the old one. Use `gs config dump`:

```bash
# Before changing anything — capture the baseline.
graphsense-cli config --env prod dump > /tmp/before.json
graphsense-cli config --env prod dump --hash  # sha256:4ad96…

# Migrate (env vars, YAML split, or both).
# Then, under the new config:
graphsense-cli config --env prod dump > /tmp/after.json
graphsense-cli config --env prod dump --hash

# Same hash → byte-identical effective Settings.
diff /tmp/before.json /tmp/after.json  # expect: empty
```

The dump is a deterministic, sorted JSON rendering of the fully resolved `Settings` model — all sources merged, defaults filled in, `legacy_web_dict` excluded (it's a migration-only mirror that would otherwise differ between shapes). YAML output is available via `--format yaml`.

This works across all three migration axes:

- **Legacy env vars vs new env vars** — the loader translates legacy prefixes under the hood, so both paths resolve identically.
- **Monolithic YAML vs per-env split** — the loader applies the same hoists (`environments.<env>` to root, `web.database` → root `cassandra`, `web.gs-tagstore` → root `tagstore`) that `gs config migrate` would, so the effective Settings is the same either way.
- **Mixed** — a YAML half-migrated by hand still resolves the same way as long as the values are equivalent.

When the hashes differ, the JSON diff tells you exactly which field changed. Common causes: a legacy env var was set in the old run but not the new (check with `config show --resolved --source`), or a `web.database`/`cassandra_nodes` cluster mismatch in the legacy YAML was resolved differently than expected (the loader — like the migrator — gives per-env `cassandra_nodes` precedence; see "REST and ingest share a cluster" above).

## Backward compatibility during the deprecation window

Everything below keeps working without changes:

- All legacy import paths (`from graphsenselib.config.cassandra_async_config import CassandraConfig`, etc.).
- All legacy env variables (`GS_MCP_PATH`, `GSREST_*`, `gs_tagstore_db_url`, etc.).
- All legacy YAML shapes (including the free-form `web:` block).
- `get_config()` — still returns an `AppConfig` instance.
- `AppConfig` accessor methods (`get_environment`, `get_keyspace_config`, `get_s3_credentials`, `load_partial`, `text`, `path`, `generate_yaml`, …).
- The autouse `patch_config` pytest fixture.

What changes:

- Constructing any of the five legacy config classes emits a one-shot `DeprecationWarning` naming the new `Settings.<sub>` replacement.
- Setting a legacy-prefixed env var emits a one-shot `DeprecationWarning` naming the new env var.
- Loading a YAML with a legacy `environments.<env>` block emits a one-shot `DeprecationWarning` on each file nudging toward per-env overlay files. The legacy block is automatically lifted to root (`cassandra.*` + root-level `keyspaces`) so the file still works.
- `AppConfig` now accepts `extra="allow"` (needed so YAMLs with the new top-level keys validate against both layouts).

Warnings are surfaced on the CLI via `warnings.simplefilter("default", DeprecationWarning, append=True)` inside `try_load_config`. Running tests? See *Warnings as errors* below.

## Warnings as errors in tests

`pyproject.toml` has:

```toml
[tool.pytest.ini_options]
filterwarnings = [
    "error",
    # TODO(deprecation): remove with _legacy.py
    "default:Env var .* is deprecated; use .* instead\\.:DeprecationWarning",
    "default:(CassandraConfig|TagStoreReaderConfig|TagstoreSettings|GSRestConfig|GSMCPConfig) is deprecated:DeprecationWarning",
    "default:.*nested .environments\\..*. block is deprecated.*:DeprecationWarning",
]
```

Three whitelist entries, one per intentional deprecation category:

1. **Legacy env prefixes** (`GS_MCP_FOO` → `GRAPHSENSE_MCP__FOO`, etc.)
2. **Legacy config classes** (`CassandraConfig`, `TagStoreReaderConfig`, `TagstoreSettings`, `GSRestConfig`, `GSMCPConfig`)
3. **Legacy monolithic YAML `environments.<env>` blocks** being lifted to root

Net effect: any **unexpected** warning fails the build, but the three intentional deprecation nudges are let through. When `_legacy.py` is removed, delete the three `default:` entries and any remaining legacy warnings become build errors (catching anyone still on the old path).

Do **not** pass `-W error` on the pytest CLI — it overrides the ini-file filter chain and breaks the whitelist. The `Makefile` `test` / `test-ci` targets no longer pass it for this reason.

## Migration steps for library consumers (this repo)

Each migration is independent; migrate one subsystem at a time.

1. **Grep for legacy imports:**
   ```bash
   git grep -n 'from graphsenselib\.config\.\(cassandra_async_config\|tagstore_config\)'
   git grep -n 'from graphsenselib\.web\.config import GSRestConfig'
   git grep -n 'from graphsenselib\.mcp\.config import GSMCPConfig'
   git grep -n 'TagstoreSettings'
   ```
2. **Replace each import and usage:**
   - `GSRestConfig()` → `get_settings().web` (plus `get_settings().cassandra` / `get_settings().tagstore` where those were previously `.database` / `.tagstore`).
   - `GSMCPConfig()` → `get_settings().mcp`.
   - `CassandraConfig(...)` direct instantiation (typically in tests) → construct `CassandraSettings` or use `get_settings().cassandra`.
   - `TagStoreReaderConfig()` / `TagstoreSettings()` → `get_settings().tagstore`.
3. **Drop manual env-var reading** for any field that's now in `Settings`. The new env vars are auto-resolved; you don't need `os.environ.get("GSREST_…")` anymore.
4. **Run tests.** With the warnings-as-errors config, anything still on the legacy path will either warn (whitelisted) or surface as a real error.

The web boot surface (`web/app.py:resolve_rest_config`, `web/app.py:_maybe_attach_mcp`) and the CLI (`cli/common.try_load_config`) are the obvious first migration candidates because they're at the boot-time entrypoint.

## Migration steps for external users (YAML + env)

1. Start your service/process with `GRAPHSENSE_CONFIG_YAML=/path/to/your.graphsense.yaml` (or leave to default lookup).
2. Run `graphsense-cli --env <yourenv> config show --resolved --source`. Review.
3. **Split your monolithic YAML into per-env files** — use the built-in helper:
   ```bash
   graphsense-cli config migrate --in /path/to/legacy.yaml --out-dir ./config
   ```
   This writes `./config/graphsense.yaml` (shared) plus one `./config/graphsense.<env>.yaml` per entry in your legacy `environments:` block. Warnings on stderr flag anything that needs manual review (e.g. a REST vs. ingest cluster mismatch). No data is dropped; your legacy file is read-only.

   Step-by-step equivalent, if you prefer to edit by hand:
   - Move shared fields (`tagstore:`, `web:`, `mcp:`, `slack_topics:`, `cache_directory:`, API keys) into `graphsense.yaml`.
   - For each entry in your old `environments:` dict, create a `graphsense.<env>.yaml` holding that entry's `cassandra_nodes` → `cassandra.nodes`, credentials → `cassandra.<username/password/...>`, and `keyspaces:` at root.
   - Remove the `environments:` block from `graphsense.yaml`.
4. Optionally update env vars to the new prefix. The deprecation warning message includes the exact new name.
5. When you see no more `DeprecationWarning`s emitted by graphsenselib, you are migration-complete.

The automatic legacy-lift means step 3 can be deferred — the old monolithic file keeps working with a warning. But splitting is what makes per-env review and per-env credential management clean.

### `gs config migrate` reference

```
graphsense-cli config migrate [--in FILE] [--out-dir DIR] [--overwrite]

  --in FILE        Legacy YAML to migrate. Defaults to the currently
                   resolved config file (from GRAPHSENSE_CONFIG_YAML /
                   default lookup).
  --out-dir DIR    Write graphsense.yaml + graphsense.<env>.yaml into
                   this directory. Without this flag, the translated
                   files are printed to stdout with `# ===== <name> =====`
                   delimiters — handy for quick review before committing.
  --overwrite      Allow overwriting existing files under --out-dir
                   (default: refuse with a non-zero exit).
```

Legacy fields the migrator translates:

| Legacy location | New location |
|---|---|
| `environments.<env>.cassandra_nodes` | `graphsense.<env>.yaml` → `cassandra.nodes` |
| `environments.<env>.username` / `password` / `readonly_*` | `graphsense.<env>.yaml` → `cassandra.<field>` |
| `environments.<env>.keyspaces` | `graphsense.<env>.yaml` → `keyspaces` (root) |
| `web.database` | `graphsense.yaml` → `cassandra` (root, shared) |
| `web.gs-tagstore` | `graphsense.yaml` → `tagstore` (root) |
| `web.<everything else>` | `graphsense.yaml` → `web.<field>` |
| `slack_topics`, `cache_directory`, `coingecko_api_key`, `s3_credentials`, etc. | `graphsense.yaml` → same name, root |

**Dedup:** the migrator only creates a `graphsense.<env>.yaml` when that env's contents actually differ from the shared base.

- Fields identical across *all* environments are lifted into `graphsense.yaml`. Per-env files hold only the differing fields.
- Dedup happens at sub-key granularity for `cassandra:` and `keyspaces:` — e.g. if all envs share `cassandra.username` but have different `cassandra.nodes`, the username lifts to shared and each per-env file keeps only `cassandra.nodes`.
- If after dedup a per-env file would only contain its `environment: <name>` marker, it's skipped entirely (the `--env` flag still propagates the name at load time).
- Consequence: a legacy YAML with a single `environments:` entry, or multiple identical entries, produces only `graphsense.yaml` — no overlay files.

## Rollout plan

The consolidation ships in two releases — one minor for compat, one major for the cleanup.

### This release (minor) — additive, non-breaking

- Everything new ships: `Settings`, layered loader, `gs config show --resolved`, `gs config migrate`, `gs config dump`.
- Nothing legacy is removed. All existing code paths continue to work.
- Legacy surfaces emit one-shot `DeprecationWarning`s:
  - old env prefixes (`GS_MCP_*`, `GSREST_*`, `GS_CASSANDRA_ASYNC_*`, `GRAPHSENSE_TAGSTORE_READ_*`, `gs_tagstore_*`)
  - old config classes (`CassandraConfig`, `TagStoreReaderConfig`, `TagstoreSettings`, `GSRestConfig`, `GSMCPConfig`)
  - legacy monolithic `environments.<env>` YAML blocks
- Warnings-as-errors is on in `pytest` but the three intentional deprecation categories are whitelisted — consumers that haven't migrated still have a green CI.
- **Merge risk: warnings only, no breakage.**

### Between releases — migrate on your own schedule

The order within the window is flexible. Suggested:

1. **Migrate internal consumers** (library code in this repo first). Grep for `GSRestConfig`, `GSMCPConfig`, `CassandraConfig`, `TagStoreReaderConfig`, `TagstoreSettings`, `get_config()`. Replace with `get_settings()` reads. Use `gs config dump` as the regression check after each consumer switch.
2. **Migrate deployments' YAML** — `gs config migrate --in legacy.yaml --out-dir ./config`. Verify with `gs config dump --hash` before and after (should match exactly).
3. **Migrate env vars** — rename any `GSREST_*` / `GS_MCP_*` / etc. to the `GRAPHSENSE_<sub>__<field>` form. The `DeprecationWarning` on each legacy var includes the exact new name.
4. **Migrate external clients** — any downstream service importing from `graphsenselib.config.*` or `graphsenselib.web.config` / `graphsenselib.mcp.config` gets the deprecation warnings; each such call site migrates independently.

When `gs config show --resolved` emits no `DeprecationWarning`s for a given process, that process is migration-complete.

### Next major — remove the shims

See [What happens when the deprecation window closes](#what-happens-when-the-deprecation-window-closes) below for the mechanical removal PR. At that point any remaining caller of a legacy class, legacy env prefix, or legacy YAML shape fails loudly.

## What happens when the deprecation window closes

The removal PR is mechanical:

1. `rm src/graphsenselib/config/_legacy.py`
2. `grep -rn '# TODO(deprecation): remove with _legacy.py'` — each hit is a 1–3 line removal.
3. Delete the five legacy class files or their shim bodies:
   - `src/graphsenselib/config/cassandra_async_config.py`
   - `src/graphsenselib/config/tagstore_config.py`
   - `src/graphsenselib/tagstore/config/__init__.py` (or narrow it)
   - `src/graphsenselib/web/config.py` (or narrow it)
   - `src/graphsenselib/mcp/config.py` (or narrow it)
4. Delete `AppConfig` + `Environment` + `KeyspaceConfig` + helpers from `src/graphsenselib/config/config.py` (and possibly the file itself).
5. Remove `extra="allow"` from any remaining config models that had it only for migration tolerance.
6. Remove the three `default:` filter entries from `pyproject.toml` `filterwarnings`.
7. Drop `goodconf[yaml]` from `pyproject.toml` dependencies.
8. Update `tests/conftest.py:patch_config` (currently goodconf-specific) to use `set_settings(...)` against the new `Settings` model.

After the removal PR, any remaining caller of a legacy class or a legacy env prefix will fail loudly instead of getting a warning.

## See also

- `src/graphsenselib/config/settings.py` — the root `Settings` model and all submodels.
- `src/graphsenselib/config/_sources.py` — the YAML source and per-field provenance tracker.
- `src/graphsenselib/config/_legacy.py` — the quarantined deprecation machinery.
- `src/graphsenselib/config/_provenance.py` — `iter_field_sources(settings)` public helper.
- `src/graphsenselib/config/cli.py` — `gs config show --resolved [--source]`.
- `tests/config/test_settings.py` — end-to-end tests for every source + legacy translation + provenance.
