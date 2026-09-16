# graphsense-lib

## Tests: what runs where, and what it costs

Three suites and three toolchains, and the local gate deliberately runs only
part of the Python one. **A green pre-commit does not mean a green CI.**

| command | scope | time | who runs it |
| --- | --- | --- | --- |
| `make test-fast` | Python minus `slow` marks and the four testcontainer dirs — 1307 of 2327 tests | **~33s** | the pre-commit hook |
| `make test-rust` | `cargo test` in `rust/gs_clustering` | 3.3s cold, 0.06s warm | pre-commit (gated on `rust/**`) + CI |
| `make run-codegen` | regenerates the Python client (Docker + Java openapi-generator) | ~7s | pre-commit (gated on `src/graphsenselib/web/`, client templates, compat) + CI |
| `make test-ci` | the **whole** Python suite, `slow` included — 2327 tests | ~105s (+~18% with `COVERAGE=1`) | CI only |
| `make test-spark` | `sbt test` for the Scala pipeline | minutes | CI only — never run locally |
| `make test` | whole Python suite + coverage + a forced `uv sync` | ~145s | on demand |

**What the hook does NOT cover** — run these yourself before calling a change
verified:

- `tests/web`, `tests/db`, `tests/tagstore`, `tests/integration`: everything
  needing a Cassandra/Postgres/Redis testcontainer. ~1020 tests, including the
  entire REST controller surface. `make test-ci`.
- the three `@pytest.mark.slow` tests.
- `spark/` — no hook runs `sbt test`.

Why the split: the containers are ~80s of a ~95s suite, and their fixtures are
session-scoped, so the cost is all-or-nothing per run — skipping them is the
only lever that changes the order of magnitude. Coverage adds ~18% on top and
nothing reads the local report.

Traps, each of which has already bitten once:

- **Never put `-m "not slow"` back in `make test-ci`.** The local gate skips
  those tests, so CI is their only home. Until 2026-08-31 every CI invocation
  filtered them out and they ran nowhere at all.
- **There is no `pre-push` hook and none is wanted.** `make dev` runs
  `pre-commit install`, which installs `pre-commit` only. Anything too slow for
  the commit loop belongs in CI, not in a pre-push stage.
- **`spark_tests.yml` runs unconditionally, not on a path filter.** The delta
  updater and `spark/` must agree (see the next section), and a path list is
  exactly what goes stale when a new file starts feeding that contract.
- **A test that reaches a hostname which does not resolve** costs whatever the
  local resolver charges for a dead name — 120s on a dev box behind a
  blackholing DNS server, instant in CI. Use a refused port (`127.0.0.1:1`).

## Delta updater must stay in tandem with the Spark pipeline (`spark/`)

The delta updater (`src/graphsenselib/deltaupdate/`) incrementally produces the
same derived tables that the batch Spark pipeline (**`spark/`**, the Scala
transformation formerly in the standalone graphsense-spark repo) computes from
scratch. **The Spark pipeline is the ground truth.** The two must
agree column-for-column, or an incrementally-updated keyspace diverges from what a
full Spark re-run would produce.

**Any change to delta updater logic (schema, transforms, aggregation, address/
cluster/token accounting, is_contract detection, etc.) requires checking the
corresponding `spark/` code and keeping the two in sync — ideally in the same
commit, which is why they now live in one repo.** When they
disagree, match Spark's behavior — do not "fix" it only on the delta side. If the
Spark side needs to change too, flag it; a delta-only change that Spark can't
reproduce is a divergence bug waiting to surface at the next full re-run.

`spark/` keeps its own toolchain (sbt, scalafmt/scalafix, `spark/CHANGELOG.md`)
and its own release track (`spark-vX.Y.Z` tags — see VERSIONING.md); the Python
formatting hooks deliberately skip that tree. Build and test it with the
`spark-*` Makefile targets, or `cd spark && sbt test`.

### UTXO address strings: graphsense-spark decodes, it never derives

The one exception to "check Spark before changing address logic" is the UTXO
address **string** itself. graphsense-spark does **not** derive UTXO addresses
from scripts — it is a pure bytes→string codec (`AddressDecoder.scala`), the exact
inverse of gslib's string→bytes codec (`utils/address.py`,
`AddressConverterBtcLike.to_bytes`). Both just bit-pack/unpack the base58/bech32/
base62 characters; the version byte (LTC `0x30`, ZEC t1 `0x1cb8`, DOGE `0x1e`, …)
is baked into the string upstream by the gslib **parser**
(`ingest/utxo.parse_script`, `ingest/rpc_utxo._p2pk_address_from_script`). Spark
picks the alphabet by inspecting the stored bytes' leading bits — it never reads a
version byte or re-derives from a script.

Consequence: a change to how gslib encodes a UTXO address string (e.g. the
network-aware P2PK/parse_script fix that moved LTC from `1417…` to `LNE5…`) needs
**no** graphsense-spark change — Spark round-trips whatever string gslib produced,
so the gslib parser is itself the ground truth for the string. There is no
independent Spark P2PK derivation that could disagree about a version byte. (This
does not exempt derived-table *aggregation* from Spark parity — only the
address-string encoding.)

Two related facts: **DOGE is not ingested at the moment, and graphsense-spark has
no DOGE support** (`address_to_str` handles only `btc | ltc | zec | bch`; no
`dogecoin` config), so a DOGE-side address fix has no Spark counterpart to sync —
but DOGE keyspaces also aren't produced by the batch transformation. DOGE entries
in the ingest version tables are kept only so the two ingest P2PK paths stay
consistent if/when doge ingest is enabled. And the **cross-chain pubkey dataset**
(`pubkey/job.py` → `pubkey_by_address`, deriving addresses via
`utils/pubkey_to_address.convert_pubkey_to_addresses`) is a gslib-owned Spark job,
**not** part of graphsense-spark — changes there need no graphsense-spark
coordination. Note the two P2PK version-byte tables that must stay in lockstep —
`rpc_utxo._PUBKEY_ADDRESS_VERSION` and `pubkey_to_address.MAINNET_ADDRESS_SPECS` —
are guarded by `tests/ingest/test_pubkey_address_version_parity.py`.

## Database / Cassandra retry architecture

The Cassandra retry handling in `src/graphsenselib/db/cassandra.py` is split across
two layers on purpose. **Keep them separate — do not move backoff back into the
retry policy.**

- **Driver-side (`GraphsenseRetryPolicy`)** runs on the driver's I/O reactor
  thread. Its callbacks **MUST NOT block / sleep**. A previous version called
  `time.sleep()` here for backoff; during the sleep the reactor could not service
  connection heartbeats, so healthy connections were defuncted
  (heartbeat failure / `ConnectionShutdown`) and the in-flight query failed. The
  policy now only does a few *immediate* retries (`max_retries=2`, mostly
  `RETRY_NEXT_HOST`) to absorb a single-host blip, then rethrows. Writes are only
  retried when the query is marked `is_idempotent` (non-idempotent retries can
  double-apply on timeout).

- **Application-side (`CassandraDb._execute_with_backoff`)** rides out long,
  cluster-wide stalls (e.g. nodes starved of CPU by a concurrent Spark run that
  recurs on the hour). Backoff sleeps here on the **calling thread**, never the
  reactor, so heartbeats keep flowing. ~20 retries with exponential backoff +
  full jitter, delay capped at 30s ≈ multi-minute ride-out window. Only catches
  `TRANSIENT_DB_ERRORS` (`NoHostAvailable`, `OperationTimedOut`, `ReadTimeout`,
  `WriteTimeout`, `ConnectionShutdown`). All synchronous reads (`execute`,
  `execute_safe`, `execute_statement`) route through it; `execute_async` does not.
  `ConnectionShutdown` is what a defuncted connection actually raises *to the
  caller* — `Connection.error_all_requests()` re-wraps the underlying cause and
  fails every in-flight request on that connection with it. Driver-internal
  exception types (e.g. `CrcException`) are raised on the reactor thread and never
  reach us, so listing them here is dead code.

- **Protocol version is pinned to v4** (`protocol_version=4` in `connect()`). Left
  to negotiate, the driver picks v5, whose checksummed segment framing desyncs
  under load and reads a segment header at the wrong offset (CASSANDRA-19971 /
  PYTHON-1337, still open) — surfacing as `ConnectionShutdown: CRC mismatch on
  header …` and killing long delta-update runs. Checksummed framing is v5-only, so
  v4 removes the failure mode. **Do not unpin it, and do not "fix" it by disabling
  compression instead**: the header CRC24 is verified before any decompression
  (`segment.py:decode_header`), so a *header* mismatch is a buffer desync, not bad
  lz4 output — and the framing code is shared by the compressed and uncompressed
  codecs, so `compression=None` keeps the bug and just forfeits the bandwidth
  saving. v4 still compresses, at the frame level (`protocol.py`, the
  `not has_checksumming_support(...)` branch). The only v5 feature we relied on,
  statement-level keyspace, is handled version-independently in `execute()`. The
  **async** driver (`db/asynchronous/cassandra.py`) is pinned to v4 too; it never
  needed the v5 keyspace flag, since `replaceFrom` qualifies the keyspace in CQL.

- **Connection settings** that support this: `idle_heartbeat_timeout=60` and
  `idle_heartbeat_interval=30` tolerate longer server stalls before defuncting,
  and `ExponentialReconnectionPolicy(1.0, 60.0)` re-adds a downed host within
  ~a minute. This is *host* reconnection, distinct from *query* retry.
