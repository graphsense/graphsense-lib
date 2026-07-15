# graphsense-lib

## Delta updater must stay in tandem with graphsense-spark

The delta updater (`src/graphsenselib/deltaupdate/`) incrementally produces the
same derived tables that the batch Spark pipeline (**graphsense-spark**, separate
repo) computes from scratch. **graphsense-spark is the ground truth.** The two must
agree column-for-column, or an incrementally-updated keyspace diverges from what a
full Spark re-run would produce.

**Any change to delta updater logic (schema, transforms, aggregation, address/
cluster/token accounting, is_contract detection, etc.) requires checking the
corresponding graphsense-spark code and keeping the two in sync.** When they
disagree, match Spark's behavior — do not "fix" it only on the delta side. If the
Spark side needs to change too, flag it; a delta-only change that Spark can't
reproduce is a divergence bug waiting to surface at the next full re-run.

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
