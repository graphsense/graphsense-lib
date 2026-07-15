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
