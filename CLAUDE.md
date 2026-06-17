# graphsense-lib

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
  `WriteTimeout`). All synchronous reads (`execute`, `execute_safe`,
  `execute_statement`) route through it; `execute_async` does not.

- **Connection settings** that support this: `idle_heartbeat_timeout=60` and
  `idle_heartbeat_interval=30` tolerate longer server stalls before defuncting,
  and `ExponentialReconnectionPolicy(1.0, 60.0)` re-adds a downed host within
  ~a minute. This is *host* reconnection, distinct from *query* retry.
