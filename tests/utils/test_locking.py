import contextlib
import json
import os
import socket
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from graphsenselib.utils.locking import (
    LockAcquisitionError,
    LockConfigurationError,
    create_lock,
)
from graphsenselib.utils.locking import delta_ingest_lock_name


def _mock_config(use_redis_locks=False, redis_url=None):
    """Return a mock config with the given lock settings."""
    cfg = MagicMock()
    cfg.use_redis_locks = use_redis_locks
    cfg.redis_url = redis_url
    return cfg


class TestCreateLockDisabled:
    def test_disabled_returns_nullcontext(self):
        with create_lock("test", disabled=True):
            pass  # should not raise

    def test_disabled_ignores_redis_config(self):
        # disabled=True should skip config reading entirely
        with create_lock("test", disabled=True):
            pass


class TestFileLockBackend:
    def test_acquires_and_releases_file_lock(self):
        lock_name = "test_filelock"
        lockfile = f"/tmp/{lock_name}.lock"
        with create_lock(lock_name):
            assert os.path.exists(lockfile)
        # # After release, file still exists (filelock behavior) but is not held
        # assert os.path.exists(lockfile)
        # os.unlink(lockfile)

    def test_contention_raises_lock_acquisition_error(self):
        lock_name = "test_contention"
        lockfile = f"/tmp/{lock_name}.lock"
        acquired = threading.Event()
        release = threading.Event()

        def hold_lock():
            with create_lock(lock_name, blocking_timeout=5):
                acquired.set()
                release.wait(timeout=10)

        t = threading.Thread(target=hold_lock)
        t.start()
        acquired.wait(timeout=5)

        with pytest.raises(LockAcquisitionError):
            with create_lock(lock_name, blocking_timeout=0.1):
                pass

        release.set()
        t.join(timeout=5)
        if os.path.exists(lockfile):
            os.unlink(lockfile)

    def test_lock_name_determines_file_path(self):
        lock_name = "unique_name_12345"
        lockfile = f"/tmp/{lock_name}.lock"
        with create_lock(lock_name):
            assert os.path.exists(lockfile)
        if os.path.exists(lockfile):
            os.unlink(lockfile)


class TestRedisLockBackend:
    def test_missing_redis_package_raises_configuration_error(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "redis":
                raise ImportError("No module named 'redis'")
            return real_import(name, *args, **kwargs)

        cfg = _mock_config(use_redis_locks=True, redis_url="redis://localhost")
        monkeypatch.setattr(builtins, "__import__", mock_import)

        with patch("graphsenselib.config.get_config", return_value=cfg):
            with pytest.raises(
                LockConfigurationError, match="Redis package is required"
            ):
                with create_lock("test"):
                    pass

    def test_missing_redis_url_raises_configuration_error(self):
        try:
            import redis  # noqa: F401
        except ImportError:
            pytest.skip("redis not installed")

        cfg = _mock_config(use_redis_locks=True, redis_url=None)
        with patch("graphsenselib.config.get_config", return_value=cfg):
            with pytest.raises(
                LockConfigurationError, match="redis_url not configured"
            ):
                with create_lock("test"):
                    pass

    def test_redis_connection_error_raises_acquisition_error(self):
        try:
            import redis  # noqa: F401
        except ImportError:
            pytest.skip("redis not installed")

        # Port 1 on loopback: refuses instantly. A bogus *hostname* here instead
        # makes the test cost whatever the local resolver charges for a dead
        # name -- on a dev box behind a blackholing DNS server that was 120s,
        # more than half the whole suite.
        cfg = _mock_config(use_redis_locks=True, redis_url="redis://127.0.0.1:1")
        with patch("graphsenselib.config.get_config", return_value=cfg):
            with pytest.raises(LockAcquisitionError, match="connection error"):
                with create_lock("test"):
                    pass

    def test_config_use_redis_false_uses_file_lock(self):
        """When use_redis_locks=False, file lock is used regardless of redis_url."""
        cfg = _mock_config(use_redis_locks=False, redis_url="redis://localhost")
        lock_name = "test_config_false"
        lockfile = f"/tmp/{lock_name}.lock"
        with patch("graphsenselib.config.get_config", return_value=cfg):
            with create_lock(lock_name):
                assert os.path.exists(lockfile)
        if os.path.exists(lockfile):
            os.unlink(lockfile)


KEY = "graphsense:lock:test_stale"
HEARTBEAT_KEY = f"{KEY}:heartbeat"
ALERT_KEY = f"{KEY}:stale-alert-sent"


class TestRedisLockStaleHolderReport:
    """A holder killed hard (OOM, SIGKILL) never releases. Nothing ever takes
    the lock away from it -- not a TTL, which can expire under a holder that is
    still writing, and not a liveness probe, which cannot see other hosts. A
    contender reports the silent holder and a human breaks the lock."""

    def _client(self, values=None, acquire=True, alert_set=True):
        values = {k: str(v).encode() for k, v in (values or {}).items()}
        client = MagicMock()
        fake_lock = MagicMock()
        fake_lock.acquire.return_value = acquire
        client.lock.return_value = fake_lock
        client.get.side_effect = lambda k: values.get(k)
        # SET NX on the alert key: True on the first report in the window.
        client.set.return_value = alert_set
        fake_redis = MagicMock()
        fake_redis.from_url.return_value = client
        fake_redis.exceptions.RedisError = RuntimeError
        return client, fake_lock, fake_redis

    @contextlib.contextmanager
    def _patched(self, fake_redis, notifier):
        cfg = _mock_config(use_redis_locks=True, redis_url="redis://localhost")
        with (
            patch.dict("sys.modules", {"redis": fake_redis}),
            patch("graphsenselib.config.get_config", return_value=cfg),
            patch("graphsenselib.monitoring.notifications.send_msg_to_topic", notifier),
        ):
            yield

    def _hold(self):
        """Acquire and release the lock successfully."""
        client, fake_lock, fake_redis = self._client()
        notifier = MagicMock()
        with self._patched(fake_redis, notifier):
            with create_lock("test_stale"):
                pass
        return client, fake_lock, notifier

    def _contend(self, values, alert_set=True, notifier=None):
        """Fail to acquire against a lock held by someone else."""
        client, _, fake_redis = self._client(
            values=values, acquire=False, alert_set=alert_set
        )
        notifier = notifier or MagicMock()
        with self._patched(fake_redis, notifier):
            with pytest.raises(LockAcquisitionError):
                with create_lock("test_stale"):
                    pass
        return client, notifier

    def _owner(self, acquired_at):
        return json.dumps(
            {"host": socket.gethostname(), "pid": 999999, "acquired_at": acquired_at},
            sort_keys=True,
        )

    def test_lock_has_no_ttl(self):
        client, _, _ = self._hold()
        # A TTL would let the lock expire under a live holder -> two writers.
        assert client.lock.call_args.kwargs.get("timeout") is None

    def test_owner_token_names_the_holder(self):
        _, fake_lock, _ = self._hold()
        token = json.loads(fake_lock.acquire.call_args.kwargs["token"])
        assert token["host"] == socket.gethostname()
        assert token["pid"] == os.getpid()
        assert token["acquired_at"] <= time.time()
        # release() is a CAS on this value, so it must be unique per acquisition
        # even where host+pid are not (container id + pid 1).
        _, other, _ = self._hold()
        other_token = json.loads(other.acquire.call_args.kwargs["token"])
        assert token["nonce"] != other_token["nonce"]

    def test_heartbeat_stamps_the_sibling_key_only(self):
        from graphsenselib.utils import locking

        client = MagicMock()
        stop = threading.Event()
        stop.set()  # one stamp, then return
        locking._heartbeat(client, KEY, stop)
        client.set.assert_called_once()
        assert client.set.call_args.args[0] == HEARTBEAT_KEY
        # The lock value itself must never change: release() CASes against it.
        assert all(c.args[0] != KEY for c in client.set.call_args_list)

    def test_heartbeat_failure_never_raises(self):
        from graphsenselib.utils import locking

        client = MagicMock()
        client.set.side_effect = RuntimeError("redis down")
        stop = threading.Event()
        stop.set()
        locking._heartbeat(client, KEY, stop)  # must not raise

    def test_release_clears_heartbeat_and_alert_keys(self):
        client, _, _ = self._hold()
        client.delete.assert_called_once_with(HEARTBEAT_KEY, ALERT_KEY)

    def test_silent_holder_is_reported(self):
        old = time.time() - 9 * 3600
        client, notifier = self._contend(
            {KEY: self._owner(old), HEARTBEAT_KEY: int(old)}
        )
        notifier.assert_called_once()
        topic, msg = notifier.call_args.args
        assert topic == "exceptions"
        assert KEY in msg and f"redis-cli DEL {KEY}" in msg

    def test_live_holder_is_not_reported(self):
        now = time.time()
        client, notifier = self._contend(
            {KEY: self._owner(now - 6 * 3600), HEARTBEAT_KEY: int(now)}
        )
        # Held for six hours but still stamping: a long job, not a dead one.
        notifier.assert_not_called()

    def test_report_is_rate_limited(self):
        old = time.time() - 9 * 3600
        client, notifier = self._contend(
            {KEY: self._owner(old), HEARTBEAT_KEY: int(old)}, alert_set=None
        )
        # SET NX returned None: someone already reported this within the window.
        notifier.assert_not_called()
        assert client.set.call_args.kwargs.get("nx") is True

    def test_acquisition_time_is_used_when_there_is_no_heartbeat(self):
        client, notifier = self._contend({KEY: self._owner(time.time() - 9 * 3600)})
        notifier.assert_called_once()

    def test_legacy_owner_value_is_never_reported(self):
        # A lock taken before this version: an opaque uuid, no timestamps.
        client, notifier = self._contend({KEY: "b6cf1a2e3d4f5a6b7c8d9e0f1a2b3c4d"})
        notifier.assert_not_called()

    def test_reporting_failure_does_not_mask_contention(self):
        old = time.time() - 9 * 3600
        notifier = MagicMock(side_effect=RuntimeError("slack down"))
        # _contend already asserts LockAcquisitionError is what surfaces.
        client, _ = self._contend(
            {KEY: self._owner(old), HEARTBEAT_KEY: int(old)}, notifier=notifier
        )

    def test_a_contender_never_touches_the_lock(self):
        old = time.time() - 9 * 3600
        client, _ = self._contend({KEY: self._owner(old), HEARTBEAT_KEY: int(old)})
        # Only the alert key is written, and nothing is deleted or scripted.
        assert client.set.call_args.args[0] == ALERT_KEY
        client.delete.assert_not_called()
        client.eval.assert_not_called()

    def test_holder_released_while_we_looked_is_not_reported(self):
        old = time.time() - 9 * 3600
        owner = self._owner(old)
        client, _, fake_redis = self._client(acquire=False)
        # Second read of the lock key comes back empty: released meanwhile.
        reads = {KEY: [owner.encode(), None], HEARTBEAT_KEY: [str(int(old)).encode()]}
        client.get.side_effect = lambda k: reads[k].pop(0) if reads.get(k) else None
        notifier = MagicMock()
        with self._patched(fake_redis, notifier):
            with pytest.raises(LockAcquisitionError):
                with create_lock("test_stale"):
                    pass
        notifier.assert_not_called()

    def test_format_duration(self):
        from graphsenselib.utils import locking

        assert locking._format_duration(90) == "1m"
        assert locking._format_duration(9 * 3600 + 12 * 60) == "9h 12m"


class TestLockAcquisitionError:
    def test_is_exception(self):
        assert issubclass(LockAcquisitionError, Exception)

    def test_carries_message(self):
        err = LockAcquisitionError("test message")
        assert str(err) == "test message"


class TestLockConfigurationError:
    def test_is_exception(self):
        assert issubclass(LockConfigurationError, Exception)

    def test_not_a_lock_acquisition_error(self):
        assert not issubclass(LockConfigurationError, LockAcquisitionError)


class TestDeltaIngestLockName:
    def test_s3_uri(self):
        assert (
            delta_ingest_lock_name("s3://my-bucket/foo/bar", "btc")
            == "delta_ingest_my-bucket_foo_bar_btc"
        )

    def test_s3_uri_trailing_slash(self):
        assert (
            delta_ingest_lock_name("s3://my-bucket/foo/bar/", "btc")
            == "delta_ingest_my-bucket_foo_bar_btc"
        )

    def test_local_absolute_path(self):
        assert (
            delta_ingest_lock_name("/data/delta", "btc")
            == "delta_ingest_local_data_delta_btc"
        )

    def test_local_relative_path(self):
        assert (
            delta_ingest_lock_name("data/delta", "btc")
            == "delta_ingest_local_data_delta_btc"
        )

    def test_empty_network_omitted(self):
        assert (
            delta_ingest_lock_name("s3://my-bucket/foo", "")
            == "delta_ingest_my-bucket_foo"
        )

    def test_s3_no_prefix(self):
        assert (
            delta_ingest_lock_name("s3://my-bucket", "btc")
            == "delta_ingest_my-bucket_btc"
        )

    def test_idempotent_for_same_inputs(self):
        a = delta_ingest_lock_name("s3://b/p/q/", "eth")
        b = delta_ingest_lock_name("s3://b/p/q", "eth")
        assert a == b
