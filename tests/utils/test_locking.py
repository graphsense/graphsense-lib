import os
import threading
from unittest.mock import patch, MagicMock

import pytest

from graphsenselib.utils.locking import (
    LockAcquisitionError,
    LockConfigurationError,
    create_lock,
)


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
        # After release, file still exists (filelock behavior) but is not held
        assert os.path.exists(lockfile)
        os.unlink(lockfile)

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

        cfg = _mock_config(use_redis_locks=True, redis_url="redis://invalid-host:6379")
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
        os.unlink(lockfile)


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
