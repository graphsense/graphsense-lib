import json
import subprocess
import sys

from graphsenselib.utils.grpc import MAX_GRPC_RETRY_ATTEMPTS, get_retry_service_config


def test_retry_policy_stays_within_grpc_cap():
    [method] = json.loads(get_retry_service_config())["methodConfig"]
    assert method["retryPolicy"]["maxAttempts"] <= MAX_GRPC_RETRY_ATTEMPTS


def test_channel_does_not_log_clamped_max_attempts():
    # gRPC core logs the clamp to stderr from C, so run it in a subprocess.
    # 127.0.0.1:1 refuses at once; resolving it is enough to parse the config.
    script = (
        "import grpc\n"
        "from graphsenselib.utils.grpc import get_channel\n"
        "ch = get_channel('grpc://127.0.0.1:1')\n"
        "try:\n"
        "    grpc.channel_ready_future(ch).result(timeout=1)\n"
        "except grpc.FutureTimeoutError:\n"
        "    pass\n"
        "ch.close()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert "clamped retryPolicy" not in result.stderr
