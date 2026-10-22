import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import click

from ..cli.common import require_currency, require_environment
from ..config import get_config
from ..utils.console import console

logger = logging.getLogger(__name__)


@click.group()
def sync_cli():
    pass


@sync_cli.group("sync")
def sync():
    """Keep networks continuously up to date (replaces per-network cron jobs)."""
    pass


# Fixed rather than tempfile.gettempdir(), like utils/locking.LOCK_DIR: every
# daemon and every `sync pause/status` must resolve the same directory.
FILE_STATE_DIR = "/tmp/graphsense-sync"


def _store(env: str):
    from .state import FileStateStore, RedisStateStore

    config = get_config()
    if config.use_redis_locks and config.redis_url:
        return RedisStateStore(config.redis_url, env)
    directory = os.path.join(FILE_STATE_DIR, env)
    logger.info(
        f"No Redis configured (use_redis_locks/redis_url): sync state in "
        f"{directory}. Daemons in separate containers need it on a shared volume."
    )
    return FileStateStore(directory)


class _EnvironmentReloader:
    """Re-reads the config file when it changes, so enabling a network or
    moving it to another channel needs no daemon restart."""

    def __init__(self, env: str, path: Optional[str]):
        self.env = env
        self.path = path
        self._mtime: Optional[float] = None
        self._environment = None

    def __call__(self):
        from ..config import AppConfig

        if self.path is None:
            return get_config().get_environment(self.env)
        mtime = os.stat(self.path).st_mtime
        if self._environment is None or mtime != self._mtime:
            cfg = AppConfig(config_file=self.path)
            cfg.load(filename=self.path)
            self._environment = cfg.get_environment(self.env)
            if self._mtime is not None:
                logger.info(f"Config {self.path} changed, reloaded")
            self._mtime = mtime
        return self._environment


@sync.command("run")
@require_environment()
@click.option(
    "--channel",
    default="stable",
    show_default=True,
    help="Run only the networks whose sync.networks.<c>.channel matches.",
)
@click.option(
    "--once",
    is_flag=True,
    help="One cycle per network, then exit (non-zero if any failed).",
)
@click.option(
    "--currency",
    "-c",
    "only",
    multiple=True,
    help="Restrict to these networks (repeatable). They must still be "
    "enabled and on this channel.",
)
@click.pass_context
def run(ctx, env: str, channel: str, once: bool, only):
    """Run the sync daemon.

    Per network it ingests new blocks from the node, then runs the delta
    update, as child processes of the same graphsense-lib version. Configure
    it under environments.<env>.sync; run one daemon per network (-c) or
    per channel (--channel).
    """
    from .jobs import JobRunner
    from .scheduler import SyncScheduler

    root = ctx.find_root().params
    # The loaded config does not remember a --config-file path (path() only
    # knows GRAPHSENSE_CONFIG_YAML and the default locations), so take it from
    # the option. Jobs must read the very same file.
    config_path = root.get("config_file") or get_config().path()
    config_path = os.path.abspath(config_path) if config_path else None
    global_args = ["-" + "v" * root["verbosity"]] if root.get("verbosity") else []
    if config_path:
        global_args += ["--config-file", config_path]

    scheduler = SyncScheduler(
        env=env,
        channel=channel,
        store=_store(env),
        load_environment=_EnvironmentReloader(env, config_path),
        runner=JobRunner(global_args=global_args),
        only=list(only) or None,
    )
    sys.exit(scheduler.run(once=once))


@sync.command("status")
@require_environment()
def status(env: str):
    """Show every configured network: owner, pause, heights, last result and
    whether a delta-update WAL record is pending."""
    _print_status(env, _store(env))


def _print_status(env: str, store, only: Optional[str] = None):
    from rich.table import Table

    environment = get_config().get_environment(env)
    sync_cfg = environment.sync
    networks = sorted(sync_cfg.networks) if sync_cfg else []
    if only is not None:
        networks = [n for n in networks if n == only]

    daemons = Table(title=f"sync daemons ({env})")
    for col in ("id", "channel", "host", "version", "up since", "running"):
        daemons.add_column(col)
    for key, info in sorted(store.list_daemons()):
        daemons.add_row(
            key,
            info.get("channel", ""),
            info.get("host", ""),
            info.get("version", ""),
            _fmt_ts(info.get("started_at")),
            ", ".join(info.get("running", [])),
        )
    console.print(daemons)

    table = Table(title=f"sync networks ({env})")
    for col in (
        "network",
        "channel",
        "state",
        "node tip",
        "raw",
        "behind",
        "transformed",
        "WAL",
        "last result",
        "last success",
        "next cycle",
    ):
        table.add_column(col)
    for network in networks:
        ccfg = sync_cfg.networks[network]
        st = store.read_status(network) or {}
        pause = store.get_pause(network)
        if not ccfg.enabled:
            state = "disabled"
        elif pause is not None:
            until = pause.get("until")
            state = f"paused ({pause.get('reason')}" + (
                f", until {_fmt_ts(until)})" if until else ")"
            )
        elif st.get("running"):
            state = f"running {st['running']}"
        elif store.claim_owner(network) is None:
            state = "no daemon"
        elif st.get("failures"):
            state = f"retrying ({st['failures']} failed)"
        else:
            state = "idle"
        raw, target = st.get("raw"), st.get("target")
        behind = str(target - raw) if raw is not None and target is not None else ""
        table.add_row(
            network,
            ccfg.channel,
            state,
            _fmt_int(st.get("node_tip")),
            _fmt_int(raw),
            behind,
            _fmt_int(st.get("transformed")),
            _wal_state(env, network, environment.keyspaces.get(network)),
            _shorten(str(st.get("last_result", ""))),
            _fmt_ts(st.get("last_success_at")),
            _fmt_ts(st.get("next_cycle_at")),
        )
    console.print(table)


def _wal_state(env: str, network: str, ks) -> str:
    """'empty', 'pending <blocks> (<version>)', or '?' if the check failed."""
    if ks is None:
        return ""
    from .probe import ChainProbe

    probe = ChainProbe(env, network, ks)
    try:
        header = probe.wal_pending()
    except Exception as e:
        logger.warning(f"{network}: WAL check failed: {e}")
        return "?"
    finally:
        probe.close()
    if header is None:
        return "empty"
    return (
        f"pending {header['block_lo']}-{header['block_hi']} ({header['code_version']})"
    )


def _running_somewhere(store, network: str) -> bool:
    """A daemon is in the middle of a cycle of this network. Heartbeats expire
    after DAEMON_TTL_SECONDS, so a dead daemon does not count for long."""
    return any(network in info.get("running", []) for _, info in store.list_daemons())


@sync.command("pause")
@require_environment()
@require_currency()
@click.option("--reason", required=True, help="Shown in `sync status` and alerts.")
@click.option(
    "--for",
    "duration",
    default=None,
    help="Resume automatically after this long, e.g. 30m, 6h, 2d. "
    "Default: until `sync resume`.",
)
@click.option(
    "--wait",
    is_flag=True,
    help="Wait until the running cycle (if any) has finished, then show the "
    "network's status, WAL check included. Ctrl-C stops waiting; the pause stays.",
)
def pause(env: str, currency: str, reason: str, duration: Optional[str], wait: bool):
    """Stop starting new jobs for a network. A running job finishes first."""
    seconds = _parse_duration(duration) if duration else None
    by = os.environ.get("USER") or "unknown"
    store = _store(env)
    store.pause(currency, reason, by=by, seconds=seconds)
    console.print(
        f"Paused {env}/{currency}"
        + (f" for {duration}" if duration else " until resumed")
        + ". A job already running finishes first."
    )
    if not wait:
        return
    if _running_somewhere(store, currency):
        console.print(f"Waiting for the running {currency} cycle to finish ...")
        while _running_somewhere(store, currency):
            if store.get_pause(currency) is None:
                # expired (--for) or resumed meanwhile: cycles start again
                console.print(f"{env}/{currency} is no longer paused, stopped waiting.")
                return
            time.sleep(_WAIT_POLL_SECONDS)
    console.print(f"No {currency} job running.")
    _print_status(env, store, only=currency)


_WAIT_POLL_SECONDS = 2


@sync.command("resume")
@require_environment()
@require_currency()
def resume(env: str, currency: str):
    """Lift a pause (also one set by the reorg check)."""
    if _store(env).resume(currency):
        console.print(f"Resumed {env}/{currency}.")
    else:
        console.print(f"{env}/{currency} was not paused.")


_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_duration(value: str) -> int:
    m = _DURATION.match(value.strip())
    if not m or int(m.group(1)) == 0:
        raise click.BadParameter(
            f"{value!r}: use a number and s/m/h/d, e.g. 6h", param_hint="--for"
        )
    return int(m.group(1)) * _UNIT_SECONDS[m.group(2)]


def _shorten(text: str, limit: int = 80) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _fmt_int(v) -> str:
    return f"{v:,}" if isinstance(v, int) else ""


def _fmt_ts(ts) -> str:
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, timezone.utc)
    age = time.time() - ts
    when = dt.strftime("%m-%d %H:%M")
    if abs(age) < 3600:
        return f"{when} ({int(abs(age) // 60)}m {'ago' if age >= 0 else 'ahead'})"
    return when
