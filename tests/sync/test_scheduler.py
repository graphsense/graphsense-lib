import threading

import pytest

from graphsenselib.config import Environment
from graphsenselib.sync.jobs import Outcome, StepResult
from graphsenselib.sync.scheduler import SyncScheduler, _Slots
from graphsenselib.sync.state import MemoryStateStore


def make_env(sync=None, **ingest):
    return Environment(
        cassandra_nodes=["localhost"],
        keyspaces={
            "zec": {
                "raw_keyspace_name": "zec_raw",
                "transformed_keyspace_name": "zec_transformed",
                "schema_type": "utxo",
                "ingest_config": {"node_reference": "http://node:8232", **ingest},
            }
        },
        sync=sync
        if sync is not None
        else {"networks": {"zec": {"max_blocks_per_job": 50}}},
    )


class FakeProbe:
    def __init__(self, tip=1000, raw=100, transformed=100):
        self.tip = tip
        self.raw = raw
        self.transformed = transformed
        self.raw_hashes = {}
        self.node_hashes = {}

    def node_tip(self):
        return self.tip

    def raw_height(self):
        return self.raw

    def transformed_height(self):
        return self.transformed

    def raw_block_hash(self, h):
        return self.raw_hashes.get(h, f"{h:064x}")

    def node_block_hash(self, h):
        return self.node_hashes.get(h, f"{h:064x}")

    def raw_block_timestamp(self, h):
        return None

    def reset(self):
        pass

    def close(self):
        pass


class FakeRunner:
    """Records commands; answers with queued outcomes. A successful ingest
    advances the probe's raw height to its --end-block, a delta update
    brings transformed up to raw."""

    def __init__(self, probe):
        self.probe = probe
        self.calls = []
        self.outcomes = {}

    def run(self, step, args, timeout_seconds, stop, label):
        self.calls.append((step, args))
        queued = self.outcomes.get(step)
        outcome, code = queued.pop(0) if queued else (Outcome.OK, 0)
        if outcome is Outcome.OK:
            if step == "ingest" and "--end-block" in args:
                self.probe.raw = int(args[args.index("--end-block") + 1])
            if step == "delta_update":
                self.probe.transformed = self.probe.raw
        return StepResult(step, outcome, code, 1.0, ["line 1", "boom"])


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def setup():
    def make(env=None, probe=None, channel="default", store=None):
        probe = probe or FakeProbe()
        runner = FakeRunner(probe)
        alerts = []
        clock = Clock()
        sched = SyncScheduler(
            env="prod",
            channel=channel,
            store=store or MemoryStateStore(),
            load_environment=lambda: env or make_env(),
            runner=runner,
            notify=lambda topic, msg, key, every: alerts.append((key, msg)),
            probe_factory=lambda e, c, ks: probe,
            clock=clock,
            wall=clock,
        )
        return sched, probe, runner, alerts, clock

    return make


def one_tick(sched):
    sched.tick()
    for rt in sched._runtime.values():
        if rt.future is not None:
            rt.future.result()


def test_ingest_slice_then_delta_update(setup):
    sched, probe, runner, alerts, clock = setup(env=make_env(reorg_backoff_blocks=10))
    one_tick(sched)

    (step1, ingest), (step2, delta) = runner.calls
    assert step1 == "ingest"
    assert ingest[:6] == ["ingest", "from-node", "-e", "prod", "-c", "zec"]
    assert "--no-staleness-check" in ingest
    # first cycle: exchange rates are due, so the pre-step stays on
    assert "--no-exchange-rates" not in ingest
    # bounded slice: raw 100 + 50, well below tip 1000 - backoff 10
    assert ingest[-2:] == ["--end-block", "150"]
    assert step2 == "delta_update"
    # no --end-block: it would disable delta-update's torn-bookkeeping check
    assert delta == ["delta-update", "update", "-e", "prod", "-c", "zec"]
    # still catching up: the next cycle is due at once
    assert sched._runtime["zec"].next_due == clock.t
    status = sched.store.read_status("zec")
    assert status["raw"] == 150 and status["target"] == 990
    assert status["last_result"] == "ok"
    assert alerts == []


@pytest.mark.parametrize("switch", ["keyspace", "sync"])
def test_delta_update_switched_off(setup, switch):
    env = make_env()
    if switch == "keyspace":
        env.keyspaces["zec"].disable_delta_updates = True
    else:
        env.sync.networks["zec"].delta_update = False
    sched, probe, runner, alerts, clock = setup(env=env)
    one_tick(sched)

    assert [step for step, _ in runner.calls] == ["ingest"]
    assert probe.transformed == 100
    assert sched.store.read_status("zec")["last_result"] == "ok"
    assert alerts == []


def test_extra_args_and_rates_interval(setup):
    env = make_env(
        sync={
            "rates_every_minutes": 30,
            "networks": {
                "zec": {
                    "ingest_args": ["--sinks", "cassandra"],
                    "delta_update_args": ["--forward-fill-rates"],
                }
            },
        },
        reorg_backoff_blocks=10,
    )
    sched, probe, runner, _, clock = setup(env=env, probe=FakeProbe(raw=980))
    one_tick(sched)
    assert runner.calls[0][1][6:8] == ["--sinks", "cassandra"]
    assert runner.calls[1][1][-1] == "--forward-fill-rates"

    # next poll, 10 new blocks, rates not due yet
    runner.calls.clear()
    probe.tip += 10
    clock.t += 60
    one_tick(sched)
    assert "--no-exchange-rates" in runner.calls[0][1]

    # 31 minutes later, at the tip: ingest still runs, for the rates alone
    runner.calls.clear()
    clock.t += 31 * 60
    one_tick(sched)
    step, args = runner.calls[0]
    assert step == "ingest"
    assert "--no-exchange-rates" not in args
    assert "--end-block" not in args


def test_up_to_date_runs_nothing(setup):
    sched, probe, runner, _, clock = setup(
        env=make_env(reorg_backoff_blocks=10), probe=FakeProbe(raw=990, transformed=990)
    )
    one_tick(sched)  # first cycle: rates only
    runner.calls.clear()
    clock.t += 60
    one_tick(sched)
    assert runner.calls == []
    assert sched._runtime["zec"].next_due == clock.t + 30


def test_failures_back_off_alert_and_recover(setup):
    sched, probe, runner, alerts, clock = setup()
    runner.outcomes["ingest"] = [(Outcome.FAILED, 1)] * 3
    delays = []
    for _ in range(3):
        one_tick(sched)
        delays.append(sched._runtime["zec"].next_due - clock.t)
        clock.t = sched._runtime["zec"].next_due
    assert delays == [30, 60, 120]
    # alert only from the third failure in a row, with the job's last lines
    assert len(alerts) == 1
    key, msg = alerts[0]
    assert key == "sync-failure:prod:zec"
    assert "exited with 1" in msg and "boom" in msg

    one_tick(sched)
    assert "recovered" in alerts[-1][1]
    assert sched._runtime["zec"].failures == 0


def test_lock_contention_is_not_a_failure(setup):
    sched, probe, runner, alerts, clock = setup()
    runner.outcomes["ingest"] = [(Outcome.BUSY, 911)]
    one_tick(sched)
    rt = sched._runtime["zec"]
    assert rt.failures == 0
    assert [s for s, _ in runner.calls] == ["ingest"]
    assert rt.next_due == clock.t + 30


def test_reorg_pauses_network(setup):
    probe = FakeProbe(raw=100)
    probe.node_hashes[99] = "ff" * 32
    sched, _, runner, alerts, clock = setup(probe=probe)
    one_tick(sched)
    assert runner.calls == []
    pause = sched.store.get_pause("zec")
    assert "reorg detected: block 99" in pause["reason"]
    assert alerts[0][0] == "sync-reorg:prod:zec"

    clock.t += 3600
    one_tick(sched)
    assert runner.calls == []


def test_reorg_check_ignores_heights_above_node_tip(setup):
    # node restarted and is behind us: nothing to compare, no false reorg
    probe = FakeProbe(tip=98, raw=100, transformed=100)
    probe.node_hashes[100] = probe.node_hashes[99] = "ff" * 32
    sched, _, runner, _, _ = setup(probe=probe)
    one_tick(sched)
    assert sched.store.get_pause("zec") is None


def test_pause_and_channel_and_claims(setup):
    store = MemoryStateStore()
    store.pause("zec", "node resync", by="me")
    sched, _, runner, _, _ = setup(store=store)
    one_tick(sched)
    assert runner.calls == []

    store.resume("zec")
    canary = make_env(sync={"networks": {"zec": {"channel": "canary"}}})
    sched, _, runner, _, _ = setup(env=canary, store=store)
    one_tick(sched)
    assert runner.calls == []

    store.claim("zec", "someone else")
    sched, _, runner, _, _ = setup(store=store)
    one_tick(sched)
    assert runner.calls == []


def test_network_moved_to_other_channel_releases_claim(setup):
    envs = [make_env()]
    store = MemoryStateStore()
    sched, _, runner, _, clock = setup(store=store)
    sched._load_environment = lambda: envs[0]
    one_tick(sched)
    assert store.claim_owner("zec") == sched.daemon_id

    envs[0] = make_env(sync={"networks": {"zec": {"channel": "canary"}}})
    one_tick(sched)
    assert store.claim_owner("zec") is None


def test_bad_config_reload_keeps_last_good(setup):
    sched, _, runner, _, clock = setup()
    good = make_env()
    calls = []

    def load():
        calls.append(1)
        if len(calls) > 1:
            raise ValueError("broken yaml")
        return good

    sched._load_environment = load
    one_tick(sched)
    runner.calls.clear()
    clock.t += 3600
    one_tick(sched)
    assert runner.calls  # still scheduling on the old config


def test_stop_during_slot_wait():
    slots = _Slots()
    slots.resize(1)
    assert slots.acquire(threading.Event()) is not None
    stop = threading.Event()
    stop.set()
    assert slots.acquire(stop) is None


def test_slot_given_back_where_taken_after_resize():
    # config reload while a job holds a slot: releasing it on the new,
    # untouched semaphore would raise ValueError (BoundedSemaphore)
    slots = _Slots()
    slots.resize(1)
    sem = slots.acquire(threading.Event())
    slots.resize(2)
    sem.release()
    assert slots.acquire(threading.Event()) is not None


def compacting_env(**sync):
    return make_env(
        sync={"networks": {"zec": {}}, **sync},
        raw_keyspace_file_sinks={"delta": {"directory": "/data/zec"}},
        auto_compact="1w;sunday",
    )


def test_compaction_is_a_step_of_its_own(setup):
    sched, probe, runner, _, clock = setup(
        env=compacting_env(), probe=FakeProbe(raw=100, transformed=90)
    )
    one_tick(sched)
    steps = [s for s, _ in runner.calls]
    assert steps == ["ingest", "delta_update", "compact"]
    # never inside the ingest job, where it would count against its timeout
    assert "--no-auto-compact" in runner.calls[0][1]
    assert runner.calls[2][1] == [
        "ingest", "delta-lake", "auto-compact", "-e", "prod", "-c", "zec",
    ]  # fmt: skip

    # checked once per compaction_check_every_minutes (default 60)
    runner.calls.clear()
    clock.t += 30 * 60
    one_tick(sched)
    assert "compact" not in [s for s, _ in runner.calls]
    clock.t += 31 * 60
    one_tick(sched)
    assert "compact" in [s for s, _ in runner.calls]


def test_failed_compaction_waits_for_next_check(setup):
    sched, probe, runner, alerts, clock = setup(env=compacting_env())
    runner.outcomes["compact"] = [(Outcome.TIMEOUT, None)]
    one_tick(sched)
    rt = sched._runtime["zec"]
    assert rt.failures == 1
    # the retry cycle ingests and updates, but does not compact again yet
    runner.calls.clear()
    clock.t = rt.next_due
    one_tick(sched)
    assert "compact" not in [s for s, _ in runner.calls]
    assert rt.failures == 0


def test_no_compaction_without_schedule_or_delta_sink(setup):
    for env in (
        make_env(raw_keyspace_file_sinks={"delta": {"directory": "/d"}}),
        make_env(auto_compact="1d"),
    ):
        sched, probe, runner, _, _ = setup(env=env)
        one_tick(sched)
        assert "compact" not in [s for s, _ in runner.calls]


def test_signals_escalate(setup):
    import signal

    sched, *_ = setup()
    sched.runner.running = lambda: {"zec ingest": 120.0}
    sched.runner.kill_now = threading.Event()
    sched._on_signal(signal.SIGINT, None)
    assert sched._stop.is_set() and not sched.runner.kill_now.is_set()
    sched._on_signal(signal.SIGINT, None)
    assert not sched.runner.kill_now.is_set()
    sched._on_signal(signal.SIGTERM, None)
    assert sched.runner.kill_now.is_set()


def test_heartbeat_lists_claimed_networks(setup):
    sched, probe, runner, alerts, clock = setup()
    one_tick(sched)
    sched.tick()  # the heartbeat after the cycle
    [(_, info)] = sched.store.list_daemons()
    assert info["networks"] == ["zec"]
    assert info["channel"] == "default"


def test_stop_grace_follows_config(setup):
    sched, probe, runner, alerts, clock = setup()
    sched.tick()
    assert runner.stop_grace_seconds == 480
    env = make_env(
        sync={"stop_grace_seconds": 300, "networks": {"zec": {}}},
    )
    sched._load_environment = lambda: env
    sched.tick()
    assert runner.stop_grace_seconds == 300


def test_unconfigured_network_warned_once_and_once_exits_2(setup, caplog):
    sched, probe, runner, alerts, clock = setup()
    sched.only = {"btc"}  # sync run -c btc, but only zec is configured
    with caplog.at_level("WARNING"):
        for _ in range(3):
            sched.tick()
    warnings = [r for r in caplog.records if "Not configured" in r.message]
    assert len(warnings) == 1 and "sync.networks.btc" in warnings[0].message
    assert runner.calls == []

    sched, *_ = setup()
    sched.only = {"btc"}
    result = []
    # off the main thread, so run() leaves pytest's signal handlers alone
    t = threading.Thread(target=lambda: result.append(sched.run(once=True)))
    t.start()
    t.join()
    assert result == [2]


def test_missing_sync_section_warned_once(setup, caplog):
    env = make_env()
    env.sync = None
    sched, *_ = setup(env=env)
    with caplog.at_level("WARNING"):
        for _ in range(3):
            sched.tick()
    assert sum("Not configured" in r.message for r in caplog.records) == 1
