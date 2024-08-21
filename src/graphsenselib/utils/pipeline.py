import multiprocessing as mp
import os
import random
import traceback
from functools import cache
from queue import Empty


class PipelineError:
    def __init__(self, exc, tb, task_name):
        self.exc = exc
        self.tb = tb
        self.task_name = task_name

    def __repr__(self):
        return repr(self.exc) + " @ " + self.task_name


def get_process_graph(pipedef, panic_signal, max_queue_size):
    ips = [mp.Queue(max_queue_size) for _ in pipedef]
    pas = []
    outs = []
    for s, q in zip(pipedef, ips):
        ps, outqs = s.get_flow(q, panic_signal)
        pas.extend(ps)
        outs.extend(outqs)

    return (ips, pas, outs)


def is_stop_signal(msg):
    return isinstance(msg, StopPipeline)


def is_pipline_error(msg):
    return isinstance(msg, PipelineError)


def is_special_signal(msg):
    return is_pipline_error(msg) or is_stop_signal(msg)


def set_context(ctx, *initial_data, **kwargs):
    for dictionary in initial_data:
        for key in dictionary:
            setattr(ctx, key, dictionary[key])
    for key in kwargs:
        setattr(ctx, key, kwargs[key])


class Context:
    pass


class StopPipeline:
    pass


class Pipeline:
    def __init__(
        self, pipedef, collector_task=None, collector_args=(), max_queue_size=0
    ):
        self.pipedef = pipedef
        self.panic_signal = mp.Event()
        ins, ps, outs = get_process_graph(pipedef, self.panic_signal, max_queue_size)
        self.has_joined = False
        if collector_task:
            if isinstance(collector_task, Process):
                if collector_args != ():
                    raise ValueError(
                        "collector_args are not supported for process collector."
                    )
                p, op = collector_task.get_flow(outs, self.panic_signal)
                ps.extend(p)
                outs = op
            else:
                outq = mp.Queue()
                ps.append(
                    mp.Process(
                        target=collector_task,
                        args=(
                            self.panic_signal,
                            outs,
                            outq,
                            *collector_args,
                        ),
                        daemon=True,
                    )
                )
                outs = [outq]

        self.graph = (ins, ps, outs)
        self.stopped = False

    def run(self):
        for p in self._processes:
            p.start()
        return self

    @property
    def is_in_panic_mode(self):
        return self.panic_signal.is_set()

    @property
    def failed(self):
        return self.is_in_panic_mode

    def raise_on_failed(self):
        exs = self.get_errors()
        if len(exs) > 0:
            raise exs[0].exc

    def _join(self):
        if not self.has_joined:
            self.has_joined = True
            for p in self._processes:
                p.join()
        self.stopped = True

    def join(self):
        self.put(StopPipeline())
        self._join()

    def __enter__(self):
        return self.run()

    def get_errors(self):
        res = self.get_results()
        return [ex for ex in res if is_pipline_error(ex)]

    @cache
    def get_results(self):
        if not self.stopped:
            raise ValueError(
                "Get results should only be used after pipeline is stopped."
            )
        res = []
        queues = self.outputs_queues
        while True:
            all_empty = 0
            for q in queues:
                if not q.empty():
                    res.append(q.get())
                else:
                    all_empty += 1
            if all_empty == len(queues):
                break

        return [r for r in res if not is_stop_signal(r)]

    def __exit__(self, exc_type, exc_value, tb):
        self.join()

    @property
    def _inqueues(self):
        ips, _, _ = self.graph
        return ips

    @property
    def _processes(self):
        _, ps, _ = self.graph
        return ps

    def put(self, item):
        if self.is_in_panic_mode:
            item = StopPipeline()
        for q in self._inqueues:
            q.put(item)

    @property
    def outputs_queues(self):
        _, _, ops = self.graph
        return ops


class Process:
    def __init__(
        self, task, init_func=None, init_args=None, teardown_func=None, name="worker"
    ):
        self.init_args = init_args
        self.init_func = init_func
        self.teardown_func = teardown_func
        self.task = task
        self.context = Context()
        self.iname = name
        self.nexts = []

    @property
    def name(self):
        return f"{self.iname} at {os.getpid()}"

    def __or__(self, next_step):
        return self.pipe(next_step)

    def pipe(self, next_step):
        self.nexts.append(next_step)
        return self

    def get_flow(self, inp, panic_signal):
        ps = []
        outsa = []
        if len(self.nexts) > 0:
            outputs = [mp.Queue() for _ in self.nexts]
            for n, q in zip(self.nexts, outputs):
                p, outs = n.get_flow(q, panic_signal)
                ps.extend(p)
                outsa.extend(outs)
        else:
            out = [mp.Queue()]
            return [
                mp.Process(
                    target=self,
                    name=self.name,
                    args=(inp, out, panic_signal),
                    daemon=True,
                )
            ], out

        return (
            ps
            + [
                mp.Process(
                    target=self,
                    name=self.name,
                    args=(inp, outputs, panic_signal),
                    daemon=True,
                )
            ],
            outsa,
        )

    def put_all(self, sinks, data):
        if isinstance(sinks, list):
            for s in sinks:
                s.put(data)
        else:
            sinks.put(data)

    def get_one(self, sources, timeout=1):
        if isinstance(sources, list):
            q = random.choice(sources)
            return q.get(timeout=timeout)
        else:
            return sources.get(timeout=timeout)

    def handle_msg(self, sources, sinks) -> bool:
        """Default handling of messages, get message and process

        Args:
            source (mp.Queue): Description
            sinks (List[mp.Queue]): Description

        Returns:
            bool: if worker should shutdown
        """
        try:
            data = self.get_one(sources, timeout=4)

            if is_special_signal(data):
                self.put_all(sinks, data)
                return True

            if data is not None:
                ret = self.task(self.context, data)
                self.put_all(sinks, ret)
        except Empty:
            return False

        return False

    def handle_msg_panic_mode(self, sources, sinks) -> bool:
        """Panic mode, if some worker had an unhandled exception
        only passes exceptions and stop signals, but does no work anymore.
        Args:
            source (mp.Queue): Description
            sinks (List[mp.Queue]): Description

        Returns:
            bool: if worker should shutdown
        """
        try:
            data = self.get_one(sources)

            if is_special_signal(data):
                self.put_all(sinks, data)
                return True

            return False
        except Empty:
            return False

    def __call__(self, source, sinks, panic_signal):
        if self.init_func:
            self.init_func(self.context, *self.init_args)
        try:
            while True:
                try:
                    kill_worker = False
                    if not panic_signal.is_set():
                        kill_worker = self.handle_msg(source, sinks)
                    else:
                        kill_worker = self.handle_msg_panic_mode(source, sinks)

                    if kill_worker:
                        break
                except KeyboardInterrupt:
                    pass
                except Exception as e:
                    panic_signal.set()
                    self.put_all(
                        sinks,
                        PipelineError(
                            e, traceback.TracebackException.from_exception(e), self.name
                        ),
                    )
        finally:
            if self.teardown_func:
                self.teardown_func(self.context)


class SyncCollector(Process):
    def get_one(self, sources):
        return [q.get() for q in sources]

    def handle_msg(self, sources, sinks) -> bool:
        """Default handling of messages, get message and process

        Args:
            source (mp.Queue): Description
            sinks (List[mp.Queue]): Description

        Returns:
            bool: if worker should shutdown
        """
        try:
            datas = self.get_one(sources)

            ret, end = self.task(self.context, datas)
            if end is None:
                end = False

            if ret is not None:
                self.put_all(sinks, ret)

            return end
        except Empty:
            return False

    def handle_msg_panic_mode(self, sources, sinks) -> bool:
        _ = self.handle_msg(sources, sinks)
        return True
