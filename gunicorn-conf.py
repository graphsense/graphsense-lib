import multiprocessing
import os

timeout = 30
capture_output = True
accesslog = "-"
errorlog = "-"
loglevel = "debug"
bind = "0.0.0.0:9000"

num = multiprocessing.cpu_count() * 2
try:
    workers = int(os.getenv("NUM_WORKERS", num))
except ValueError:
    workers = num

try:
    threads = int(os.getenv("NUM_THREADS", num))
except ValueError:
    threads = num


def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)


def pre_fork(server, worker):
    pass


def pre_exec(server):
    server.log.info("Forked child, re-executing.")


def when_ready(server):
    server.log.info("Server is ready. Spawning workers")
