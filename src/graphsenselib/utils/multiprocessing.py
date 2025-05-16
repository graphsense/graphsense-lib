from multiprocessing import Lock, Value


class MPCounter(object):
    def __init__(self, initval=0):
        self.val = Value("i", initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def decrement(self):
        with self.lock:
            self.val.value -= 1

    def value(self):
        with self.lock:
            return self.val.value
