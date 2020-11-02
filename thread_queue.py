import threading
from typing import Any, List
import queue


class ThreadQueue:

    end_signal = object()

    def __init__(self, nthreads: int, fn, payloads: List[Any]):
        self.nthreads = nthreads
        self.fn = fn
        self.queue = queue.Queue()
        self.payloads = payloads
        for payload in payloads:
            self.queue.put(payload)

    def run(self):
        self.threads = [
            threading.Thread(target=self.worker) for _ in range(self.nthreads)
        ]
        for t in self.threads:
            t.start()
        self.queue.join()
        for _ in self.threads:
            self.queue.put(self.end_signal)
        for t in self.threads:
            t.join()

    def worker(self):
        while True:
            payload = self.queue.get()
            if payload is self.end_signal:
                return
            self.fn(payload)
            self.queue.task_done()