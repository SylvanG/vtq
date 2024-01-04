import queue
import threading
import time
import unittest
from collections.abc import Callable

from vtq.coordinator import simple_waiting_queue


class SimpleWaitingQueueTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = queue.Queue()

        def fetcher(num: int = 1):
            rv = []
            while len(rv) < num:
                try:
                    item = self.queue.get_nowait()
                    rv.append(item)
                except queue.Empty:
                    break

            return rv

        self.callback: Callable = None

        def notification_hook(callback):
            self.callback = callback

        self.waiting_queue = simple_waiting_queue.SimpleWaitingQueue(
            fetcher=fetcher,
            notification_hook=notification_hook,
            default_factory=list,
        )

    def tearDown(self) -> None:
        self.queue = queue.Queue()
        self.callback = None

    def test_with_exsting_data(self):
        rv = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            rv.extend(fs.result())

        for i in range(1, 4):
            self.queue.put(i)

        t = threading.Thread(target=wait, args=(1,))
        t.start()
        t.join()

        assert rv == [1]

        t = threading.Thread(target=wait, args=(3,))
        t.start()
        t.join()

        assert rv == [1, 2, 3]

    def test_waiting_for_data(self):
        rv = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        time.sleep(0.1)
        assert rv == [], rv

        self.queue.put(1)
        self.callback()
        time.sleep(0.1)
        assert rv == [[1]]

        for i in range(2, 5):
            self.queue.put(i)
        self.callback()

        for t in threads:
            t.join()

        assert rv == [[1], [2, 3]], rv

    def test_notify_next(self):
        ...

    def test_cancel(self):
        ...

    def test_timeout(self):
        ...

    def test_empty(self):
        ...
