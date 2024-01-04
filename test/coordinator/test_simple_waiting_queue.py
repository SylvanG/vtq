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

        self.callback: Callable = None  # type: ignore

        def notification_hook(callback):
            self.callback = callback

        self.waiting_queue = simple_waiting_queue.SimpleWaitingQueue(
            fetcher=fetcher,
            notification_hook=notification_hook,
            default_factory=list,
        )

    def tearDown(self) -> None:
        self.queue = queue.Queue()
        self.callback = None  # type: ignore

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
        rv = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            rv.append(fs.result())

        threads = []
        for i in range(3):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        for i in range(1, 6):
            self.queue.put(i)
        self.callback()

        for t in threads:
            t.join()

        assert rv == [[1, 2], [3, 4], [5]], rv

    def test_cancel(self):
        rv = []
        futures = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            futures.append(fs)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        time.sleep(0.1)
        assert rv == []

        futures[0].cancel()

        time.sleep(0.1)
        # cancelled then returned default value []
        assert rv == [[]]

        for i in range(1, 6):
            self.queue.put(i)
        self.callback()

        for t in threads:
            t.join()

        assert rv == [[], [1, 2]], rv

    def test_timeout(self):
        rv = []
        futures = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(timeout=0.2, num=num)
            futures.append(fs)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        time.sleep(0.1)
        assert rv == [], rv

        for t in threads:
            t.join()

        assert rv == [[], []], rv

    def test_empty(self):
        assert self.waiting_queue.empty()

        rv = []
        futures = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            futures.append(fs)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        assert not self.waiting_queue.empty()

        futures[0].cancel()
        time.sleep(0.1)
        assert rv == [[]]
        assert not self.waiting_queue.empty()

        self.queue.put(1)
        self.callback()
        time.sleep(0.1)
        assert rv == [[], [1]]
        assert self.waiting_queue.empty()

    def test_notify_without_data(self):
        rv = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        self.callback()
        time.sleep(0.1)
        assert rv == []

        for i in range(1, 6):
            self.queue.put(i)
        self.callback()
        time.sleep(0.1)
        assert rv == [[1, 2], [3, 4]], rv

    def test_data_exists(self):
        self.waiting_queue.data_exists = lambda data: data is not None

        rv = []

        def wait(num: int = 1):
            fs = self.waiting_queue.wait(num=num)
            rv.append(fs.result())

        threads = []
        for i in range(2):
            t = threading.Thread(target=wait, args=(2,))
            t.start()
            threads.append(t)

        self.callback()
        time.sleep(0.1)
        # [] returned by the fetcher will be taken as a valid non-empty data
        assert rv == [[], []], rv
