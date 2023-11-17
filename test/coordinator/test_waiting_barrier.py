import unittest
import threading
import time
from vtq.coordinator import waiting_barrier


class SimpleWaitingBarrierTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.barrier = waiting_barrier.SimpleWaitingBarrier()

    def _wait(self, timeout=0):
        with self.barrier.wait(timeout) as rv:
            if rv:
                time.sleep(0.3)

    def test_wait(self):
        with self.barrier.wait() as rv:
            assert rv is True

    def test_wait_timeout(self):
        with self.barrier.wait(1) as rv:
            assert rv is True

    def test_wait_false(self):
        t = threading.Thread(target=self._wait)
        t.start()
        with self.barrier.wait() as rv:
            assert rv is False

    def test_wait_timeout_false(self):
        t = threading.Thread(target=self._wait)
        t.start()
        with self.barrier.wait(0.1) as rv:
            assert rv is False

    def test_wait_timeout_true(self):
        t = threading.Thread(target=self._wait)
        t.start()
        with self.barrier.wait(0.4) as rv:
            assert rv is True

    def test_is_clear(self):
        assert self.barrier.is_clear is True

    def test_is_clear_with_one_waiting_item(self):
        t = threading.Thread(target=self._wait)
        t.start()
        assert self.barrier.is_clear is False
        time.sleep(0.4)
        assert self.barrier.is_clear is True

    def test_is_clear_with_waiting_items(self):
        t = threading.Thread(target=self._wait)
        t2 = threading.Thread(target=self._wait)
        t.start()
        t2.start()
        assert self.barrier.is_clear is False
        time.sleep(0.4)
        assert self.barrier.is_clear is True
