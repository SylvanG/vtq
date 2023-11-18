import unittest
from unittest import mock
import time
from vtq.coordinator.notification_worker import SimpleNotificationWorker


class SimpleNotificationWorkerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = SimpleNotificationWorker(0.1)

    def test_connect(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        time.sleep(0.11)
        self.worker.disconnect(f)
        f.assert_called_once()

    def test_connect_repeat_callback(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        time.sleep(0.11)
        f.assert_called_once()
        f.reset_mock()
        time.sleep(0.1)
        f.assert_called_once()
        self.worker.disconnect(f)

    def test_connect_idempotent(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        self.worker.connect_to_available_task(f)
        f.assert_not_called()
        time.sleep(0.11)
        f.assert_called_once()
        self.worker.disconnect(f)

    def test_disconnect(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        self.worker.disconnect(f)
        time.sleep(0.11)
        f.assert_not_called()

    def test_disconnect_idempotent(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        self.worker.disconnect(f)
        self.worker.disconnect(f)
        time.sleep(0.11)
        f.assert_not_called()

    def test_stop(self):
        f = mock.Mock()
        self.worker.connect_to_available_task(f)
        self.worker.stop()
        time.sleep(0.11)
        f.assert_called_once()
        f.reset_mock()
        time.sleep(0.11)
        f.assert_not_called()
