import abc
from abc import abstractmethod
from collections.abc import Callable
import time
import threading


SubscriberType = Callable[[], None]


class NotificationWorker(abc.ABC):
    """Delay task avaiable or New task added or Virtual Queue state changes"""

    @abstractmethod
    def connect_to_available_task(self, subscriber: SubscriberType):
        """It means you will be able to start monitoring after the method finished.

        Note: It may call the subscriber more times than expected.
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self, subscriber: SubscriberType):
        raise NotImplementedError


class SimpleNotificationWorker(NotificationWorker):
    def __init__(self, interval=60) -> None:
        self._interval = interval
        self._subscribers: set[SubscriberType] = set()

        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._event = threading.Event()

    def _loop(self):
        event = self._event
        while not event.is_set():
            time.sleep(self._interval)
            for subscribe in self._subscribers:
                subscribe()

    def connect_to_available_task(self, subscriber: SubscriberType):
        self._subscribers.add(subscriber)

        # Start the loop if any subscribers.
        if not self._thread:
            with self._lock:
                if not self._thread:
                    self._event.clear()
                    self._thread = threading.Thread(target=self._loop)
                    self._thread.start()

    def disconnect(self, subscriber: SubscriberType):
        try:
            self._subscribers.remove(subscriber)
        except KeyError:
            pass

        if not self._subscribers and self._thread:
            with self._lock:
                if not self._subscribers and self._thread:
                    self._event.set()
                    self._thread = None
