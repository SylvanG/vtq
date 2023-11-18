import abc
import threading
from abc import abstractmethod
from collections.abc import Callable

SubscriberType = Callable[[], None]


class NotificationWorker(abc.ABC):
    """Delay task avaiable or New task added or Virtual Queue state changes

    Current, the NotificationWorker is in the auto (start/stop) model, the conenct/disconnect method will trigger the start/stop of the worker. In the future, it could add a property to enable/disable the automode.
    """

    @abstractmethod
    def connect_to_available_task(self, subscriber: SubscriberType):
        """It means you will be able to start monitoring after the method finished.

        Note: It may call the subscriber more times than expected.
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self, subscriber: SubscriberType):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Manually stop the worker."""


class SimpleNotificationWorker(NotificationWorker):
    def __init__(self, interval=60) -> None:
        self._interval = interval
        self._subscribers: set[SubscriberType] = set()

        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._event = threading.Event()

    def _loop(self):
        event = self._event
        while True:
            if event.wait(self._interval):
                return
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

    def stop(self):
        with self._lock:
            if self._thread:
                self._event.set()
                self._thread = None
