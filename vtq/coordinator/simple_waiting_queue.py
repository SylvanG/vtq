import threading
import time
from collections import deque
from collections.abc import Callable

from .waiting_queue import NotificaitonHook, ReceiveFuture, WaitingQueue


class ReceiveProxy[**P, R, D](ReceiveFuture[R | D]):
    def __init__(
        self,
        waiting_queue: "SimpleWaitingQueue[P, R, D]",
        timeout: float | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._waiting_queue = waiting_queue
        self.wait_until_seconds = timeout + time.time() if timeout is not None else None
        self.args = args
        self.kwargs = kwargs

        self.event = threading.Event()
        self.cancelled = False
        self.notified = False  # notified normally by the fetcher data notification

    @property
    def timeout(self) -> float | None:
        if self.wait_until_seconds is None:
            return None
        return self.wait_until_seconds - time.time()

    def cancel(self) -> None:
        return self._waiting_queue.cancel(self)

    def result(self) -> R | D:
        return self._waiting_queue.receive(self)


class SimpleWaitingQueue[**P, R, D](WaitingQueue[P, R, D]):
    def __init__(
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
    ) -> None:
        super().__init__(fetcher, notification_hook, default_factory)

        self._lock = threading.Lock()
        self._waiting_queue = deque[ReceiveProxy[P, R, D]]()

        notification_hook(self._notify)

    def empty(self) -> bool:
        return not self._waiting_queue

    def wait(
        self,
        timeout: float | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ReceiveProxy[P, R, D]:
        return ReceiveProxy(self, timeout, *args, **kwargs)

    def receive(self, proxy: ReceiveProxy[P, R, D]) -> R | D:
        with self._lock:
            if not self._waiting_queue:
                if (data := self.fetcher(*proxy.args, **proxy.kwargs)) is not None:
                    return data

            self._waiting_queue.append(proxy)

        while True:
            timed_out = not proxy.event.wait(proxy.timeout)
            # if not timed out:
            # 1. cancelled
            # 2. notification hook
            # 3. by last top proxy which was just cancelled or timeout

            with self._lock:
                if timed_out or proxy.cancelled:
                    # Edge case: self is the top one and being notified and timed-out/cancelled near the same time.
                    if proxy.notified:
                        assert self._waiting_queue[0] is proxy
                        self._waiting_queue.popleft()
                        self._notify_next()
                    else:
                        self._waiting_queue.remove(proxy)
                    return self.default_factory()

                assert self._waiting_queue[0] is proxy

                if (data := self.fetcher(*proxy.args, **proxy.kwargs)) is not None:
                    self._waiting_queue.popleft()
                    self._notify_next()
                    return data

                proxy.event.clear()
                proxy.notified = False

    def cancel(self, proxy: ReceiveProxy[P, R, D]):
        with self._lock:
            proxy.cancelled = True
            proxy.event.set()

    def _notify(self):
        with self._lock:
            self._notify_next()

    def _notify_next(self):
        """Only used under the lock"""
        if self._waiting_queue:
            proxy = self._waiting_queue[0]
            proxy.notified = True
            proxy.event.set()
