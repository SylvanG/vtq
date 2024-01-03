from .waiting_queue import NotificaitonHook, ReceiveFuture, WaitingQueue
from collections.abc import Callable
import threading
import time


class ReceiveProxy[**P, R, D](ReceiveFuture[R | D]):
    def __init__(
        self,
        waiting_queue: "NonCancellableWaitingQueue[P, R, D]",
        timeout: float | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._waiting_queue = waiting_queue
        self.wait_until_seconds = timeout + time.time() if timeout is not None else None
        self.args = args
        self.kwargs = kwargs

    @property
    def timeout(self) -> float | None:
        if self.wait_until_seconds is None:
            return None
        return self.wait_until_seconds - time.time()

    def result(self) -> R | D:
        return self._waiting_queue.receive(self)

    def cancel(self) -> None:
        raise NotImplementedError


class NonCancellableWaitingQueue[**P, R, D](WaitingQueue[P, R, D]):
    def __init__(
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
    ) -> None:
        super().__init__(fetcher, notification_hook, default_factory)

        self._event = threading.Event()
        self._lock = threading.Lock()

        notification_hook(self._event.set)

    def empty(self) -> bool:
        return not self._lock.locked()

    def wait(
        self,
        timeout: float | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ReceiveProxy[P, R, D]:
        return ReceiveProxy(self, timeout, *args, **kwargs)

    def receive(self, proxy: ReceiveProxy[P, R, D]) -> R | D:
        with self._lock:
            event = self._event

            while True:
                # Prepare to listen to the notification (task add notify and delay task timer) before querying the table.
                event.clear()

                # query the table
                data = self.fetcher(*proxy.args, **proxy.kwargs)
                if data is not None:
                    return data

                if event.is_set():
                    # It's possible the new available task is taken by other process or service.
                    continue

                # wait for the avaiable tasks
                if not event.wait(proxy.timeout):
                    # timed out
                    return self.default_factory()
