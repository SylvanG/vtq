import abc
from abc import abstractmethod
from collections.abc import Generator
import contextlib
from contextlib import AbstractContextManager
import threading


class WaitingBarrier(abc.ABC):
    @property
    @abstractmethod
    def is_clear(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def wait(self, timeout: float = 0) -> AbstractContextManager[bool]:
        raise NotImplementedError


class SimpleWaitingBarrier(WaitingBarrier):
    def __init__(self) -> None:
        self._lock = threading.Lock()

    @property
    def is_clear(self) -> bool:
        return not self._lock.locked()

    @contextlib.contextmanager
    def wait(self, timeout: float = 0) -> Generator[bool]:
        try:
            if timeout == 0:
                rv = self._lock.acquire(blocking=False)
            else:
                rv = self._lock.acquire(blocking=True, timeout=timeout)
            yield rv
        finally:
            if rv:
                self._lock.release()
