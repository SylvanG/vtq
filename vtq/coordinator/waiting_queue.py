import abc
import typing
from abc import abstractmethod
from collections.abc import Callable
from typing import Protocol


class ReceiveFuture[R](abc.ABC):
    @abstractmethod
    def result(self) -> R:
        raise NotImplementedError

    @abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError

    def __hash__(self) -> int:
        return id(self)


class SimpleReceiveFuture[R](ReceiveFuture[R]):
    def __init__(self, result: R) -> None:
        self._result = result

    def result(self) -> R:
        return self._result

    def cancel(self) -> None:
        return


class NotificaitonHook(Protocol):
    def __call__(self, callback: Callable[[], None], /):
        ...


class WaitingQueue[**P, R, D]:
    def __init__(
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
    ) -> None:
        self.fetcher = fetcher
        self.notification_hook = notification_hook
        self.default_factory = default_factory

    def wait(
        self,
        timeout: float | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ReceiveFuture[R | D]:
        ...

    def empty(self) -> bool:
        ...


class WaitingQueueFactory(Protocol):
    @typing.overload
    def __call__[**P, R](
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
    ) -> WaitingQueue[P, R, None]:
        ...

    @typing.overload
    def __call__[**P, R, D](
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_value: D,
    ) -> WaitingQueue[P, R, D]:
        ...

    @typing.overload
    def __call__[**P, R, D](
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
    ) -> WaitingQueue[P, R, D]:
        ...
