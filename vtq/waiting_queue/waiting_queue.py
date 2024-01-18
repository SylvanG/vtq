import typing
from collections.abc import Callable
from typing import Protocol

from .receive_future import ReceiveFuture


class NotificaitonHook(Protocol):
    def __call__(self, callback: Callable[[], None], /):
        ...


class WaitingQueue[**P, R, D]:
    def __init__(
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
        data_exists: Callable[[R], bool] = bool,
    ) -> None:
        self.fetcher = fetcher
        self.notification_hook = notification_hook
        self.default_factory = default_factory
        self.data_exists = data_exists

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
        data_exists: Callable[[R], bool] = bool,
    ) -> WaitingQueue[P, R, None]:
        ...

    @typing.overload
    def __call__[**P, R, D](
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_value: D,
        data_exists: Callable[[R], bool] = bool,
    ) -> WaitingQueue[P, R, D]:
        ...

    @typing.overload
    def __call__[**P, R, D](
        self,
        fetcher: Callable[P, R],
        notification_hook: NotificaitonHook,
        default_factory: Callable[[], D],
        data_exists: Callable[[R], bool] = bool,
    ) -> WaitingQueue[P, R, D]:
        ...
