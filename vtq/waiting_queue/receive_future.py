import abc
from abc import abstractmethod
from collections.abc import Callable, Hashable


class ReceiveFuture[R](abc.ABC, Hashable):
    @abstractmethod
    def result(self) -> R:
        raise NotImplementedError

    @abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError

    def __hash__(self) -> int:
        return id(self)

    def transform[T](self, transformer: Callable[[R], T]) -> "ReceiveFuture[T]":
        return TransformerReceiveFuture(self, transformer)


class TransformerReceiveFuture[R, O](ReceiveFuture[O]):
    def __init__(self, future: ReceiveFuture[R], transformer: Callable[[R], O]) -> None:
        self.future = future
        self.transformer = transformer

    def result(self) -> O:
        return self.transformer(self.future.result())

    def cancel(self):
        return self.future.cancel()

    def __hash__(self) -> int:
        return hash(self.future)


class SimpleReceiveFuture[R](ReceiveFuture[R]):
    def __init__(self, result: R) -> None:
        self._result = result

    def result(self) -> R:
        return self._result

    def cancel(self) -> None:
        return
