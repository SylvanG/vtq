import abc
from abc import abstractmethod

from vtq.task import Task


class ReceiveFuture(abc.ABC):
    @abstractmethod
    def result(self) -> list[Task]:
        """Return an empty list asap when cancel() is called."""
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError
