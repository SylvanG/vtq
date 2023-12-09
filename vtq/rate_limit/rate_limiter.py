import abc
from abc import abstractmethod
from collections.abc import Generator
from typing import Protocol
import contextlib
import dataclasses
from vtq.rate_limit.type import RateLimitType


class ResourceNotExistsError(Exception):
    pass


class ResourceAccess(abc.ABC):
    @property
    @abstractmethod
    def count(self) -> int:
        pass

    @property
    @abstractmethod
    def version(self) -> int:
        pass

    @property
    @abstractmethod
    def limit_reached(self) -> bool | None:
        """Indicates that if the resource limit has been reached."""
        pass

    @property
    @abstractmethod
    def until(self) -> float:
        pass


class ResourceAccessChange(abc.ABC):
    @property
    @abstractmethod
    def version(self) -> int:
        pass


class RateLimiter[T: ResourceAccess](abc.ABC):
    @abstractmethod
    def acquire_access(self, count=1) -> ResourceAccess:
        raise NotImplementedError

    @abstractmethod
    def release_access(self, count=1) -> ResourceAccessChange | None:
        raise NotImplementedError


class SimpleRateLimiter(RateLimiter):
    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name


class RateLimiterFactory(Protocol):
    def __call__(self, type: RateLimitType, resource_name: str) -> RateLimiter:
        pass
