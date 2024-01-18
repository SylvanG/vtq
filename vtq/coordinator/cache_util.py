import contextlib
import functools
import threading
from collections.abc import Callable

_NOT_FOUND = object()


def cached_method[C, R](
    fn: Callable[[C], R], use_lock: bool = True, cached_name: str = ""
) -> Callable[[C], R]:
    cached_name = cached_name or fn.__name__ + "_cached"
    lock = threading.Lock() if use_lock else None

    @functools.wraps(fn)
    def wrapper(self: C) -> R:
        cached = self.__dict__.get(cached_name, _NOT_FOUND)
        if cached is _NOT_FOUND:
            lock_context = lock if lock else contextlib.nullcontext()
            with lock_context:
                if cached is _NOT_FOUND:
                    cached = fn(self)
                    self.__dict__[cached_name] = cached
        return cached

    return wrapper
