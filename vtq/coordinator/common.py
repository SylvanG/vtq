import functools
import time


def log_time(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        s = time.perf_counter()
        try:
            return f(*args, **kwargs)
        except:
            d = time.perf_counter() - s
            print(f"{f.__name__}: {d}")
            raise

    return wrap
