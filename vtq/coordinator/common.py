import functools
import time


def log_time(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        s = time.time()
        try:
            return f(*args, **kwargs)
        except:
            d = time.time() - s
            print(f"{f.__name__}: {d}")
            raise

    return wrap
