import functools
import time
import random


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


def wrs_distribution(weights: list[int], k: int) -> list[int]:
    """It combines the expectation value and WRS (Weighted Random Sampling) to efficiently compute the weighted distribution of k samples from the poulation.

    It should be noted that there may be bias for specific values of k within a given population. For instance, some items may exhibit a static probability, while others don't. This bias can lead to serious issues in some situations. However, it may still function effectively if it depends solely on the final distribution in a long-running program.
    """

    total_weights = sum(weights)
    distribution = [0] * len(weights)
    remainders = [0] * len(weights)
    total_count = 0

    for idx, weight in enumerate(weights):
        count, remainder = divmod(weight * k, total_weights)
        distribution[idx] = count
        remainders[idx] = remainder
        total_count += count

    if total_count < k:
        # For a given seed, the choices() function with equal weighting typically produces a different sequence than repeated calls to choice().
        for idx in random.choices(
            population=range(len(weights)),
            weights=remainders,  # we can filter out the items of 0 weight before do a WRS.
            k=k - total_count,
        ):
            distribution[idx] += 1

    return distribution
