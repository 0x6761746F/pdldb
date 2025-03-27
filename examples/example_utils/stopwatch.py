from datetime import datetime
import time
from functools import wraps


def _time(units="ms"):
    if units == "s":
        return int(datetime.now().timestamp())
    if units == "ms":
        return time.time_ns() // 1000000
    if units == "ns":
        return time.time_ns()


def stopwatch(_func=None, *, units="ms", return_time=False):
    assert units in ["ns", "ms", "s"]

    def _wrapper(func):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            a = _time(units=units)
            result = func(*args, **kwargs)
            duration = _time(units=units) - a
            if not return_time:
                print(f"{func.__name__}::{duration}{units}")
                return result
            else:
                return duration

        return sync_wrapper

    if _func is None:
        return _wrapper
    else:
        return _wrapper(_func)
