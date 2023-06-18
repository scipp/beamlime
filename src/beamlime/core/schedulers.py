# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# async-related tools

from typing import Callable, Type


class MaxTrialsReached(Exception):
    ...


def async_retry(
    *exceptions: Type, max_trials: int = 1, interval: float = 0
) -> Callable:
    """
    Retry calling an async method under expected exceptions.

    """

    def inner_decorator(func: Callable) -> Callable:
        import asyncio
        from functools import wraps

        @wraps(func)
        async def wrapper(*args, **kwargs):
            for _ in range(max_trials - 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions:
                    await asyncio.sleep(interval)

            return await func(*args, **kwargs)

        return wrapper

    return inner_decorator


def retry(*exceptions: Type, max_trials: int = 1, interval: float = 0) -> Callable:
    """
    Retry calling a method under expected exceptions.

    """

    def inner_decorator(func: Callable) -> Callable:
        import time
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(max_trials - 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    time.sleep(interval)

            return func(*args, **kwargs)

        return wrapper

    return inner_decorator
