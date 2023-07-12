# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# async-related tools

from typing import Any, Awaitable, Callable, Type, TypeVar


class MaxTrialsReached(Exception):
    ...


T = TypeVar("T")
WrappedAsyncCallable = Callable[..., Awaitable[T]]


def async_retry(
    *exceptions: Type[Exception], max_trials: int = 1, interval: float = 0
) -> Callable[..., WrappedAsyncCallable[T]]:
    """
    Retry calling an async method under expected exceptions.

    """

    def inner_decorator(func: WrappedAsyncCallable[T]) -> WrappedAsyncCallable[T]:
        import asyncio
        from functools import wraps

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            for _ in range(max_trials - 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions:
                    await asyncio.sleep(interval)

            return await func(*args, **kwargs)

        return wrapper

    return inner_decorator


WrappedCallable = Callable[..., T]


def retry(
    *exceptions: Type[Exception], max_trials: int = 1, interval: float = 0
) -> Callable[..., WrappedCallable[T]]:
    """
    Retry calling a method under expected exceptions.

    """

    def inner_decorator(func: WrappedCallable[T]) -> WrappedCallable[T]:
        import time
        from functools import wraps

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            for _ in range(max_trials - 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    time.sleep(interval)

            return func(*args, **kwargs)

        return wrapper

    return inner_decorator
