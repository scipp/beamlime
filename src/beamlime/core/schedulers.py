# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# async-related tools

from typing import Callable, Type

from ..tools.wrappers import wraps_better

MINIMUM_TIMEOUT = 0
MINIMUM_AWAIT_INTERVAL = 0.1
MAXIMUM_AWAIT_INTERVAL = 1000  # TODO: Remove or update the number


def _retrieve_timeout_interval(
    async_func: Callable, *args, **kwargs
) -> tuple[float, float]:
    """
    Retrieve ``timeout`` and ``interval`` from arguments binding of ``async_func``.
    Instead of passing ``timeout`` and ``interval`` to the ``async_timeout`` decorator,
    it retrieves ``timeout`` and ``interval``
    from the signature of the decorated function call.

    It is because ``async_func`` will be called in multiple places
    and they will often need different ``timeout`` and ``wait_interval`` for each call.

    """
    from inspect import signature

    binded = signature(async_func).bind(*args, **kwargs)
    binded.apply_defaults()

    try:
        timeout = binded.arguments.get("timeout")
        wait_interval = binded.arguments.get("wait_interval")
    except KeyError:
        raise ValueError(
            "async_timeout can only decorate a function with "
            "``timeout: float`` and ``wait_interval: float`` "
            " as positional arguments or key-word arguments "
            " with numeric default values."
        )

    # Validate ``timeout`` and ``interval``.
    if timeout < MINIMUM_TIMEOUT:
        raise ValueError(
            f"``timeout`` out of [{MINIMUM_TIMEOUT}, ) for ``async_timeout``."
        )
    elif not (MINIMUM_AWAIT_INTERVAL <= wait_interval <= MAXIMUM_AWAIT_INTERVAL):
        raise ValueError(
            "``interval`` out of range "
            f"[{MINIMUM_AWAIT_INTERVAL}, {MAXIMUM_AWAIT_INTERVAL}]"
            " for ``async_timeout``."
        )

    return timeout, wait_interval


def async_timeout(exception: Type = Exception):
    """
    Timeout specified async function decorator.

    """

    def inner_decorator(func: Callable):
        import asyncio

        @wraps_better(func)
        async def wrapper(*args, **kwargs):
            timeout, interval = _retrieve_timeout_interval(func, *args, **kwargs)
            num_rounds = max(min(int(timeout / interval), timeout // interval + 1), 1)
            for _ in range(num_rounds):
                try:
                    return await func(*args, **kwargs)
                except exception:
                    await asyncio.sleep(interval)
                except Exception as err:
                    raise (err)

            raise TimeoutError(f"Call {func} failed due to timeout of {timeout}s.")

        return wrapper

    return inner_decorator
