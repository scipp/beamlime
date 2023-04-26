# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from queue import Empty
from typing import Any

from ..core.schedulers import async_timeout


class QueueMixin:
    def check_space(self) -> bool:
        # TODO: Check how much space ``self._queue`` is consumming.
        ...


class SingleProcessInQueue:
    def __init__(self) -> None:
        from queue import Queue

        self._queue = Queue()

    async def receive_data(self, channel: str, *args, **kwargs) -> Any:
        @async_timeout(Empty)
        async def _receive_data(timeout: int, wait_interval: int, *args, **kwargs):
            # TODO: Move async_timeout(exception=Empty) to communication handler
            # and remove the decorator or use @async_timeout(exception=TimeoutError).
            return self._queue.get(*args, timeout=wait_interval, **kwargs)

        try:
            # TODO: Replace the below line to self.broker.receive_data(...)
            return await _receive_data(
                *args, timeout=self.timeout, wait_interval=self.wait_interval, **kwargs
            )
        except TimeoutError:
            return None


class DownStreamMixin:
    """
    Down-stream communication interfaces

    Protocol
    --------
    DownstreamCommunicationProtocol

    """

    async def get(self, *args, **kwargs) -> Any:
        @async_timeout(Empty)
        async def _get(timeout: int, wait_interval: int, *args, **kwargs):
            # TODO: Move async_timeout(exception=Empty) to communication handler
            # and remove the decorator or use @async_timeout(exception=TimeoutError).
            return self._in_queue.get(*args, timeout=wait_interval, **kwargs)

        try:
            # TODO: Replace the below line to self.broker.receive_data(...)
            return await _get(
                *args, timeout=self.timeout, wait_interval=self.wait_interval, **kwargs
            )
        except TimeoutError:
            return None

    async def put(self, data, *args, **kwargs) -> None:
        @async_timeout(Empty)
        async def _put(timeout: int, wait_interval: int, *args, **kwargs):
            # TODO: Move async_timeout(exception=Empty) to communication handler
            # and remove the decorator or use @async_timeout(exception=TimeoutError).
            self._out_queue.put(data, *args, timeout=wait_interval, **kwargs)
            return True

        try:
            # TODO: Replace the below line to self.broker.send_data(...)
            return await _put(
                *args, timeout=self.timeout, wait_interval=self.wait_interval, **kwargs
            )
        except TimeoutError:
            return False
