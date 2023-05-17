# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from queue import Queue
from typing import Any, Callable, Iterable
from unittest.mock import MagicMock

from confluent_kafka import Message


# Administration behavior (delete topics, get partition lists, etc ...)
# should be tested with real kafka daemon and not with this dummy broker.
# It shall only be used for simple interface testings of
# ``communication.broker`` or communication handlers in ``communication.interfaces``.
class DummyKafkaBroker:
    """
    Simulates(mimics) kafka broker behaviors
    on a few confluent_kafka calls for testing.
    """

    def __init__(self, max_buffer_size: int = 0, initial_data: Iterable = ()) -> None:
        self._buffer = Queue(maxsize=max_buffer_size)
        self._queue = Queue(maxsize=max_buffer_size)
        for data in initial_data:
            self.produce("", value=data)
        self.poll(0)

    def consume(self, chunk_size: int = 1, timeout: int = 0) -> Any:
        return tuple(
            self._queue.get(timeout=timeout)
            for _ in range(min(chunk_size, self._queue.qsize()))
        )

    def produce(self, topic: str, *, key: str = "", value: Any) -> Any:
        message = MagicMock(Message)
        message.topic.return_value = topic
        message.key.return_value = key
        message.value.return_value = value

        from queue import Full

        try:
            self._buffer.put(message, timeout=0)
        except Full:
            raise BufferError

    def poll(self, timeout: float = 0) -> int:
        if self._buffer.qsize() == 0:
            return 0

        while self._buffer.qsize():
            self._queue.put(self._buffer.get(timeout=timeout), timeout=0)

        return 1


def mock_kafka(initial_data: Iterable = (), max_buffer_size: int = 12) -> Callable:
    def inner_wrapper(func: Callable) -> Callable:
        def wrapper(*newargs, **newkwargs) -> Any:
            _broker = DummyKafkaBroker(
                initial_data=initial_data, max_buffer_size=max_buffer_size
            )
            mocked_kafka_objs = [
                arg
                for arg in (*newargs, *newkwargs.values())
                if ("Producer" in (desc := str(arg)) or "Consumer" in desc)
            ]
            if len(mocked_kafka_objs) == 0:
                raise ValueError(
                    "Producer or Consumer mock not found in the arguments. "
                    "Remove decorator if you are not using "
                    "Producer or Consumer mocks. "
                    "Otherwise, check if ``mock_kafka`` is the first decorator."
                )

            for arg in (*newargs, *newkwargs.values()):
                if "Producer" in (desc := str(arg)):
                    arg.produce = _broker.produce
                    arg.poll = _broker.poll
                elif "Consumer" in desc:
                    arg.consume = _broker.consume
            return func(*newargs, **newkwargs)

        return wrapper

    return inner_wrapper
