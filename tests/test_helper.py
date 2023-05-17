# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from queue import Queue
from typing import Any, Iterable, Union, overload

from beamlime.applications.interfaces import BeamlimeApplicationInterface


class DummyApp(BeamlimeApplicationInterface):
    def __del__(self):
        ...

    async def _run(self) -> None:
        ...


@overload
def stub_consume(testing_queue: Queue) -> Any:
    """
    Returns a stubbed consume call.
    Returned call retrieves ``Message`` mock objects from the testing queue.
    """
    ...


@overload
def stub_consume(data: Iterable) -> Any:
    """
    Returns a stubbed consume call.
    Returned call creates ``Message`` mock object with values of ``data``
    and returns a tuple of them.
    """
    ...


def stub_consume(data_container: Union[Queue, Iterable] = None) -> Any:
    from functools import partial
    from unittest.mock import MagicMock

    if isinstance(data_container, Queue):
        # Queue implies message was created by ``stub_produce``.
        data_len = partial(data_container.qsize)
        data_grab = partial(data_container.get, timeout=0)
    elif isinstance(data_container, Iterable):
        from confluent_kafka import Message

        data_len = partial(len, data_container)

        def data_grab() -> Any:
            message = MagicMock(Message)
            message.value.return_value = data_container.pop(0)
            return message

    else:
        return stub_consume([data_container])

    return lambda chunk_size, _: tuple(
        data_grab() for _ in range(min(chunk_size, data_len()))
    )


def stub_produce(testing_queue: Queue) -> Any:
    from unittest.mock import MagicMock

    from confluent_kafka import Message

    async def _put(data: Any) -> Any:
        if not isinstance(data, Message):
            message = MagicMock(Message)
            message.value.return_value = data

        testing_queue.put(message, timeout=0)

    return _put
