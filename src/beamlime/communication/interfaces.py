# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Wrappers of communication interfaces and data structure for communication,
# to have consistent read/write interfaces and error raise behaviour.
# - BullettinBoard: dict
# - SQueue: queue.Queue
# - MQueue: multiprocessing.queues.Queue
# - KafkaConsumer: confluent_kafka.Consumer
# - KafkaProducer: confluent_kafka.Producer
#
# These interfaces are only used by ``beamlime.communication.broker``.
# Since ``broker`` handles timeout(maximum retrials),
# all reading/writing methods should have timeout of 0, so that it does not block.
# Reading methods should raise ``Empty`` in case there is no data to read.
# Writing methods should raise ``Full`` in case there is no space to write.


from multiprocessing import Queue as MQueue
from multiprocessing.context import BaseContext
from queue import Empty, Full
from queue import Queue as SQueue
from typing import Any, Callable


class BullettinBoard:
    """Bullettin Board that can be shared by all application instances."""

    def __init__(self, maxsize: int = 10) -> None:
        self.maxsize = maxsize
        self._board = dict()

    def clear(self) -> None:
        self._board.clear()

    async def read(self) -> Any:
        from copy import copy

        if not self._board:
            raise Empty
        return copy(self._board)

    async def post(self, data: dict) -> None:
        if len(self._board) >= self.maxsize and any(
            [key not in self._board for key in data.keys()]
        ):
            # TODO: Update or remove this statement
            # after making sure the BulletinBoard is only used for short messages.
            raise Full
        self._board.update(data)


class SingleProcessQueue(SQueue):
    """Single process queue."""

    async def get(self, *args, **kwargs) -> Any:
        return super().get(*args, timeout=0, **kwargs)

    async def put(self, data: Any, *args, **kwargs) -> None:
        super().put(data, *args, timeout=0, **kwargs)


class MultiProcessQueue:
    """Multi process queue with serialize/deserialize options."""

    def __init__(self, maxsize: int = 0, *, ctx: BaseContext = None) -> None:
        if ctx is None:
            from multiprocessing import Manager

            self._queue = Manager().Queue(maxsize=maxsize)
        else:
            self._queue = MQueue(maxsize=maxsize, ctx=ctx)

    async def get(self, *args, deserializer: Callable = None, **kwargs) -> Any:
        raw_data = self._queue.get(*args, timeout=0, **kwargs)
        if deserializer is None:
            return raw_data
        else:
            return deserializer(raw_data)

    async def put(
        self, data: Any, *args, serializer: Callable = None, **kwargs
    ) -> None:
        if serializer is None:
            self._queue.put(data, *args, timeout=0, **kwargs)
        else:
            self._queue.put(serializer(data), *args, timeout=0, **kwargs)


class KafkaConsumer:
    """Kafka consumer interface wrapper."""

    _minimum_default_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "beamlime",
    }

    def __init__(
        self,
        /,
        options: dict,
        topic: str,
    ) -> None:
        from confluent_kafka import Consumer

        self._consumer = Consumer(options)
        self._consumer.subscribe([topic])

    def __del__(self) -> None:
        self._consumer.close()

    async def consume(self, *args, chunk_size: int = 1, **kwargs) -> tuple[Any]:
        """Retrieves ``chunk_size`` of messages from the kafka broker."""
        messages = self._consumer.consume(chunk_size, 0)
        if len(messages) == 0:
            raise Empty
        else:
            # TODO: We might want the ``Message`` object instead of just values.
            return tuple(msg.value() for msg in messages)


class KafkaProducer:
    """Kafka producer interfaces wrapper."""

    def __init__(
        self,
        /,
        options: dict,
        topic: str,
    ) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self._producer = Producer(options)

    async def produce(self, *args, key: str = "", value: Any, **kwargs) -> None:
        """Produce 1 data(``key``, ``value``) under the ``self.topic``."""
        try:
            _topic = kwargs.get("topic", self.topic)
            self._producer.produce(_topic, key=key, value=value, **kwargs)
        except BufferError:
            raise Full
        result = self._producer.poll(0)
        # TODO: poll is directly called after each produce call.
        # We may want to separate poll or flush
        # if we need to use this wrapper for faster testing.
        if result != 1:
            raise Full
