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
from typing import Any, Callable, overload

from confluent_kafka import Message


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

    @overload
    def __init__(self, config: dict) -> None:
        ...  # pragma: no cover

    @overload
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: tuple[str],
        auto_offset_reset: str,
        **kwargs,
    ) -> None:
        ...  # pragma: no cover

    def __init__(
        self,
        /,
        config: dict = None,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "beamlime",
        topics: tuple[str] = None,
        auto_offset_reset: str = "smallest",
        **kwargs,
    ) -> None:
        from confluent_kafka import Consumer

        if config is None:
            kafka_kwargs = {
                key.replace("_", "."): value for key, value in kwargs.items()
            }
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                **kafka_kwargs,
            }
        else:
            from copy import copy

            conf = copy(config)
        self._consumer = Consumer(conf)
        self._consumer.subscribe(topics)

    def __del__(self) -> None:
        self._consumer.close()

    async def consume(self, *args, chunk_size: int = 1, **kwargs) -> tuple[Any]:
        """Retrieves ``chunk_size`` of messages from the kafka broker."""
        messages: list[Message] = self._consumer.consume(chunk_size, 0)
        if len(messages) == 0:
            raise Empty
        else:
            # TODO: We might want the ``Message`` object instead of just values.
            return tuple(msg.value() for msg in messages)


class KafkaProducer:
    """Kafka producer interfaces wrapper."""

    @overload
    def __init__(self, config: dict) -> None:
        ...  # pragma: no cover

    @overload
    def __init__(self, bootstrap_servers: str, client_id: str, **kwargs) -> None:
        ...  # pragma: no cover

    def __init__(
        self,
        /,
        config: dict = None,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = None,
        **kwargs,
    ) -> None:
        from confluent_kafka import Producer

        if config is None:
            import socket

            kafka_kwargs = {
                key.replace("_", "."): value for key, value in kwargs.items()
            }

            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id or socket.gethostname(),
                **kafka_kwargs,
            }
        else:
            from copy import copy

            conf = copy(config)
        self._producer = Producer(conf)

    async def produce(
        self, topic: str, *args, key: str = "", value: Any, **kwargs
    ) -> None:  # TODO: we might want to move ``topic`` to configuration.
        """Produce 1 data(``key``, ``value``) under the ``topic``."""
        try:
            self._producer.produce(topic, key=key, value=value, **kwargs)
        except BufferError:
            raise Full
        result = self._producer.poll(0)
        # TODO: poll is directly called after each produce call.
        # We may want to separate poll or flush
        # if we need to use this wrapper for faster testing.
        if result != 1:
            raise Full
