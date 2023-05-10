# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from multiprocessing import Queue as MQueue
from queue import Empty, Full
from queue import Queue as SQueue
from typing import Any, overload

from confluent_kafka import KafkaException
from scipp import DataArray, DataGroup, Variable
from scipp.serialization import deserialize, serialize

from ..core.schedulers import async_timeout


class BullettinBoard:
    """Bullettin Board that can be shared by all application instances."""

    def __init__(self) -> None:
        self._board = dict()

    def clear(self) -> None:
        self._board.clear()

    @async_timeout(Empty)
    async def read(self, timeout: float, wait_interval: float) -> Any:
        if not self._board:
            raise Empty
        return self._board

    @async_timeout(Full)
    async def post(self, data: dict, timeout: float, wait_interval: float) -> None:
        if len(self._board) > 10 and any(
            [key not in self._board for key in data.keys()]
        ):
            # TODO: Update or remove this statement
            # after making sure the BulletinBoard is only used for short messages.
            raise Full
        self._board.update(data)


class SingleProcessQueue(SQueue):
    @async_timeout(Empty)
    async def get(self, *args, timeout: float, wait_interval: float, **kwargs) -> Any:
        return super().get(*args, **kwargs)

    @async_timeout(Full)
    async def put(
        self, data: Any, *args, timeout: float, wait_interval: float, **kwargs
    ) -> None:
        super().put(data, *args, **kwargs)


class MultiProcessQueue(MQueue):
    @async_timeout(Empty)
    async def get(self, *args, timeout: float, wait_interval: float, **kwargs) -> Any:
        raw_data = super().get(*args, **kwargs)
        if isinstance(raw_data, dict) and raw_data.get("SCIPP"):
            try:
                return deserialize(header=raw_data["header"], frames=["frames"])
            except (KeyError, IndexError, TypeError, OSError):
                ...
        else:
            return raw_data

    @async_timeout(Full)
    async def put(
        self, data: Any, *args, timeout: float, wait_interval: float, **kwargs
    ) -> None:
        if isinstance(data, (Variable, DataArray, DataGroup)):
            header, frames = serialize(data)
            super().put(
                {"SCIPP": True, "header": header, "frames": frames}, *args, **kwargs
            )
        else:
            super().put(data, *args, **kwargs)


class KafkaConsumer:
    @overload
    def __init__(self, config: dict) -> None:
        ...

    @overload
    def __init__(
        self, bootstrap_servers: str, group_id: str, auto_offset_reset: str, **kwargs
    ) -> None:
        ...

    def __init__(
        self,
        /,
        config: dict = None,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "beamlime",
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

    @async_timeout(Empty)
    async def consume(
        self, *args, timeout: float, wait_interval: float, chunk_size: int = 1, **kwargs
    ) -> Any:
        messages = await self._consumer.consume(
            chunk_size, 0
        )  # ``async_timeout`` will handle the timeout.
        if len(messages) == 0:
            raise Empty


class KafkaProducer:
    @overload
    def __init__(self, config: dict) -> None:
        ...

    @overload
    def __init__(self, bootstrap_servers: str, client_id: str, **kwargs) -> None:
        ...

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
            kafka_kwargs = {
                key.replace("_", "."): value for key, value in kwargs.items()
            }
            import socket

            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id or socket.gethostname(),
                **kafka_kwargs,
            }
        else:
            from copy import copy

            conf = copy(config)
        self._producer = Producer(conf)

    @async_timeout(KafkaException)
    async def produce(
        self, topic, *args, timeout: float, wait_interval: float, key, value, **kwargs
    ) -> Any:
        self._producer.produce(topic, key=key, value=value)
