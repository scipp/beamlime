# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from queue import Queue
from typing import Iterable, Optional, overload

from ..core.protocols import BeamlimeApplicationProtocol


def validate_queue_connection(
    sender: BeamlimeApplicationProtocol,
    receivers: list[BeamlimeApplicationProtocol],
    raise_error: bool = True,
) -> Optional[bool]:
    for receiver in receivers:
        if receiver.input_channel is not sender.output_channel:
            if raise_error:
                raise RuntimeError(
                    "Application communication pipe broken. "
                    "Please check the application mapping."
                )
            else:
                return False
    return True


def glue(
    sender: BeamlimeApplicationProtocol,
    receivers: Iterable[BeamlimeApplicationProtocol],
) -> None:
    # TODO: Move this to QueueHandler method.
    # Always create and assign output-channel-queue of sender before
    # input-channel-queue of receiver.
    if sender.output_channel is None:
        new_queue = Queue(maxsize=100)
        sender.output_channel = new_queue
    for receiver in receivers:
        if receiver.input_channel is None:
            receiver.input_channel = sender.output_channel

    validate_queue_connection(sender, receivers)


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
