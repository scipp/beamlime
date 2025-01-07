# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Protocol

from ..core.message import MessageSource
from .message_adapter import KafkaMessage


class KafkaConsumer(Protocol):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        pass

    def close(self) -> None:
        pass


class KafkaMessageSource(MessageSource[KafkaMessage]):
    """
    Message source for messages from Kafka.

    Parameters
    ----------
    consumer:
        Kafka consumer instance.
    num_messages:
        Number of messages to consume and return in a single call to `get_messages`.
        Fewer messages may be returned if the timeout is reached.
    timeout:
        Timeout in seconds to wait for messages before returning.
    """

    def __init__(
        self, consumer: KafkaConsumer, num_messages: int = 100, timeout: float = 0.05
    ):
        self._consumer = consumer
        self._num_messages = num_messages
        self._timeout = timeout

    def get_messages(self) -> list[KafkaMessage]:
        return self._consumer.consume(
            num_messages=self._num_messages, timeout=self._timeout
        )

    def close(self) -> None:
        self._consumer.close()
