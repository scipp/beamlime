# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Protocol

from ..core.message import MessageSource
from .message_adapter import KafkaMessage


class KafkaConsumer(Protocol):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]: ...


class MultiConsumer:
    """
    Message source for multiple Kafka consumers.

    This class allows for consuming messages from multiple Kafka consumers with
    different configuration. In particular, we need to use different topic offsets for
    data topics vs. config/command topics.
    """

    def __init__(self, consumers):
        self._consumers = consumers

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        messages = []
        for consumer in self._consumers:
            messages.extend(consumer.consume(num_messages, timeout))
        return messages


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
