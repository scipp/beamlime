# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Generic, Protocol, TypeVar

from ..core.message import MessageSource
from .message_adapter import KafkaMessage

T = TypeVar("T")


class KafkaConsumer(Protocol, Generic[T]):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage[T]]:
        pass


class KafkaMessageSource(MessageSource[KafkaMessage[T]]):
    def __init__(
        self, consumer: KafkaConsumer[T], num_messages: int = 100, timeout: float = 0.05
    ):
        self._consumer = consumer
        self._num_messages = num_messages
        self._timeout = timeout

    def get_messages(self) -> list[KafkaMessage[T]]:
        return self._consumer.consume(
            num_messages=self._num_messages, timeout=self._timeout
        )
