# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Protocol

from .message_adapter import KafkaMessage


class KafkaConsumer(Protocol):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        pass


class KafkaMessageSource:
    def __init__(self, consumer: KafkaConsumer):
        self._consumer = consumer

    def get_messages(self) -> list[KafkaMessage]:
        return self._consumer.consume(num_messages=100, timeout=0.05)
