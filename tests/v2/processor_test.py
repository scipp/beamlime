# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import TypeVar

from beamlime.v2.core.handler import Handler, HandlerRegistry, Message
from beamlime.v2.core.processor import StreamProcessor


class FakeConsumer:
    def __init__(self, messages: list[list[Message[int]]]) -> None:
        self._messages = messages
        self._index = 0

    def get_messages(self) -> list[Message[int]]:
        messages = (
            self._messages[self._index] if self._index < len(self._messages) else []
        )
        self._index += 1
        return messages


class FakeProducer:
    def __init__(self) -> None:
        self.messages = []

    def publish_messages(self, messages: dict[str, Message[str]]) -> None:
        self.messages.extend(messages)


T = TypeVar('T')


class ValueToStringHandler(Handler[T, str]):
    def handle(self, message: Message[T]) -> list[Message[str]]:
        msg = Message(
            timestamp=message.timestamp, topic='string', value=str(message.value)
        )
        return [msg]


def test_consumes_and_produces_messages() -> None:
    config = {}
    consumer = FakeConsumer(
        messages=[[], [Message(timestamp=0, topic='topic', value=42)]]
    )
    producer = FakeProducer()
    handler_registry = HandlerRegistry(config=config, handler_cls=ValueToStringHandler)
    processor = StreamProcessor(
        consumer=consumer, producer=producer, handler_registry=handler_registry
    )
    processor.process()
    assert len(producer.messages) == 0
    processor.process()
    assert len(producer.messages) == 1
    assert producer.messages[0].value == '42'
    processor.process()
    assert len(producer.messages) == 1
