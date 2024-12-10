# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import TypeVar

from beamlime.v2.core.handler import Handler, HandlerRegistry, Message
from beamlime.v2.core.processor import StreamProcessor
from beamlime.v2.fakes import FakeConsumer, FakeProducer

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
        messages=[
            [],
            [Message(timestamp=0, topic='topic', value=111)],
            [
                Message(timestamp=0, topic='topic', value=222),
                Message(timestamp=0, topic='topic', value=333),
            ],
        ]
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
    assert producer.messages[0].value == '111'
    processor.process()
    assert len(producer.messages) == 3
    assert producer.messages[1].value == '222'
    assert producer.messages[2].value == '333'
    processor.process()
    assert len(producer.messages) == 3
