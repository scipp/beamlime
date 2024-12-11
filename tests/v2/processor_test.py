# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import TypeVar

from beamlime.v2.core.handler import Handler, HandlerRegistry, Message
from beamlime.v2.core.processor import StreamProcessor
from beamlime.v2.fakes import FakeMessageSink, FakeMessageSource

T = TypeVar('T')


class ValueToStringHandler(Handler[T, str]):
    def handle(self, message: Message[T]) -> list[Message[str]]:
        msg = Message(
            timestamp=message.timestamp, key='string', value=str(message.value)
        )
        return [msg]


def test_consumes_and_produces_messages() -> None:
    config = {}
    source = FakeMessageSource(
        messages=[
            [],
            [Message(timestamp=0, key='topic', value=111)],
            [
                Message(timestamp=0, key='topic', value=222),
                Message(timestamp=0, key='topic', value=333),
            ],
        ]
    )
    sink = FakeMessageSink()
    handler_registry = HandlerRegistry(config=config, handler_cls=ValueToStringHandler)
    processor = StreamProcessor(
        source=source, sink=sink, handler_registry=handler_registry
    )
    processor.process()
    assert len(sink.messages) == 0
    processor.process()
    assert len(sink.messages) == 1
    assert sink.messages[0].value == '111'
    processor.process()
    assert len(sink.messages) == 3
    assert sink.messages[1].value == '222'
    assert sink.messages[2].value == '333'
    processor.process()
    assert len(sink.messages) == 3
