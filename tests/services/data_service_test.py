# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time

from beamlime import CommonHandlerFactory, Handler, Message, MessageKey
from beamlime.fakes import FakeMessageSink
from beamlime.kafka.message_adapter import (
    FakeKafkaMessage,
    KafkaMessage,
    MessageAdapter,
)
from beamlime.kafka.source import KafkaConsumer
from beamlime.service_factory import DataServiceBuilder


def fake_message_with_value(message: KafkaMessage, value: str) -> Message[str]:
    return Message(
        timestamp=1234,
        key=MessageKey(topic=message.topic(), source_name="dummy"),
        value=value,
    )


class ForwardingAdapter(MessageAdapter[KafkaMessage, Message[int]]):
    def adapt(self, message: KafkaMessage) -> Message[int]:
        return fake_message_with_value(message, value=int(message.value().decode()))


class ForwardingHandler(Handler[int, int]):
    def handle(self, messages: list[Message[int]]) -> list[Message[int]]:
        return messages


class EmptyConsumer(KafkaConsumer):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        return []

    def close(self) -> None:
        pass


class IntConsumer(KafkaConsumer):
    def __init__(self) -> None:
        self._values = [11, 22, 33, 44]
        self._index = 0

    @property
    def at_end(self) -> bool:
        return self._index >= len(self._values)

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        if self.at_end:
            return []
        message = FakeKafkaMessage(
            value=str(self._values[self._index]).encode(), topic="dummy"
        )
        self._index += 1
        return [message]

    def close(self) -> None:
        pass


def test_basics() -> None:
    builder = DataServiceBuilder(
        instrument='instrument',
        name='name',
        adapter=ForwardingAdapter(),
        handler_factory_cls=CommonHandlerFactory.from_handler(ForwardingHandler),
    )
    sink = FakeMessageSink()
    consumer = IntConsumer()
    service = builder.build(
        control_consumer=EmptyConsumer(), consumer=consumer, sink=sink
    )
    service.start(blocking=False)
    while not consumer.at_end:
        time.sleep(0.1)
    service.stop()
    assert len(sink.messages) == 4
    values = [msg.value for msg in sink.messages]
    assert values == [11, 22, 33, 44]
