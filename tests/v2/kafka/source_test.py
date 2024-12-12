# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

from beamlime.v2.kafka.message_adapter import FakeKafkaMessage
from beamlime.v2.kafka.source import KafkaMessageSource


class FakeKafkaConsumer:
    def consume(self, num_messages: int, timeout: float) -> list[FakeKafkaMessage]:
        return [
            FakeKafkaMessage(value='abc', topic="topic1"),
            FakeKafkaMessage(value='def', topic="topic2"),
            FakeKafkaMessage(value='xyz', topic="topic1"),
        ][:num_messages]


def test_get_messages_returns_multiple() -> None:
    source = KafkaMessageSource(consumer=FakeKafkaConsumer())
    messages = source.get_messages()
    assert len(messages) == 3
    assert messages[0].topic() == "topic1"
    assert messages[0].value() == "abc"
    assert messages[1].topic() == "topic2"
    assert messages[1].value() == "def"
    assert messages[2].topic() == "topic1"
    assert messages[2].value() == "xyz"
