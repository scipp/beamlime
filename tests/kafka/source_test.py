# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

from beamlime.kafka.message_adapter import FakeKafkaMessage
from beamlime.kafka.source import KafkaMessageSource


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


def test_get_messages_returns_results_of_consume() -> None:
    source = KafkaMessageSource(consumer=FakeKafkaConsumer())
    messages = source.get_messages()
    # The FakeKafkaConsumer returns the same messages every time
    assert messages == source.get_messages()
    assert messages == source.get_messages()


def test_limit_number_of_consumed_messages() -> None:
    source = KafkaMessageSource(consumer=FakeKafkaConsumer(), num_messages=2)
    messages1 = source.get_messages()
    assert len(messages1) == 2
    assert messages1[0].topic() == "topic1"
    assert messages1[0].value() == "abc"
    assert messages1[1].topic() == "topic2"
    assert messages1[1].value() == "def"
    messages2 = source.get_messages()
    # The FakeKafkaConsumer returns the same messages every time
    assert messages1 == messages2
