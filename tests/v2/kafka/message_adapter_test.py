# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass

from streaming_data_types import eventdata_ev44

from beamlime.v2.core.message import MessageSource
from beamlime.v2.kafka.message_adapter import KafkaMessage


@dataclass
class FakeKafkaMessage:
    value: bytes
    topic: str


def make_serialized_ev44() -> bytes:
    return eventdata_ev44.serialise_ev44(
        source_name="monitor1",
        message_id=0,
        reference_time=[1234],
        reference_time_index=0,
        time_of_flight=[123456],
        pixel_id=[1],
    )


class FakeKafkaMessageSource(MessageSource[KafkaMessage]):
    def get_messages(self) -> list[KafkaMessage]:
        ev44 = make_serialized_ev44()
        return [FakeKafkaMessage(value=ev44, topic="monitors")]


def test_fake_kafka_message_source() -> None:
    source = FakeKafkaMessageSource()
    messages = source.get_messages()
    assert len(messages) == 1
    assert messages[0].topic == "monitors"
    assert messages[0].value == make_serialized_ev44()
