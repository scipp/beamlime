# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pytest
from streaming_data_types import eventdata_ev44

from beamlime.core.message import Message, MessageKey, MessageSource
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Ev44ToMonitorEventsAdapter,
    FakeKafkaMessage,
    KafkaMessage,
    KafkaToEv44Adapter,
    KafkaToMonitorEventsAdapter,
    RoutingAdapter,
)


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
    assert messages[0].topic() == "monitors"
    assert messages[0].value() == make_serialized_ev44()


def test_adapting_source() -> None:
    source = AdaptingMessageSource(
        source=FakeKafkaMessageSource(),
        adapter=ChainedAdapter(
            first=KafkaToEv44Adapter(), second=Ev44ToMonitorEventsAdapter()
        ),
    )
    messages = source.get_messages()
    assert len(messages) == 1
    assert messages[0].key.topic == "monitors"
    assert messages[0].key.source_name == "monitor1"
    assert messages[0].value.time_of_arrival == [123456]
    assert messages[0].timestamp == 1234


def test_KafkaToMonitorEventsAdapter() -> None:
    source = AdaptingMessageSource(
        source=FakeKafkaMessageSource(),
        adapter=KafkaToMonitorEventsAdapter(),
    )
    messages = source.get_messages()
    assert len(messages) == 1
    assert messages[0].key.topic == "monitors"
    assert messages[0].key.source_name == "monitor1"
    assert messages[0].value.time_of_arrival == [123456]
    assert messages[0].timestamp == 1234


def message_with_schema(schema: str) -> KafkaMessage:
    """
    Create a fake Kafka message with the given schema.

    The streaming_data_types library uses bytes 4:8 to store the schema.
    """
    return FakeKafkaMessage(value=f"xxxx{schema}".encode(), topic=schema)


def test_routing_adapter_raises_KeyError_if_no_route_found() -> None:
    adapter = RoutingAdapter(routes={})
    with pytest.raises(KeyError, match="ev44"):
        adapter.adapt(message_with_schema("ev44"))


def fake_message_with_value(message: KafkaMessage, value: str) -> Message[str]:
    return Message(
        timestamp=1234,
        key=MessageKey(topic=message.topic(), source_name="dummy"),
        value=value,
    )


def test_routing_adapter_calls_adapter_based_on_route() -> None:
    class Adapter:
        def __init__(self, value: str):
            self._value = value

        def adapt(self, message: KafkaMessage) -> Message[str]:
            return fake_message_with_value(message, self._value)

    adapter = RoutingAdapter(
        routes={"ev44": Adapter('adapter1'), "da00": Adapter('adapter2')}
    )
    assert adapter.adapt(message_with_schema('ev44')).value == "adapter1"
    assert adapter.adapt(message_with_schema('da00')).value == "adapter2"
