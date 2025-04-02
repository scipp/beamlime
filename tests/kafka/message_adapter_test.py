# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json

import pytest
from streaming_data_types import eventdata_ev44, logdata_f144

from beamlime.core.message import Message, MessageKey, MessageSource
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    BeamlimeConfigMessageAdapter,
    ChainedAdapter,
    Ev44ToMonitorEventsAdapter,
    F144ToLogDataAdapter,
    FakeKafkaMessage,
    KafkaMessage,
    KafkaToEv44Adapter,
    KafkaToF144Adapter,
    KafkaToMonitorEventsAdapter,
    RawConfigItem,
    RouteBySchemaAdapter,
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


def make_serialized_f144() -> bytes:
    return logdata_f144.serialise_f144(
        source_name="temperature1", value=123.45, timestamp_unix_ns=9876543210
    )


class FakeF144KafkaMessageSource(MessageSource[KafkaMessage]):
    def get_messages(self) -> list[KafkaMessage]:
        f144 = make_serialized_f144()
        return [FakeKafkaMessage(value=f144, topic="sensors")]


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


def test_KafkaToF144Adapter() -> None:
    source = AdaptingMessageSource(
        source=FakeF144KafkaMessageSource(),
        adapter=KafkaToF144Adapter(),
    )
    messages = source.get_messages()
    assert len(messages) == 1
    assert messages[0].key.topic == "sensors"
    assert messages[0].key.source_name == "temperature1"
    assert messages[0].value.value == 123.45
    assert messages[0].timestamp == 9876543210


def test_F144ToLogDataAdapter() -> None:
    source = AdaptingMessageSource(
        source=FakeF144KafkaMessageSource(),
        adapter=ChainedAdapter(
            first=KafkaToF144Adapter(), second=F144ToLogDataAdapter()
        ),
    )
    messages = source.get_messages()
    assert len(messages) == 1
    assert messages[0].key.topic == "sensors"
    assert messages[0].key.source_name == "temperature1"
    assert messages[0].value.value == 123.45
    assert messages[0].value.time == 9876543210
    assert messages[0].timestamp == 9876543210


def message_with_schema(schema: str) -> KafkaMessage:
    """
    Create a fake Kafka message with the given schema.

    The streaming_data_types library uses bytes 4:8 to store the schema.
    """
    return FakeKafkaMessage(value=f"xxxx{schema}".encode(), topic=schema)


def test_RouteBySchemaAdapter_raises_KeyError_if_no_route_found() -> None:
    adapter = RouteBySchemaAdapter(routes={})
    with pytest.raises(KeyError, match="ev44"):
        adapter.adapt(message_with_schema("ev44"))


def fake_message_with_value(message: KafkaMessage, value: str) -> Message[str]:
    return Message(
        timestamp=1234,
        key=MessageKey(topic=message.topic(), source_name="dummy"),
        value=value,
    )


def test_RouteBySchemaAdapter_calls_adapter_based_on_route() -> None:
    class Adapter:
        def __init__(self, value: str):
            self._value = value

        def adapt(self, message: KafkaMessage) -> Message[str]:
            return fake_message_with_value(message, self._value)

    adapter = RouteBySchemaAdapter(
        routes={"ev44": Adapter('adapter1'), "da00": Adapter('adapter2')}
    )
    assert adapter.adapt(message_with_schema('ev44')).value == "adapter1"
    assert adapter.adapt(message_with_schema('da00')).value == "adapter2"


def test_BeamlimeCommandsAdapter() -> None:
    key = b'my_source/my_service/my_key'
    encoded = json.dumps('my_value').encode('utf-8')
    message = FakeKafkaMessage(key=key, value=encoded, topic="dummy_beamlime_commands")
    adapter = BeamlimeConfigMessageAdapter()
    adapted_message = adapter.adapt(message)
    assert adapted_message.key.topic == 'dummy_beamlime_commands'
    # So it gets routed to config handler
    assert adapted_message.key.source_name == 'config'
    assert adapted_message.value == RawConfigItem(key=key, value=encoded)
