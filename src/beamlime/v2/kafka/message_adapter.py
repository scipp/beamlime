# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Generic, Protocol, TypeVar

from streaming_data_types import eventdata_ev44

from ..core.message import Message, MessageKey, MessageSource
from ..handlers.monitor_data_handler import MonitorEvents

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class KafkaMessage(Protocol):
    """Simplified Kafka message interface for testing purposes."""

    def value(self) -> bytes:
        pass

    def topic(self) -> str:
        pass


class FakeKafkaMessage:
    def __init__(self, value: bytes, topic: str):
        self._value = value
        self._topic = topic

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic


class MessageAdapter(Protocol, Generic[T, U]):
    def adapt(self, message: T) -> U:
        pass


class KafkaToEv44Adapter(
    MessageAdapter[KafkaMessage, Message[eventdata_ev44.EventData]]
):
    def adapt(self, message: KafkaMessage) -> Message[eventdata_ev44.EventData]:
        ev44 = eventdata_ev44.deserialise_ev44(message.value())
        key = MessageKey(topic=message.topic(), source_name=ev44.source_name)
        timestamp = ev44.reference_time[0]
        return Message(timestamp=timestamp, key=key, value=ev44)


class Ev44ToMonitorEventsAdapter(
    MessageAdapter[Message[eventdata_ev44.EventData], Message[MonitorEvents]]
):
    def adapt(
        self, message: Message[eventdata_ev44.EventData]
    ) -> Message[MonitorEvents]:
        return Message(
            timestamp=message.timestamp,
            key=message.key,
            value=MonitorEvents.from_ev44(message.value),
        )


class ChainedAdapter(MessageAdapter[T, V]):
    def __init__(self, first: MessageAdapter[T, U], second: MessageAdapter[U, V]):
        self._first = first
        self._second = second

    def adapt(self, message: T) -> V:
        intermediate = self._first.adapt(message)
        return self._second.adapt(intermediate)


class AdaptingMessageSource(MessageSource[U]):
    def __init__(self, source: MessageSource[T], adapter: MessageAdapter[T, U]):
        self._source = source
        self._adapter = adapter

    def get_messages(self) -> list[U]:
        raw_messages = self._source.get_messages()
        return [self._adapter.adapt(msg) for msg in raw_messages]
