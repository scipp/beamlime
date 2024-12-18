# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import replace
from typing import Any, Generic, Protocol, TypeVar

import scipp as sc
import streaming_data_types
import streaming_data_types.exceptions
from streaming_data_types import dataarray_da00, eventdata_ev44

from ..core.message import Message, MessageKey, MessageSource
from ..handlers.monitor_data_handler import MonitorEvents
from .scipp_da00_compat import da00_to_scipp

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class KafkaMessage(Protocol):
    """Simplified Kafka message interface for testing purposes."""

    def error(self) -> Any | None:
        pass

    def value(self) -> bytes:
        pass

    def topic(self) -> str:
        pass


def message_schema(msg: KafkaMessage) -> str | None:
    """
    Extracts the schema from a Kafka message by the streaming_data_types library.
    """
    if msg.error() is not None or len(msg.value()) < 8:
        return None
    return msg.value()[4:8].decode()


class FakeKafkaMessage(KafkaMessage):
    def __init__(self, value: bytes, topic: str):
        self._value = value
        self._topic = topic

    def error(self) -> Any | None:
        return None

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FakeKafkaMessage):
            return False
        return self._value == other._value and self._topic == other._topic


class MessageAdapter(Protocol, Generic[T, U]):
    def adapt(self, message: T) -> U:
        pass


class KafkaToEv44Adapter(
    MessageAdapter[KafkaMessage, Message[eventdata_ev44.EventData]]
):
    def adapt(self, message: KafkaMessage) -> Message[eventdata_ev44.EventData]:
        ev44 = eventdata_ev44.deserialise_ev44(message.value())
        key = MessageKey(topic=message.topic(), source_name=ev44.source_name)
        timestamp = ev44.reference_time[-1]
        return Message(timestamp=timestamp, key=key, value=ev44)


class KafkaToDa00Adapter(
    MessageAdapter[KafkaMessage, Message[list[dataarray_da00.Variable]]]
):
    def adapt(self, message: KafkaMessage) -> Message[list[dataarray_da00.Variable]]:
        da00 = dataarray_da00.deserialise_da00(message.value())
        key = MessageKey(topic=message.topic(), source_name=da00.source_name)
        timestamp = da00.timestamp_ns
        return Message(timestamp=timestamp, key=key, value=da00.data)


class Ev44ToMonitorEventsAdapter(
    MessageAdapter[Message[eventdata_ev44.EventData], Message[MonitorEvents]]
):
    def adapt(
        self, message: Message[eventdata_ev44.EventData]
    ) -> Message[MonitorEvents]:
        return replace(message, value=MonitorEvents.from_ev44(message.value))


class Da00ToScippAdapter(
    MessageAdapter[Message[list[dataarray_da00.Variable]], Message[sc.DataArray]]
):
    def adapt(
        self, message: Message[list[dataarray_da00.Variable]]
    ) -> Message[sc.DataArray]:
        return replace(message, value=da00_to_scipp(message.value))


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
        adapted = []
        for msg in raw_messages:
            try:
                adapted.append(self._adapter.adapt(msg))
            except streaming_data_types.exceptions.WrongSchemaException:  # noqa: PERF203
                pass
        return adapted