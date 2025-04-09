# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
import time
from dataclasses import replace
from typing import Any, Generic, Protocol, TypeVar

import confluent_kafka as kafka
import scipp as sc
from streaming_data_types import dataarray_da00, logdata_f144

from ..config.streams import stream_kind_to_topic
from ..core.message import Message, MessageSink
from .scipp_da00_compat import scipp_to_da00

T = TypeVar("T")


class SerializationError(Exception):
    """Raised when serialization of a message fails."""


class Serializer(Protocol, Generic[T]):
    def __call__(self, value: Message[T]) -> bytes: ...


def serialize_dataarray_to_da00(msg: Message[sc.DataArray]) -> bytes:
    try:
        da00 = dataarray_da00.serialise_da00(
            source_name=msg.stream.name,
            timestamp_ns=time.time_ns(),
            data=scipp_to_da00(msg.value),
        )
    except (ValueError, TypeError) as e:
        raise SerializationError(f"Failed to serialize message: {e}") from None
    return da00


def serialize_dataarray_to_f144(msg: Message[sc.DataArray]) -> bytes:
    try:
        da = msg.value
        f144 = logdata_f144.serialise_f144(
            source_name=msg.stream.name,
            value=da.value,
            timestamp_unix_ns=da.coords['time'].to(unit='ns', copy=False).value,
        )
    except (ValueError, TypeError) as e:
        raise SerializationError(f"Failed to serialize message: {e}") from None
    return f144


class KafkaSink(MessageSink[T]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        instrument: str,
        kafka_config: dict[str, Any],
        serializer: Serializer[T] = serialize_dataarray_to_da00,
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._producer = kafka.Producer(kafka_config)
        self._serializer = serializer
        self._instrument = instrument

    def publish_messages(self, messages: Message[T]) -> None:
        def delivery_callback(err, msg):
            if err is not None:
                self._logger.error(
                    "Failed to deliver message to %s: %s", msg.topic(), err
                )

        self._logger.debug("Publishing %d messages", len(messages))
        for msg in messages:
            try:
                topic = stream_kind_to_topic(
                    instrument=self._instrument, kind=msg.stream.kind
                )
                value = self._serializer(msg)
            except SerializationError as e:  # noqa: PERF203
                self._logger.error("Failed to serialize message: %s", e)
            else:
                try:
                    self._producer.produce(
                        topic=topic, value=value, callback=delivery_callback
                    )
                    self._producer.poll(0)
                except kafka.KafkaException as e:
                    self._logger.error("Failed to publish message to %s: %s", topic, e)

        try:
            self._producer.flush(timeout=3)
        except kafka.KafkaException as e:
            self._logger.error("Error flushing producer: %s", e)


class UnrollingSinkAdapter(MessageSink[T | sc.DataGroup[T]]):
    def __init__(self, sink: MessageSink[T]):
        self._sink = sink

    def publish_messages(self, messages: list[Message[T | sc.DataGroup[T]]]) -> None:
        unrolled: list[Message[T]] = []
        for msg in messages:
            if isinstance(msg.value, sc.DataGroup):
                for name, value in msg.value.items():
                    stream = replace(msg.stream, name=f'{msg.stream.name}/{name}')
                    unrolled.append(replace(msg, stream=stream, value=value))
            else:
                unrolled.append(msg)
        self._sink.publish_messages(unrolled)
