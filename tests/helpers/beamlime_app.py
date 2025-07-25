# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Helper class for testing Beamlime services.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np
from streaming_data_types import eventdata_ev44

from beamlime import Service, StreamKind
from beamlime.config import models
from beamlime.config.instruments import get_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.fakes import FakeMessageSink
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.sink import UnrollingSinkAdapter
from beamlime.kafka.source import KafkaConsumer
from beamlime.service_factory import DataServiceBuilder


class FakeConsumer(KafkaConsumer):
    """Fake consumer that can be filled with custom messages for consumption."""

    def __init__(self) -> None:
        self._messages: list[KafkaMessage] = []

    def add_message(self, message: KafkaMessage) -> None:
        self._messages.append(message)

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        _ = timeout  # Ignore timeout for the fake consumer
        result = self._messages[:num_messages]
        self._messages = self._messages[num_messages:]
        return result


@dataclass(kw_only=True)
class BeamlimeApp:
    """A testable "application" with a fake consumer and sink."""

    service: Service
    consumer: FakeConsumer
    sink: FakeMessageSink
    instrument: str

    def __post_init__(self) -> None:
        self.detector_topic = stream_kind_to_topic(
            instrument=self.instrument, kind=StreamKind.DETECTOR_EVENTS
        )
        self.monitor_topic = stream_kind_to_topic(
            instrument=self.instrument, kind=StreamKind.MONITOR_EVENTS
        )
        self._detector_config = get_config(self.instrument).detectors_config['fakes']
        self._rng = np.random.default_rng(seed=1234)  # Avoid test flakiness
        self._detector_events: bytes | None = None

    @staticmethod
    def from_service_builder(builder: DataServiceBuilder) -> BeamlimeApp:
        """
        Create a BeamlimeApp from a service builder.

        The app is created with a fake sink that allows to inspect the messages sent to
        the sink. The consumer is a fake consumer that can be filled with custom
        messages for consumption.
        """
        sink = FakeMessageSink()
        consumer = FakeConsumer()
        service = builder.from_consumer(
            consumer=consumer,
            sink=UnrollingSinkAdapter(sink),
            raise_on_adapter_error=False,
        )
        return BeamlimeApp(
            service=service, consumer=consumer, sink=sink, instrument=builder.instrument
        )

    def step(self) -> None:
        """Run one step of the service."""
        self.service.step()

    def publish_config_message(self, key: models.ConfigKey, value: Any) -> None:
        message = FakeKafkaMessage(
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            topic=stream_kind_to_topic(
                instrument=self.instrument, kind=StreamKind.BEAMLIME_CONFIG
            ),
            timestamp=0,
        )
        self.consumer.add_message(message)

    def publish_monitor_events(self, *, size: int, time: int) -> None:
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_ev44(name='monitor1', size=size, with_ids=False),
            topic=self.monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_ev44(name='monitor2', size=size, with_ids=False),
            topic=self.monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)

    def publish_events(
        self, *, size: int, time: int, reuse_events: bool = False
    ) -> None:
        """
        Publish events to the consumer.

        If `reuse_events` is True, the same events are reused for each call to this
        method. This is useful for speeding up tests that need to send many event
        messages but do not require different events for each call.
        """
        if not reuse_events or self._detector_events is None:
            events = self.make_serialized_ev44(
                name=next(iter(self._detector_config)), size=size, with_ids=True
            )
            self._detector_events = events
        else:
            events = self._detector_events
        message = FakeKafkaMessage(
            value=events,
            topic=self.detector_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(message)

    def publish_data(self, *, topic: str, time: int, data: bytes) -> None:
        """Publish data to the consumer."""
        message = FakeKafkaMessage(
            value=data,
            topic=topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(message)

    def make_serialized_ev44(self, name: str, size: int, with_ids: bool) -> bytes:
        time_of_arrival = self._rng.uniform(0, 70_000_000, size).astype(np.int32)
        if with_ids:
            first, last = self._detector_config[name]
            pixel_id = self._rng.integers(first, last + 1, size, dtype=np.int32)
        else:
            pixel_id = np.zeros(size, dtype=np.int32)
        # Empty reference_time. KafkaToEv44Adapter falls back to message.timestamp().
        return eventdata_ev44.serialise_ev44(
            source_name=name,
            message_id=0,
            reference_time=[],
            reference_time_index=0,
            time_of_flight=time_of_arrival,
            pixel_id=pixel_id,
        )
