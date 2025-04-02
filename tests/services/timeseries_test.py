# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time
from typing import Any

import numpy as np
import pytest
import scipp as sc
from streaming_data_types import logdata_f144

from beamlime.config.raw_detectors import available_instruments
from beamlime.config.topics import motion_topic
from beamlime.fakes import FakeMessageSink
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.sink import UnrollingSinkAdapter
from beamlime.kafka.source import KafkaConsumer
from beamlime.services.timeseries import make_timeseries_service_builder


class F144Consumer(KafkaConsumer):
    def __init__(
        self,
        instrument: str = 'dummy',
        max_messages: int = 100,
        source_names: list[str] | None = None,
    ) -> None:
        self._instrument = instrument
        self._max_messages = max_messages
        self._source_names = source_names or [
            'detector_rotation',
            'temperature',
            'pressure',
        ]
        self._topic = motion_topic(instrument=instrument)

        self._reference_time = int(sc.epoch(unit='ns').value)
        self._running = False
        self._count = 0
        self._rng = np.random.default_rng(seed=1234)  # Avoid test flakiness
        self._current_source_index = 0

    def start(self) -> None:
        self._running = True

    def reset(self) -> None:
        self._running = False
        self._count = 0

    @property
    def at_end(self) -> bool:
        return self._count >= self._max_messages

    def _make_timestamp(self) -> int:
        """Create an incremental timestamp."""
        self._reference_time += 1_000_000_000  # 1 second in ns
        return self._reference_time

    def _make_f144_message(self, source_name: str) -> bytes:
        """Create a serialized f144 message with random data."""
        timestamp = self._make_timestamp()
        value = self._rng.uniform(0, 100)

        # For testing purposes, each source has a different value pattern
        if 'rotation' in source_name:
            value = float(self._count % 90)  # 0-89 degrees
        elif 'temperature' in source_name:
            value = 20.0 + float(self._count % 10)  # 20-29 degrees C
        elif 'pressure' in source_name:
            value = 100.0 + float(self._count % 5)  # 100-104 kPa

        return logdata_f144.serialise_f144(
            source_name=source_name, timestamp_unix_ns=timestamp, value=value
        )

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        if not self._running or self.at_end:
            return []

        messages_to_produce = min(3, self._max_messages - self._count)
        if messages_to_produce <= 0:
            return []

        messages = []
        for _ in range(messages_to_produce):
            # Rotate through the source names
            source_name = self._source_names[
                self._current_source_index % len(self._source_names)
            ]
            self._current_source_index += 1

            messages.append(
                FakeKafkaMessage(
                    value=self._make_f144_message(source_name),
                    topic=self._topic,
                    timestamp=self._make_timestamp(),
                )
            )
            self._count += 1

        return messages

    def close(self) -> None:
        pass


def start_and_wait_for_completion(consumer: F144Consumer) -> None:
    """Start the consumer and wait until it has finished producing messages."""
    consumer.start()
    while not consumer.at_end:
        time.sleep(0.01)
    consumer.reset()


class FakeAttributeRegistry(dict[str, dict[str, Any]]):
    """Fake attribute registry for testing."""

    def __init__(self):
        super().__init__()
        # Add attributes for common source names
        self["detector_rotation"] = {
            'time': {'start': '1970-01-01T00:00:00.000000', 'units': 'ns'},
            'value': {'units': 'deg'},
        }
        self["temperature"] = {
            'time': {'start': '1970-01-01T00:00:00.000000', 'units': 'ns'},
            'value': {'units': 'C'},
        }
        self["pressure"] = {
            'time': {'start': '1970-01-01T00:00:00.000000', 'units': 'ns'},
            'value': {'units': 'kPa'},
        }

    def __getitem__(self, item):
        if item in self:
            return super().__getitem__(item)
        # Fallback for unknown items
        return {
            'time': {'start': '1970-01-01T00:00:00.000000', 'units': 'ns'},
            'value': {'units': ''},
        }


@pytest.mark.parametrize('instrument', available_instruments())
def test_timeseries_service(instrument: str) -> None:
    attribute_registry = FakeAttributeRegistry()

    builder = make_timeseries_service_builder(
        instrument=instrument, attribute_registry=attribute_registry
    )
    sink = FakeMessageSink()
    source_names = ['detector_rotation', 'temperature', 'pressure']
    consumer = F144Consumer(
        instrument=instrument,
        max_messages=50,
        source_names=source_names,
    )

    service = builder.from_consumer(consumer=consumer, sink=UnrollingSinkAdapter(sink))

    service.start(blocking=False)
    start_and_wait_for_completion(consumer=consumer)
    service.stop()

    # Validate outputs
    assert len(sink.messages) > 0

    # Group messages by source name
    messages_by_source = {}
    for msg in sink.messages:
        source_name = msg.stream.name.split(':')[0]
        if source_name not in messages_by_source:
            messages_by_source[source_name] = []
        messages_by_source[source_name].append(msg)

    # Check that we have messages for each source
    for source_name in source_names:
        assert source_name in messages_by_source, f"No messages for {source_name}"

        # Check properties of the accumulated data
        for msg in messages_by_source[source_name]:
            data = msg.value
            assert isinstance(data, sc.DataArray)
            assert 'time' in data.dims

            # Check that the data has the expected units
            if source_name == 'detector_rotation:timeseries':
                assert data.unit == sc.Unit('deg')
            elif source_name == 'temperature:timeseries':
                assert data.unit == sc.Unit('C')
            elif source_name == 'pressure:timeseries':
                assert data.unit == sc.Unit('kPa')


def test_timeseries_accumulation() -> None:
    """Test that timeseries accumulates data correctly over time."""
    attribute_registry = FakeAttributeRegistry()

    instrument = 'dummy'
    builder = make_timeseries_service_builder(
        instrument=instrument, attribute_registry=attribute_registry
    )
    sink = FakeMessageSink()
    consumer = F144Consumer(
        instrument=instrument,
        max_messages=20,
        source_names=['detector_rotation'],  # Use just one source for simplicity
    )

    service = builder.from_consumer(consumer=consumer, sink=UnrollingSinkAdapter(sink))

    service.start(blocking=False)
    start_and_wait_for_completion(consumer=consumer)
    service.stop()

    # Get the messages for detector_rotation
    messages = [
        msg
        for msg in sink.messages
        if msg.stream.name == 'detector_rotation:timeseries'
    ]
    assert len(messages) > 1

    # Check that time is increasing
    for msg in messages:
        data = msg.value
        time_values = data.coords['time'].values
        assert np.all(
            np.diff(time_values) > 0
        ), "Time values should be strictly increasing"

    # Check that data accumulates over time
    sizes = [msg.value.sizes['time'] for msg in messages]
    assert sizes[-1] > sizes[0], "Data size should increase over time"
