# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime.fakes import FakeMessageSink
from beamlime.kafka.helpers import source_name
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.source import KafkaConsumer
from beamlime.services.monitor_data import make_monitor_service_builder


class EmptyConsumer(KafkaConsumer):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        return []

    def close(self) -> None:
        pass


class Ev44Consumer(KafkaConsumer):
    def __init__(
        self,
        num_sources: int = 1,
        events_per_message: int = 1_000,
        max_events: int = 1_000_000,
    ) -> None:
        self._num_sources = num_sources
        self._events_per_message = events_per_message
        self._max_events = max_events
        self._pixel_id = np.ones(events_per_message, dtype=np.int32)
        rng = np.random.default_rng()
        self._time_of_flight = rng.uniform(0, 70_000_000, events_per_message).astype(
            np.int32
        )
        self._reference_time = 0
        self._running = False
        self._count = 0
        self._content = [
            self.make_serialized_ev44(source) for source in range(self._num_sources)
        ]

    def start(self) -> None:
        self._running = True

    def reset(self) -> None:
        self._running = False
        self._count = 0

    @property
    def at_end(self) -> bool:
        return (
            self._count * self._events_per_message
            >= self._max_events * self._num_sources
        )

    def _make_timestamp(self) -> int:
        self._reference_time += 1
        self._count += 1
        return self._reference_time * 71_000_000 // self._num_sources

    def make_serialized_ev44(self, source: int) -> bytes:
        # Note empty reference_time. KafkaToEv44Adapter uses message.timestamp(), which
        # allows use to reuse the serialized content, to avoid seeing the cost in the
        # benchmarks.
        return eventdata_ev44.serialise_ev44(
            source_name=f"monitor_{source}",
            message_id=0,
            reference_time=[],
            reference_time_index=0,
            time_of_flight=self._time_of_flight,
            pixel_id=self._pixel_id,
        )

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        if not self._running or self.at_end:
            return []
        messages = [
            FakeKafkaMessage(
                value=self._content[msg % self._num_sources],
                topic="dummy",
                timestamp=self._make_timestamp(),
            )
            for msg in range(num_messages)
        ]
        return messages

    def close(self) -> None:
        pass


def start_and_wait_for_completion(consumer: Ev44Consumer) -> None:
    consumer.start()
    while not consumer.at_end:
        time.sleep(0.01)
    consumer.reset()


@pytest.mark.parametrize('num_sources', [1, 2, 4, 8])
@pytest.mark.parametrize('events_per_message', [10_000, 100_000, 1_000_000])
def test_performance(benchmark, num_sources: int, events_per_message: int) -> None:
    # There is some caveat in this benchmark: Ev44Consumer has no concept of real time.
    # It is this always returning messages quickly, which shifts the balance in the
    # services to a different place than in reality.
    builder = make_monitor_service_builder(instrument='dummy')
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=EmptyConsumer(),
        sink=FakeMessageSink(),
    )

    sink = FakeMessageSink()
    consumer = Ev44Consumer(
        num_sources=num_sources,
        events_per_message=events_per_message,
        max_events=50_000_000,
    )
    service = builder.build(
        control_consumer=EmptyConsumer(), consumer=consumer, sink=sink
    )
    service.start(blocking=False)
    benchmark(start_and_wait_for_completion, consumer=consumer)
    service.stop()


def test_monitor_data_service() -> None:
    builder = make_monitor_service_builder(instrument='dummy')
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=EmptyConsumer(),
        sink=FakeMessageSink(),
    )

    sink = FakeMessageSink()
    consumer = Ev44Consumer(num_sources=2, events_per_message=100, max_events=10_000)
    service = builder.build(
        control_consumer=EmptyConsumer(), consumer=consumer, sink=sink
    )
    service.start(blocking=False)
    start_and_wait_for_completion(consumer=consumer)
    source_names = [msg.key.source_name for msg in sink.messages]
    assert source_name('monitor_0', 'cumulative') in source_names
    assert source_name('monitor_1', 'cumulative') in source_names
    assert source_name('monitor_0', 'current') in source_names
    assert source_name('monitor_1', 'current') in source_names
    size = len(sink.messages)
    start_and_wait_for_completion(consumer=consumer)
    assert len(sink.messages) > size
    service.stop()
