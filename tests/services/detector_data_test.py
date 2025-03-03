# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime.config.raw_detectors import available_instruments, get_config
from beamlime.fakes import FakeMessageSink
from beamlime.kafka.helpers import source_name
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.sink import UnrollingSinkAdapter
from beamlime.kafka.source import KafkaConsumer
from beamlime.services.detector_data import make_detector_service_builder
from beamlime.services.fake_detectors import detector_config


class EmptyConsumer(KafkaConsumer):
    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        return []

    def close(self) -> None:
        pass


class Ev44Consumer(KafkaConsumer):
    def __init__(
        self,
        instrument: str = 'dummy',
        events_per_message: int = 1_000,
        max_events: int = 1_000_000,
    ) -> None:
        self._detector_config = detector_config[instrument]
        self._events_per_message = events_per_message
        self._max_events = max_events
        self._pixel_id = np.ones(events_per_message, dtype=np.int32)
        self._rng = np.random.default_rng(seed=1234)  # Avoid test flakiness

        self._reference_time = 0
        self._running = False
        self._count = 0
        self._content = [
            self.make_serialized_ev44(name) for name in self._detector_config
        ]
        self._current = 0

    def start(self) -> None:
        self._running = True

    def reset(self) -> None:
        self._running = False
        self._count = 0

    @property
    def at_end(self) -> bool:
        return self._count * self._events_per_message >= self._max_events * len(
            self._detector_config
        )

    def _make_timestamp(self) -> int:
        self._reference_time += 1
        self._count += 1
        return self._reference_time * 71_000_000 // len(self._detector_config)

    def make_serialized_ev44(self, name: str) -> bytes:
        time_of_arrival = self._rng.uniform(
            0, 70_000_000, self._events_per_message
        ).astype(np.int32)
        first, last = self._detector_config[name]
        pixel_id = self._rng.integers(
            first, last + 1, self._events_per_message, dtype=np.int32
        )
        # Note empty reference_time. KafkaToEv44Adapter uses message.timestamp(), which
        # allows us to reuse the serialized content, to avoid seeing the cost in the
        # benchmarks.
        return eventdata_ev44.serialise_ev44(
            source_name=name,
            message_id=0,
            reference_time=[],
            reference_time_index=0,
            time_of_flight=time_of_arrival,
            pixel_id=pixel_id,
        )

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        if not self._running or self.at_end:
            return []
        remaining_events = (
            self._max_events * len(self._detector_config)
            - self._count * self._events_per_message
        )
        remaining_messages = remaining_events // self._events_per_message
        messages_to_produce = min(num_messages, remaining_messages)
        if messages_to_produce <= 0:
            return []
        messages = [
            FakeKafkaMessage(
                value=self._content[(self._current + msg) % len(self._content)],
                topic="dummy",
                timestamp=self._make_timestamp(),
            )
            for msg in range(messages_to_produce)
        ]
        self._current += messages_to_produce
        return messages

    def close(self) -> None:
        pass


def start_and_wait_for_completion(consumer: Ev44Consumer) -> None:
    consumer.start()
    while not consumer.at_end:
        time.sleep(0.01)
    consumer.reset()


@pytest.mark.parametrize('instrument', ['dummy', 'nmx', 'loki', 'dream'])
@pytest.mark.parametrize('events_per_message', [10_000, 100_000, 1_000_000])
def test_performance(benchmark, instrument: str, events_per_message: int) -> None:
    # There is some caveat in this benchmark: Ev44Consumer has no concept of real time.
    # It is thus always returning messages quickly, which shifts the balance in the
    # services to a different place than in reality.
    builder = make_detector_service_builder(instrument=instrument)
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=EmptyConsumer(),
        sink=FakeMessageSink(),
    )

    sink = FakeMessageSink()
    consumer = Ev44Consumer(
        instrument=instrument,
        events_per_message=events_per_message,
        max_events=50_000_000,
    )
    service = builder.build(
        control_consumer=EmptyConsumer(), consumer=consumer, sink=sink
    )
    service.start(blocking=False)
    benchmark(start_and_wait_for_completion, consumer=consumer)
    service.stop()
    assert len(sink.messages) > len(detector_config[instrument]) * 10


@pytest.mark.parametrize('instrument', available_instruments())
def test_detector_data_service(instrument: str) -> None:
    builder = make_detector_service_builder(instrument=instrument)
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=EmptyConsumer(),
        sink=FakeMessageSink(),
    )
    sink = FakeMessageSink()
    consumer = Ev44Consumer(
        instrument=instrument, events_per_message=100, max_events=10_000
    )
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=consumer,
        sink=UnrollingSinkAdapter(sink),
    )
    service.start(blocking=False)
    start_and_wait_for_completion(consumer=consumer)
    service.stop()
    source_names = [msg.key.source_name for msg in sink.messages]

    detectors = get_config(instrument).detectors_config['detectors']
    for view_name, view_config in detectors.items():
        base_key = source_name(device=view_config['detector_name'], signal=view_name)
        assert f'{base_key}/cumulative' in source_names
        assert f'{base_key}/current' in source_names
        assert f'{base_key}/roi' in source_names

    # Implicitly yields the latest cumulative message for each detector
    cumulative = {
        msg.key.source_name: msg.value
        for msg in sink.messages
        if msg.key.source_name.endswith('/cumulative')
    }
    assert len(cumulative) == len(detectors)
    for name, msg in cumulative.items():
        if instrument == 'dream':
            if 'mantle_front_layer' in name:
                # fraction of voxels => fracion of counts
                assert 50 < msg.sum().value < 500
            elif 'High-Res' in name:
                # non-contiguous detector_number, but the fake produces random numbers
                # in the gaps
                assert 5000 < msg.sum().value < 8000
            else:
                assert msg.sum().value == 10000
        elif instrument == 'bifrost':
            # non-contiguous detector_number, but we get them "back" by merging banks
            ndet = 45
            assert msg.sum().value == 10000 * ndet
        else:
            assert msg.sum().value == 10000
