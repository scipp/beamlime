# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time
from functools import partial

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime.fakes import FakeMessageSink
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory
from beamlime.kafka.message_adapter import (
    ChainedAdapter,
    Ev44ToDetectorEventsAdapter,
    FakeKafkaMessage,
    KafkaMessage,
    KafkaToEv44Adapter,
)
from beamlime.kafka.source import KafkaConsumer
from beamlime.service_factory import DataServiceBuilder
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
        self._rng = np.random.default_rng()

        self._reference_time = 0
        self._running = False
        self._count = 0
        self._content = [
            self.make_serialized_ev44(name) for name in self._detector_config
        ]

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
        # allows use to reuse the serialized content, to avoid seeing the cost in the
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
        messages = [
            FakeKafkaMessage(
                value=self._content[msg % len(self._content)],
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


@pytest.mark.parametrize('instrument', ['dummy', 'nmx', 'loki', 'dream'])
@pytest.mark.parametrize('events_per_message', [10_000, 100_000, 1_000_000])
def test_performance(benchmark, instrument: str, events_per_message: int) -> None:
    # There is some caveat in this benchmark: Ev44Consumer has no concept of real time.
    # It is thus always returning messages quickly, which shifts the balance in the
    # services to a different place than in reality.
    builder = DataServiceBuilder(
        instrument=instrument,
        name='detector_data',
        adapter=ChainedAdapter(
            first=KafkaToEv44Adapter(), second=Ev44ToDetectorEventsAdapter()
        ),
        handler_factory_cls=partial(DetectorHandlerFactory, instrument=instrument),
    )
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
