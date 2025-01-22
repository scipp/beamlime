# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import time

import numpy as np
from streaming_data_types import eventdata_ev44

from beamlime import CommonHandlerFactory
from beamlime.fakes import FakeMessageSink
from beamlime.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.source import KafkaConsumer
from beamlime.service_factory import DataServiceBuilder
from beamlime.services.monitor_data import make_monitor_data_adapter


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

    def start(self) -> None:
        self._running = True

    def reset(self) -> None:
        self._running = False
        self._reference_time = 0

    @property
    def at_end(self) -> bool:
        return self._reference_time * self._events_per_message >= self._max_events

    def make_serialized_ev44(self, source: int) -> bytes:
        self._reference_time += 1
        return eventdata_ev44.serialise_ev44(
            source_name=f"monitor_{source}",
            message_id=0,
            reference_time=[self._reference_time],
            reference_time_index=0,
            time_of_flight=self._time_of_flight,
            pixel_id=self._pixel_id,
        )

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        if not self._running or self.at_end:
            return []
        messages = [
            FakeKafkaMessage(
                value=self.make_serialized_ev44(msg % self._num_sources), topic="dummy"
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


def test_performance(benchmark) -> None:
    builder = DataServiceBuilder(
        instrument='dummy',
        name='monitor_data',
        adapter=make_monitor_data_adapter(),
        handler_factory_cls=CommonHandlerFactory.from_handler(
            create_monitor_data_handler
        ),
    )
    service = builder.build(
        control_consumer=EmptyConsumer(),
        consumer=EmptyConsumer(),
        sink=FakeMessageSink(),
    )

    sink = FakeMessageSink()
    num_sources = 3
    events_per_message = 100_000
    consumer = Ev44Consumer(
        num_sources=num_sources,
        events_per_message=events_per_message,
        max_events=10_000_000,
    )
    service = builder.build(
        control_consumer=EmptyConsumer(), consumer=consumer, sink=sink
    )
    service.start(blocking=False)
    benchmark(start_and_wait_for_completion, consumer=consumer)
    service.stop()
