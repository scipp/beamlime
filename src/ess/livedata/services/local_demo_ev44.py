# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time
from typing import NoReturn

import numpy as np
from streaming_data_types import eventdata_ev44

from ess.livedata import CommonHandlerFactory, Service, StreamProcessor
from ess.livedata.handlers.monitor_data_handler import create_monitor_data_handler
from ess.livedata.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Ev44ToMonitorEventsAdapter,
    FakeKafkaMessage,
    KafkaToEv44Adapter,
)
from ess.livedata.kafka.source import KafkaConsumer, KafkaMessageSource
from ess.livedata.sinks import PlotToPngSink


class FakeMonitorEventKafkaConsumer(KafkaConsumer):
    """Fake Kafka consumer that generates random monitor events."""

    def __init__(self):
        self._rng = np.random.default_rng()

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def consume(self, num_messages: int, timeout: float) -> list[FakeKafkaMessage]:
        messages = []
        for _ in range(num_messages):
            time_of_flight = self._make_normal(mean=30_000_000, std=10_000_000, size=10)
            ev44 = eventdata_ev44.serialise_ev44(
                source_name="monitor1",
                message_id=0,
                reference_time=[time.time_ns()],
                reference_time_index=[0],
                time_of_flight=time_of_flight,
                pixel_id=np.ones_like(time_of_flight, dtype=np.int32),
            )
            messages.append(FakeKafkaMessage(value=ev44, topic="dummy_beam_monitor"))
        return messages


def main() -> NoReturn:
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=FakeMonitorEventKafkaConsumer()),
            adapter=ChainedAdapter(
                first=KafkaToEv44Adapter(), second=Ev44ToMonitorEventsAdapter()
            ),
        ),
        sink=PlotToPngSink(),
        handler_factory=CommonHandlerFactory(
            config_handler={}, handler_cls=create_monitor_data_handler
        ),
    )
    service = Service(processor=processor, name="local_demo_ev44")
    service.start()


if __name__ == "__main__":
    main()
