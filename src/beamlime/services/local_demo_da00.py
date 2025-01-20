# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time
from typing import NoReturn

import numpy as np
import scipp as sc
from streaming_data_types import dataarray_da00

from beamlime import CommonHandlerFactory, Service, StreamProcessor
from beamlime.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    FakeKafkaMessage,
    KafkaToDa00Adapter,
)
from beamlime.kafka.scipp_da00_compat import scipp_to_da00
from beamlime.kafka.source import KafkaConsumer, KafkaMessageSource
from beamlime.sinks import PlotToPngSink


class FakeMonitorDa00KafkaConsumer(KafkaConsumer):
    """Fake Kafka consumer that generates random monitor events."""

    def __init__(self):
        self._rng = np.random.default_rng()
        self._tof = sc.linspace('tof', 0, 71_000_000, num=50, unit='ns')

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def consume(self, num_messages: int, timeout: float) -> list[FakeKafkaMessage]:
        _ = num_messages
        _ = timeout
        return [
            self._make_message(name="monitor1", size=30),
            self._make_message(name="monitor2", size=10),
        ]

    def _make_message(self, name: str, size: int) -> FakeKafkaMessage:
        time_of_flight = self._make_normal(mean=30_000_000, std=10_000_000, size=size)
        var = sc.array(dims=['time_of_arrival'], values=time_of_flight, unit='ns')
        da = var.hist(tof=self._tof)
        da00 = dataarray_da00.serialise_da00(
            source_name=name,
            timestamp_ns=time.time_ns(),
            data=scipp_to_da00(da),
        )
        return FakeKafkaMessage(value=da00, topic="dummy_beam_monitor")


def main() -> NoReturn:
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=FakeMonitorDa00KafkaConsumer()),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        ),
        sink=PlotToPngSink(),
        handler_factory=CommonHandlerFactory(
            config={}, handler_cls=create_monitor_data_handler
        ),
    )
    service = Service(processor=processor, name="local_demo_da00")
    service.start()


if __name__ == "__main__":
    main()
