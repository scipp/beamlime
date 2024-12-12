# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time
from typing import NoReturn

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from beamlime.v2.core.handler import HandlerRegistry, Message
from beamlime.v2.core.message import MessageSink
from beamlime.v2.core.processor import StreamProcessor
from beamlime.v2.core.service import Service
from beamlime.v2.handlers.monitor_data_handler import MonitorDataHandler
from beamlime.v2.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Ev44ToMonitorEventsAdapter,
    FakeKafkaMessage,
    KafkaToEv44Adapter,
)
from beamlime.v2.kafka.source import KafkaConsumer, KafkaMessageSource


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
            messages.append(FakeKafkaMessage(value=ev44, topic="monitors"))
        return messages


class PlotToPngSink(MessageSink[sc.DataArray]):
    def publish_messages(self, messages: Message[sc.DataArray]) -> None:
        for msg in messages:
            title = f"{msg.key.topic} - {msg.key.source_name}"
            filename = f"{msg.key.topic}_{msg.key.source_name}.png"
            msg.value.plot(title=title).save(filename)


def main() -> NoReturn:
    handler_config = {'sliding_window_seconds': 5}
    service_config = {}
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=FakeMonitorEventKafkaConsumer()),
            adapter=ChainedAdapter(
                first=KafkaToEv44Adapter(), second=Ev44ToMonitorEventsAdapter()
            ),
        ),
        sink=PlotToPngSink(),
        handler_registry=HandlerRegistry(
            config=handler_config, handler_cls=MonitorDataHandler
        ),
    )
    service = Service(config=service_config, processor=processor, name="local_demo")
    service.start()


if __name__ == "__main__":
    main()
