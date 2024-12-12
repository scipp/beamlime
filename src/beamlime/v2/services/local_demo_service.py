# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time
from typing import NoReturn

import numpy as np
import scipp as sc

from beamlime.v2.core.handler import HandlerRegistry, Message, MessageKey
from beamlime.v2.core.message import MessageSink
from beamlime.v2.core.processor import StreamProcessor
from beamlime.v2.core.service import Service
from beamlime.v2.handlers.monitor_data_handler import MonitorDataHandler, MonitorEvents
from beamlime.v2.kafka.message_adapter import (
    AdaptingMessageSource,
    FakeKafkaMessage,
    KafkaMessage,
    MessageAdapter,
)
from beamlime.v2.kafka.source import KafkaConsumer, KafkaMessageSource


class FakeMonitorEventConsumer(KafkaConsumer[np.ndarray]):
    """Fake Kafka consumer that generates random monitor events."""

    def __init__(self):
        self._rng = np.random.default_rng()

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def consume(
        self, num_messages: int, timeout: float
    ) -> list[FakeKafkaMessage[np.ndarray]]:
        return [
            FakeKafkaMessage(
                value=self._make_normal(mean=30_000_000, std=10_000_000, size=10),
                topic="monitor1",
            )
            for _ in range(num_messages)
        ]


class KafkaNumPyToMonitorEventsAdapter(
    MessageAdapter[KafkaMessage[np.ndarray], Message[MonitorEvents]]
):
    def adapt(self, message: KafkaMessage[np.ndarray]) -> Message[MonitorEvents]:
        return Message(
            timestamp=time.time_ns(),
            key=MessageKey(topic=message.topic(), source_name="monitor1"),
            value=MonitorEvents(time_of_arrival=message.value()),
        )


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
            source=KafkaMessageSource(consumer=FakeMonitorEventConsumer()),
            adapter=KafkaNumPyToMonitorEventsAdapter(),
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
