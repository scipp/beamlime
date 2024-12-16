# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time
from typing import NoReturn

import numpy as np
import scipp as sc

from beamlime.v2 import (
    Handler,
    HandlerRegistry,
    Message,
    MessageKey,
    MessageSource,
    Service,
    StreamProcessor,
)
from beamlime.v2.kafka.sink import KafkaSink


class FakeMonitorSource(MessageSource[sc.DataArray]):
    """Fake message source that generates random monitor events."""

    def __init__(self):
        self._rng = np.random.default_rng()
        self._tof = sc.linspace('tof', 0, 71_000_000, num=50, unit='ns')

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def get_messages(self) -> list[Message[sc.DataArray]]:
        return [
            self._make_message(name="monitor1", size=30),
            self._make_message(name="monitor2", size=10),
        ]

    def _make_message(self, name: str, size: int) -> Message[sc.DataArray]:
        time_of_flight = self._make_normal(mean=30_000_000, std=10_000_000, size=size)
        var = sc.array(dims=['time_of_arrival'], values=time_of_flight, unit='ns')
        da = var.hist(tof=self._tof)
        return Message(
            timestamp=time.time_ns(),
            key=MessageKey(topic="monitors", source_name=name),
            value=da,
        )


class IdentityHandler(Handler[sc.DataArray, sc.DataArray]):
    def handle(self, message: Message[sc.DataArray]) -> list[Message[sc.DataArray]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return [message]


def main() -> NoReturn:
    handler_config = {}
    service_config = {}
    producer_config = {"bootstrap.servers": "localhost:9092"}
    processor = StreamProcessor(
        source=FakeMonitorSource(),
        sink=KafkaSink(kafka_config=producer_config),
        handler_registry=HandlerRegistry(
            config=handler_config, handler_cls=IdentityHandler
        ),
    )
    service = Service(
        config=service_config, processor=processor, name="fake_da00_producer"
    )
    service.start()


if __name__ == "__main__":
    main()
