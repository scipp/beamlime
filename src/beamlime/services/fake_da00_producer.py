# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import NoReturn

import numpy as np
import scipp as sc

from beamlime import (
    Handler,
    HandlerRegistry,
    Message,
    MessageKey,
    MessageSource,
    Service,
    StreamProcessor,
)
from beamlime.config.config_loader import load_config
from beamlime.kafka.helpers import beam_monitor_topic
from beamlime.kafka.sink import KafkaSink


class FakeMonitorSource(MessageSource[sc.DataArray]):
    """Fake message source that generates random monitor events."""

    def __init__(self, *, interval_ns: int = int(1e9 / 14), instrument: str):
        self._topic = beam_monitor_topic(instrument=instrument)
        self._rng = np.random.default_rng()
        self._tof = sc.linspace('tof', 0, 71_000_000, num=50, unit='ns')
        self._interval_ns = interval_ns
        self._last_message_time = {
            "monitor1": time.time_ns(),
            "monitor2": time.time_ns(),
        }

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def get_messages(self) -> list[Message[sc.DataArray]]:
        current_time = time.time_ns()
        messages = []

        for name, size in [("monitor1", 30), ("monitor2", 10)]:
            elapsed = current_time - self._last_message_time[name]
            num_intervals = int(elapsed // self._interval_ns)

            for i in range(num_intervals):
                msg_time = self._last_message_time[name] + (i + 1) * self._interval_ns
                messages.append(
                    self._make_message(name=name, size=size, timestamp=msg_time)
                )
            self._last_message_time[name] += num_intervals * self._interval_ns

        return messages

    def _make_message(
        self, name: str, size: int, timestamp: int
    ) -> Message[sc.DataArray]:
        time_of_flight = self._make_normal(mean=30_000_000, std=10_000_000, size=size)
        var = sc.array(dims=['time_of_arrival'], values=time_of_flight, unit='ns')
        da = var.hist(tof=self._tof)
        return Message(
            timestamp=timestamp,
            key=MessageKey(topic=self._topic, source_name=name),
            value=da,
        )


class IdentityHandler(Handler[sc.DataArray, sc.DataArray]):
    def handle(self, message: Message[sc.DataArray]) -> list[Message[sc.DataArray]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return [message]


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    service_name = f'{instrument}_fake_da00_producer'
    producer_config = load_config(namespace='fake_da00')['producer']
    processor = StreamProcessor(
        source=FakeMonitorSource(instrument=instrument),
        sink=KafkaSink(kafka_config=producer_config['kafka']),
        handler_registry=HandlerRegistry(config={}, handler_cls=IdentityHandler),
    )
    service = Service(
        config={}, processor=processor, name=service_name, log_level=log_level
    )
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes random da00 monitor data')
    args = parser.parse_args()
    run_service(instrument=args.instrument, log_level=args.log_level)


if __name__ == "__main__":
    main()
