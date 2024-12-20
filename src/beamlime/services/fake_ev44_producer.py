# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import NoReturn

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

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
from beamlime.kafka.sink import KafkaSink, SerializationError


class FakeMonitorSource(MessageSource[sc.Variable]):
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

    def get_messages(self) -> list[Message[sc.Variable]]:
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
    ) -> Message[sc.Variable]:
        time_of_flight = self._make_normal(mean=30_000_000, std=10_000_000, size=size)
        var = sc.array(dims=['time_of_arrival'], values=time_of_flight, unit='ns')
        return Message(
            timestamp=timestamp,
            key=MessageKey(topic=self._topic, source_name=name),
            value=var,
        )


class IdentityHandler(Handler[sc.Variable, sc.Variable]):
    def handle(self, message: Message[sc.Variable]) -> list[Message[sc.Variable]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return [message]


def serialize_variable_to_monitor_ev44(msg: Message[sc.Variable]) -> bytes:
    if msg.value.unit != 'ns':
        raise SerializationError(f"Expected unit 'ns', got {msg.value.unit}")
    try:
        ev44 = eventdata_ev44.serialise_ev44(
            source_name=msg.key.source_name,
            message_id=0,
            reference_time=msg.timestamp,
            reference_time_index=0,
            time_of_flight=msg.value.values,
            pixel_id=np.ones_like(msg.value.values),
        )
    except (ValueError, TypeError) as e:
        raise SerializationError(f"Failed to serialize message: {e}") from None
    return ev44


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    service_name = f'{instrument}_fake_ev44_producer'
    config = load_config(namespace='fake_ev44')
    processor = StreamProcessor(
        source=FakeMonitorSource(instrument=instrument),
        sink=KafkaSink(
            kafka_config=config['producer']['kafka'],
            serializer=serialize_variable_to_monitor_ev44,
        ),
        handler_registry=HandlerRegistry(config={}, handler_cls=IdentityHandler),
    )
    service = Service(
        config=config['service'],
        processor=processor,
        name=service_name,
        log_level=log_level,
    )
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes random da00 monitor data')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
