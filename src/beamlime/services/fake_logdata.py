# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
from typing import NoReturn, TypeVar

import scipp as sc

from beamlime import (
    Handler,
    Message,
    MessageKey,
    MessageSource,
    Service,
    StreamProcessor,
)
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.core.handler import CommonHandlerFactory
from beamlime.kafka.helpers import motion_topic
from beamlime.kafka.sink import KafkaSink, serialize_dataarray_to_f144


def _make_ramp(size: int) -> sc.DataArray:
    return sc.DataArray(
        sc.linspace('time', 0.0, 90.0, num=size + 1, unit='deg'),
        coords={
            'time': sc.datetime('now', unit='ns')
            - sc.epoch(unit='ns')
            + sc.linspace('time', 0.0, 180.0, num=size + 1, unit='s').to(
                unit='ns', dtype='int64'
            )
        },
    )


class FakeLogdataSource(MessageSource[sc.DataArray]):
    """Fake message source that generates random monitor events."""

    def __init__(self, *, instrument: str):
        self._topic = motion_topic(instrument=instrument)
        self._logdata = {'detector_rotation': _make_ramp(size=100)}
        self._last_message_time = {name: self._time_ns() for name in self._logdata}

    def _time_ns(self) -> sc.Variable:
        """Return the current time in nanoseconds."""
        return sc.datetime('now', unit='ns') - sc.epoch(unit='ns')

    def get_messages(self) -> list[Message[sc.DataArray]]:
        messages = []

        for name, logdata in self._logdata.items():
            start = self._last_message_time[name]
            end = self._time_ns()
            data = logdata['time', start:end]
            self._last_message_time[name] = end
            messages.extend(
                self._make_message(name=name, data=data['time', i])
                for i in range(data.sizes['time'])
            )

        return messages

    def _make_message(self, name: str, data: sc.DataArray) -> Message[sc.DataArray]:
        """Create a message with the given data and timestamp."""
        return Message(
            timestamp=self._time_ns().value,
            key=MessageKey(topic=self._topic, source_name=name),
            value=data,
        )


T = TypeVar('T')


class IdentityHandler(Handler[T, T]):
    def handle(self, messages: list[Message[T]]) -> list[Message[T]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return messages


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    service_name = f'{instrument}_fake_f144_producer'
    kafka_config = load_config(namespace=config_names.kafka_upstream)
    source = FakeLogdataSource(instrument=instrument)
    serializer = serialize_dataarray_to_f144
    processor = StreamProcessor(
        source=source,
        sink=KafkaSink(kafka_config=kafka_config, serializer=serializer),
        handler_factory=CommonHandlerFactory(config={}, handler_cls=IdentityHandler),
    )
    service = Service(processor=processor, name=service_name, log_level=log_level)
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes f144 logdata')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
