# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
from typing import NoReturn, TypeVar

import numpy as np
import scipp as sc

from beamlime import Handler, Message, MessageKey, MessageSource, Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.core.handler import CommonHandlerFactory
from beamlime.kafka.helpers import motion_topic
from beamlime.kafka.sink import KafkaSink, serialize_dataarray_to_f144
from beamlime.service_factory import DataServiceBuilder


def _make_ramp(size: int) -> sc.DataArray:
    """Create a ramp pattern that can be used for continuous data generation.

    Returns a DataArray with values that ramp up and down between 0 and 90 degrees.
    """
    # Create values that go from 0 to 90 and back to 0
    values = np.concatenate(
        [
            np.linspace(0.0, 90.0, size // 2 + 1),
            np.linspace(90.0, 0.0, size // 2 + 1)[1:],
        ]
    )

    # Time offsets, one second between each point
    time_offsets = sc.linspace(
        'time', 0.0, float(len(values) - 1), num=len(values), unit='s'
    )

    return sc.DataArray(
        sc.array(dims=['time'], values=values, unit='deg'),
        coords={'time': time_offsets.to(unit='ns', dtype='int64')},
    )


class FakeLogdataSource(MessageSource[sc.DataArray]):
    """Fake message source that generates continuous monitor events in a loop."""

    def __init__(self, *, instrument: str):
        self._topic = motion_topic(instrument=instrument)
        # Create the base ramp patterns
        self._ramp_patterns = {'detector_rotation': _make_ramp(size=100)}
        # Track the current time and cycle count for each log data
        self._current_time = {name: self._time_ns() for name in self._ramp_patterns}
        # Track the last index we produced for each log
        self._current_index = {name: 0 for name in self._ramp_patterns}
        # How often to produce new data points (in seconds)
        self._interval_ns = int(1e9)  # 1 second in nanoseconds
        self._last_produce_time = self._time_ns()

    def _time_ns(self) -> sc.Variable:
        """Return the current time in nanoseconds."""
        return sc.datetime('now', unit='ns') - sc.epoch(unit='ns')

    def get_messages(self) -> list[Message[sc.DataArray]]:
        messages = []
        current_time = self._time_ns()

        # Only produce new messages if enough time has passed
        elapsed_ns = current_time.value - self._last_produce_time.value
        if elapsed_ns < self._interval_ns:
            return messages

        self._last_produce_time = current_time

        for name, pattern in self._ramp_patterns.items():
            # Get the next point in the pattern
            idx = self._current_index[name]
            pattern_size = pattern.sizes['time']

            # Get position in the pattern cycle
            cycle_idx = idx % pattern_size

            # Calculate how many complete cycles we've done
            cycles = idx // pattern_size

            # Calculate the new time, ensuring it always increases
            # Base time + cycle duration + current offset within the pattern
            cycle_duration_ns = pattern.coords['time'][-1].value
            new_time = (
                self._current_time[name].value
                + cycle_duration_ns * cycles
                + pattern.coords['time'][cycle_idx].value
            )

            # Get the scalar value from the pattern
            value = pattern.values[cycle_idx]

            # Create the data point with the updated timestamp - using scalar values
            data_point = sc.DataArray(
                data=sc.scalar(value, unit=pattern.unit),
                coords={'time': sc.scalar(new_time, unit='ns')},
            )

            # Create and add the message
            messages.append(self._make_message(name=name, data=data_point))

            # Move to the next index
            self._current_index[name] = idx + 1

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
    kafka_config = load_config(namespace=config_names.kafka_upstream)
    serializer = serialize_dataarray_to_f144
    builder = DataServiceBuilder(
        instrument=instrument,
        name='fake_f144_producer',
        log_level=log_level,
        handler_factory=CommonHandlerFactory(handler_cls=IdentityHandler),
    )
    service = builder.from_source(
        source=FakeLogdataSource(instrument=instrument),
        sink=KafkaSink(kafka_config=kafka_config, serializer=serializer),
    )
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes f144 logdata')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
