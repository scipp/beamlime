# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Fake that publishes random detector data"""

import logging
import time
from typing import NoReturn, TypeVar

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from beamlime import Handler, Message, MessageSource, Service, StreamId, StreamKind
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.instruments import get_config
from beamlime.core.handler import CommonHandlerFactory
from beamlime.kafka.sink import KafkaSink, SerializationError
from beamlime.service_factory import DataServiceBuilder


class FakeDetectorSource(MessageSource[sc.Dataset]):
    """Fake message source that generates random detector events."""

    def __init__(
        self,
        *,
        interval_ns: int = int(1e9 / 14),
        instrument: str,
    ):
        self._instrument = instrument
        self._config = get_config(instrument).detectors_config['fakes']
        self._rng = np.random.default_rng()
        self._tof = sc.linspace('tof', 0, 71_000_000, num=50, unit='ns')
        self._interval_ns = interval_ns
        self._last_message_time = {
            detector: time.time_ns() for detector in self._config
        }

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def _make_ids(self, name: str, size: int) -> np.ndarray:
        low, high = self._config[name]
        return self._rng.integers(low=low, high=high + 1, size=size)

    def get_messages(self) -> list[Message[sc.Dataset]]:
        current_time = time.time_ns()
        messages = []

        for name in self._last_message_time:
            size = 1_000 if self._instrument == 'bifrost' else 100_000
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
        pixel_id = self._make_ids(name=name, size=size)
        ds = sc.Dataset(
            {
                'time_of_arrival': sc.array(
                    dims=['time_of_arrival'], values=time_of_flight, unit='ns'
                ),
                'pixel_id': sc.array(
                    dims=['time_of_arrival'], values=pixel_id, unit=None
                ),
            }
        )

        return Message(
            timestamp=timestamp,
            stream=StreamId(kind=StreamKind.DETECTOR_EVENTS, name=name),
            value=ds,
        )


T = TypeVar('T')


class IdentityHandler(Handler[T, T]):
    def handle(self, messages: list[Message[T]]) -> list[Message[T]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return messages


def serialize_detector_events_to_ev44(
    msg: Message[tuple[sc.Variable, sc.Variable]],
) -> bytes:
    if msg.value['time_of_arrival'].unit != 'ns':
        raise SerializationError(f"Expected unit 'ns', got {msg.value.unit}")
    try:
        ev44 = eventdata_ev44.serialise_ev44(
            source_name=msg.stream.name,
            message_id=0,
            reference_time=msg.timestamp,
            reference_time_index=0,
            time_of_flight=msg.value['time_of_arrival'].values,
            pixel_id=msg.value['pixel_id'].values,
        )
    except (ValueError, TypeError) as e:
        raise SerializationError(f"Failed to serialize message: {e}") from None
    return ev44


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    kafka_config = load_config(namespace=config_names.kafka_upstream)
    serializer = serialize_detector_events_to_ev44
    builder = DataServiceBuilder(
        instrument=instrument,
        name='fake_producer',
        log_level=log_level,
        handler_factory=CommonHandlerFactory(handler_cls=IdentityHandler),
    )
    service = builder.from_source(
        source=FakeDetectorSource(instrument=instrument),
        sink=KafkaSink(
            instrument=instrument, kafka_config=kafka_config, serializer=serializer
        ),
    )
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser(
        'Fake that publishes random detector data', dev_flag=False
    )
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
