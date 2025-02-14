# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Fake that publishes random detector data"""

import logging
import time
from typing import NoReturn, TypeVar

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

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
from beamlime.kafka.helpers import detector_topic
from beamlime.kafka.sink import KafkaSink, SerializationError

# Configure detectors to fake for each instrument
# Values as of January 2025. These may change if the detector configuration changes.
detector_config = {
    'dummy': {
        'panel_0': (1, 128**2),
    },
    'dream': {
        'mantle_detector': (229377, 720896),
        'endcap_backward_detector': (71618, 229376),
        'endcap_forward_detector': (1, 71680),
        'high_resolution_detector': (1122337, 1523680),  # Note: Not consecutive!
    },
    'loki': {
        'loki_detector_0': (1, 802816),
        'loki_detector_1': (802817, 1032192),
        'loki_detector_2': (1032193, 1204224),
        'loki_detector_3': (1204225, 1433600),
        'loki_detector_4': (1433601, 1605632),
        'loki_detector_5': (1605633, 2007040),
        'loki_detector_6': (2007041, 2465792),
        'loki_detector_7': (2465793, 2752512),
        'loki_detector_8': (2752513, 3211264),
    },
    'nmx': {
        f'detector_panel_{i}': (i * 1280**2 + 1, (i + 1) * 1280**2) for i in range(3)
    },
    'bifrost': {
        '123_channel_1_1_triplet': (1, 300),
        '127_channel_1_2_triplet': (301, 600),
        '131_channel_1_3_triplet': (601, 900),
        '135_channel_1_4_triplet': (901, 1200),
        '139_channel_1_5_triplet': (1201, 1500),
        '144_channel_2_1_triplet': (1501, 1800),
        '148_channel_2_2_triplet': (1801, 2100),
        '152_channel_2_3_triplet': (2101, 2400),
        '156_channel_2_4_triplet': (2401, 2700),
        '160_channel_2_5_triplet': (2701, 3000),
        '165_channel_3_1_triplet': (3001, 3300),
        '169_channel_3_2_triplet': (3301, 3600),
        '173_channel_3_3_triplet': (3601, 3900),
        '177_channel_3_4_triplet': (3901, 4200),
        '181_channel_3_5_triplet': (4201, 4500),
        '186_channel_4_1_triplet': (4501, 4800),
        '190_channel_4_2_triplet': (4801, 5100),
        '194_channel_4_3_triplet': (5101, 5400),
        '198_channel_4_4_triplet': (5401, 5700),
        '202_channel_4_5_triplet': (5701, 6000),
        '207_channel_5_1_triplet': (6001, 6300),
        '211_channel_5_2_triplet': (6301, 6600),
        '215_channel_5_3_triplet': (6601, 6900),
        '219_channel_5_4_triplet': (6901, 7200),
        '223_channel_5_5_triplet': (7201, 7500),
        '228_channel_6_1_triplet': (7501, 7800),
        '232_channel_6_2_triplet': (8101, 8400),
        '236_channel_6_3_triplet': (8701, 9000),
        '240_channel_6_4_triplet': (9001, 9300),
        '244_channel_6_5_triplet': (8401, 8700),
        '249_channel_7_1_triplet': (9001, 9300),
        '253_channel_7_2_triplet': (9301, 9600),
        '257_channel_7_3_triplet': (9601, 9900),
        '261_channel_7_4_triplet': (9901, 10200),
        '265_channel_7_5_triplet': (10201, 10500),
        '270_channel_8_1_triplet': (10501, 10800),
        '274_channel_8_2_triplet': (10801, 11100),
        '278_channel_8_3_triplet': (10801, 11100),
        '282_channel_8_4_triplet': (11101, 11400),
        '286_channel_8_5_triplet': (11401, 11700),
        '291_channel_9_1_triplet': (11701, 12000),
        '295_channel_9_2_triplet': (12001, 12300),
        '299_channel_9_3_triplet': (12301, 12600),
        '303_channel_9_4_triplet': (12601, 12900),
        '307_channel_9_5_triplet': (12901, 13200),
    },
}


class FakeDetectorSource(MessageSource[sc.Dataset]):
    """Fake message source that generates random detector events."""

    def __init__(
        self,
        *,
        interval_ns: int = int(1e9 / 14),
        instrument: str,
    ):
        self._instrument = instrument
        self._topic = detector_topic(instrument=instrument)
        self._rng = np.random.default_rng()
        self._tof = sc.linspace('tof', 0, 71_000_000, num=50, unit='ns')
        self._interval_ns = interval_ns
        self._last_message_time = {
            detector: time.time_ns() for detector in detector_config[instrument]
        }

    def _make_normal(self, mean: float, std: float, size: int) -> np.ndarray:
        return self._rng.normal(loc=mean, scale=std, size=size).astype(np.int64)

    def _make_ids(self, name: str, size: int) -> np.ndarray:
        low, high = detector_config[self._instrument][name]
        return self._rng.integers(low=low, high=high + 1, size=size)

    def get_messages(self) -> list[Message[sc.Dataset]]:
        current_time = time.time_ns()
        messages = []

        for name in self._last_message_time:
            size = 100_000
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
            key=MessageKey(topic=self._topic, source_name=name),
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
            source_name=msg.key.source_name,
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
    service_name = f'{instrument}_fake_producer'
    kafka_config = load_config(namespace=config_names.kafka_upstream)
    source = FakeDetectorSource(instrument=instrument)
    serializer = serialize_detector_events_to_ev44
    processor = StreamProcessor(
        source=source,
        sink=KafkaSink(kafka_config=kafka_config, serializer=serializer),
        handler_factory=CommonHandlerFactory(config={}, handler_cls=IdentityHandler),
    )
    service = Service(processor=processor, name=service_name, log_level=log_level)
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes random detector data')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
