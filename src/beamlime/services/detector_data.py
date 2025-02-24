# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes detector event data into 2-D data for plotting."""

import argparse
import logging
from contextlib import ExitStack
from functools import partial
from typing import Literal, NoReturn

from beamlime import Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    ChainedAdapter,
    Ev44ToDetectorEventsAdapter,
    KafkaToEv44Adapter,
)
from beamlime.kafka.sink import KafkaSink, UnrollingSinkAdapter
from beamlime.service_factory import DataServiceBuilder
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Detector Data Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def make_detector_service_builder(
    *, instrument: str, log_level: int = logging.INFO
) -> DataServiceBuilder:
    adapter = ChainedAdapter(
        first=KafkaToEv44Adapter(),
        second=Ev44ToDetectorEventsAdapter(merge_detectors=instrument == 'bifrost'),
    )
    handler_factory_cls = partial(DetectorHandlerFactory, instrument=instrument)
    return DataServiceBuilder(
        instrument=instrument,
        name='detector_data',
        log_level=log_level,
        adapter=adapter,
        handler_factory_cls=handler_factory_cls,
    )


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    log_level: int = logging.INFO,
) -> NoReturn:
    config = load_config(namespace=config_names.detector_data, env='')
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()
    sink = UnrollingSinkAdapter(sink)

    builder = make_detector_service_builder(instrument=instrument, log_level=log_level)

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='detector_data',
            )
        )
        service = builder.build(
            control_consumer=control_consumer, consumer=consumer, sink=sink
        )
        service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
