# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes detector event data into 2-D data for plotting."""

import argparse
import logging
from contextlib import ExitStack
from typing import Literal, NoReturn

from beamlime import Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.stream_mapping import get_stream_mapping
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import RouteByTopicAdapter
from beamlime.kafka.routes import beamlime_config_route, detector_route
from beamlime.kafka.sink import KafkaSink, UnrollingSinkAdapter
from beamlime.kafka.source import MultiConsumer
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
    *, instrument: str, dev: bool = True, log_level: int = logging.INFO
) -> DataServiceBuilder:
    service_name = 'detector_data'
    config_handler = ConfigHandler(service_name=service_name)
    handler_factory = DetectorHandlerFactory(
        instrument=instrument, config_registry=config_handler
    )
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    adapter = RouteByTopicAdapter(
        routes={**detector_route(stream_mapping), **beamlime_config_route(instrument)},
    )
    builder = DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        adapter=adapter,
        handler_factory=handler_factory,
    )
    builder.add_handler(CONFIG_STREAM_ID, config_handler)
    return builder


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    dev: bool,
    log_level: int = logging.INFO,
) -> NoReturn:
    config = load_config(namespace=config_names.detector_data, env='')
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(instrument=instrument, kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()
    sink = UnrollingSinkAdapter(sink)

    builder = make_detector_service_builder(
        instrument=instrument, dev=dev, log_level=log_level
    )

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        data_consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='detector_data',
            )
        )
        consumer = MultiConsumer([control_consumer, data_consumer])
        service = builder.from_consumer(consumer=consumer, sink=sink)
        service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
