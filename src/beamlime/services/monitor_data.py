# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
from contextlib import ExitStack
from typing import Literal, NoReturn

from beamlime import CommonHandlerFactory, Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.helpers import beam_monitor_topic, beamlime_command_topic
from beamlime.kafka.message_adapter import (
    BeamlimeCommandsAdapter,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
    KafkaToMonitorEventsAdapter,
    RouteByTopicAdapter,
    RoutingAdapter,
)
from beamlime.kafka.sink import KafkaSink
from beamlime.kafka.source import MultiConsumer
from beamlime.service_factory import DataServiceBuilder
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Kafka Demo da00/ev44 Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def make_monitor_data_adapter(instrument: str) -> RoutingAdapter:
    monitors = RoutingAdapter(
        routes={
            'ev44': KafkaToMonitorEventsAdapter(),
            'da00': ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
            # TODO adapter for control messages
        }
    )
    return RouteByTopicAdapter(
        routes={
            beam_monitor_topic(instrument): monitors,
            beamlime_command_topic(instrument): BeamlimeCommandsAdapter(),
        },
    )


def make_monitor_service_builder(
    *, instrument: str, log_level: int = logging.INFO
) -> DataServiceBuilder:
    config = {}
    config_handler = ConfigHandler(config=config)
    handler_factory = CommonHandlerFactory(
        handler_cls=create_monitor_data_handler, config=config
    )
    builder = DataServiceBuilder(
        instrument=instrument,
        name='monitor_data',
        log_level=log_level,
        adapter=make_monitor_data_adapter(instrument=instrument),
        handler_factory=handler_factory,
    )
    builder.add_handler(ConfigHandler.message_key(instrument), config_handler)
    return builder


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    log_level: int = logging.INFO,
) -> NoReturn:
    config = load_config(namespace=config_names.monitor_data, env='')
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()

    builder = make_monitor_service_builder(instrument=instrument, log_level=log_level)

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        data_consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='monitor_data',
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
