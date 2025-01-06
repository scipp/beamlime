# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
from typing import Literal, NoReturn

from beamlime import ConfigSubscriber, HandlerRegistry, Service, StreamProcessor
from beamlime.config.config_loader import load_config
from beamlime.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    Ev44ToMonitorEventsAdapter,
    KafkaToDa00Adapter,
    KafkaToEv44Adapter,
    RoutingAdapter,
)
from beamlime.kafka.sink import KafkaSink
from beamlime.kafka.source import KafkaMessageSource
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


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    log_level: int = logging.INFO,
) -> NoReturn:
    service_name = f'{instrument}_monitor_data_demo'
    initial_config = {'sliding_window_seconds': 5}

    config = load_config(namespace='monitor_data')
    control_consumer = kafka_consumer.make_consumer_from_config(
        config=config['control'], instrument=instrument, group='beamlime_control'
    )
    config_subscriber = ConfigSubscriber(
        consumer=control_consumer, config=initial_config
    )
    consumer = kafka_consumer.make_consumer_from_config(
        config=config['consumer'], instrument=instrument, group='monitor_data'
    )

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=config['producer']['kafka'])
    else:
        sink = PlotToPngSink()
    adapter = RoutingAdapter(
        routes={
            'ev44': ChainedAdapter(
                first=KafkaToEv44Adapter(), second=Ev44ToMonitorEventsAdapter()
            ),
            'da00': ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        }
    )
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer), adapter=adapter
        ),
        sink=sink,
        handler_registry=HandlerRegistry(
            config=config_subscriber, handler_cls=create_monitor_data_handler
        ),
    )
    service = Service(
        config=config['service'],
        children=[config_subscriber],
        processor=processor,
        name=service_name,
        log_level=log_level,
    )
    service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
