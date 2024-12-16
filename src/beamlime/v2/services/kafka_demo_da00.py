# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
from typing import NoReturn

from beamlime.v2 import ConfigManager, HandlerRegistry, Service, StreamProcessor
from beamlime.v2.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.v2.kafka import consumer as kafka_consumer
from beamlime.v2.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.v2.kafka.sink import KafkaSink
from beamlime.v2.kafka.source import KafkaMessageSource
from beamlime.v2.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Kafka Demo da00 Service')
    parser.add_argument(
        '--sink',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def run_service(*, sink_type: str) -> NoReturn:
    service_name = 'local_demo_da00'
    initial_config = {
        'sliding_window_seconds': 5,
        'kafka.bootstrap.servers': 'localhost:9092',
    }

    config_manager = ConfigManager(
        bootstrap_servers='localhost:9092',
        service_name=service_name,
        initial_config=initial_config,
    )
    consumer = kafka_consumer.make_bare_consumer(
        topics=['monitors'], config=kafka_consumer.kafka_config
    )
    producer_config = {"bootstrap.servers": "localhost:9092"}

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=producer_config)
    else:
        sink = PlotToPngSink()

    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        ),
        sink=sink,
        handler_registry=HandlerRegistry(
            config=config_manager, handler_cls=create_monitor_data_handler
        ),
    )
    service = Service(
        config_manager=config_manager, processor=processor, name=service_name
    )
    service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    args = parser.parse_args()
    run_service(sink_type=args.sink)


if __name__ == "__main__":
    main()
