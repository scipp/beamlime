# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
from typing import Literal, NoReturn

from beamlime import ConfigManager, HandlerRegistry, Service, StreamProcessor
from beamlime.config.config_loader import load_config
from beamlime.handlers.monitor_data_handler import (
    create_monitor_data_handler,
    create_monitor_event_data_handler,
)
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    Ev44ToMonitorEventsAdapter,
    KafkaToDa00Adapter,
    KafkaToEv44Adapter,
)
from beamlime.kafka.sink import KafkaSink
from beamlime.kafka.source import KafkaMessageSource
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Kafka Demo da00/ev44 Service')
    parser.add_argument(
        '--sink',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    parser.add_argument(
        '--mode',
        choices=['ev44', 'da00'],
        required=True,
        help='Select mode: ev44 or da00',
    )
    return parser


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    mode: Literal['ev44', 'da00'],
    log_level: int = logging.INFO,
) -> NoReturn:
    service_name = f'{instrument}_monitor_data_demo'
    service_config = {}
    initial_config = {'sliding_window_seconds': 5}

    config = load_config(namespace='monitor_data')
    control_config = config['control']
    consumer_config = config['consumer']
    producer_config = config['producer']

    config_manager = ConfigManager(config=control_config, initial_config=initial_config)
    consumer = kafka_consumer.make_bare_consumer(
        topics=[f'{instrument}_{topic}' for topic in consumer_config['topics']],
        config=consumer_config['kafka'],
    )

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=producer_config['kafka'])
    else:
        sink = PlotToPngSink()
    if mode == 'ev44':
        adapter = ChainedAdapter(
            first=KafkaToEv44Adapter(), second=Ev44ToMonitorEventsAdapter()
        )
        handler_cls = create_monitor_event_data_handler
    else:
        adapter = ChainedAdapter(
            first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
        )
        handler_cls = create_monitor_data_handler

    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer), adapter=adapter
        ),
        sink=sink,
        handler_registry=HandlerRegistry(
            config=config_manager, handler_cls=handler_cls
        ),
    )
    service = Service(
        config=service_config,
        config_manager=config_manager,
        processor=processor,
        name=service_name,
        log_level=log_level,
    )
    service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    args = parser.parse_args()
    run_service(
        sink_type=args.sink,
        instrument=args.instrument,
        mode=args.mode,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
