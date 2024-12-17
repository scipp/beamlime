# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
from typing import NoReturn

from beamlime import ConfigManager, HandlerRegistry, Service, StreamProcessor
from beamlime.config.config_loader import load_config
from beamlime.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.sink import KafkaSink
from beamlime.kafka.source import KafkaMessageSource
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Kafka Demo da00 Service')
    parser.add_argument(
        '--sink',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def run_service(*, sink_type: str, instrument: str) -> NoReturn:
    # What config do we have?
    # Note: Only the handler config can be updated via Kafka (ConfigSubscriber)
    # - Service (name, polling behavior/interval)
    # - JSON files for consumer and producer configs
    #   - Server could be different for consumer and producer
    #   - Topics to consume
    # - Handler config
    #   - Handler itself (update interval)
    #   - Preprocessing (TOF range, bin count)
    #   - Accumulator config (start, sliding window,)
    service_name = 'local_demo_da00'
    service_config = {}
    initial_config = {
        'sliding_window_seconds': 5,
    }

    control_config = load_config(namespace='monitor_data', kind='control')
    consumer_config = load_config(namespace='monitor_data', kind='consumer')
    producer_config = load_config(namespace='monitor_data', kind='producer')

    config_manager = ConfigManager(config=control_config, initial_config=initial_config)
    consumer = kafka_consumer.make_bare_consumer(
        topics=[f'{instrument}_{topic}' for topic in consumer_config['topics']],
        config=consumer_config['kafka'],
    )

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=producer_config['kafka'])
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
        config=service_config,
        config_manager=config_manager,
        processor=processor,
        name=service_name,
    )
    service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    args = parser.parse_args()
    run_service(sink_type=args.sink, instrument=args.instrument)


if __name__ == "__main__":
    main()
