# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
from typing import NoReturn

import scipp as sc

from beamlime import CommonHandlerFactory, Handler, Message, Service, StreamProcessor
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.source import KafkaMessageSource
from beamlime.sinks import PlotToPngSink


class IdentityHandler(Handler[sc.DataArray, sc.DataArray]):
    def handle(self, message: Message[sc.DataArray]) -> list[Message[sc.DataArray]]:
        # We know the message is not put back into Kafka, so we can keep the key
        return [message]


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    handler_config = {}
    consumer_config = load_config(namespace=config_names.reduced_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    config = load_config(namespace=config_names.visualization, env='')
    with kafka_consumer.make_consumer_from_config(
        topics=config['topics'],
        config={**consumer_config, **kafka_downstream_config},
        instrument=instrument,
        group='visualization',
    ) as consumer:
        processor = StreamProcessor(
            source=AdaptingMessageSource(
                source=KafkaMessageSource(consumer=consumer),
                adapter=ChainedAdapter(
                    first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
                ),
            ),
            sink=PlotToPngSink(),
            handler_factory=CommonHandlerFactory(
                config=handler_config, handler_cls=IdentityHandler
            ),
        )
        service = Service(
            processor=processor,
            name=f'{instrument}_plot_da00_to_png',
            log_level=log_level,
        )
        service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Plot da00 data arrays to PNG')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
