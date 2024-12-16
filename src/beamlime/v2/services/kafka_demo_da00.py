# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import NoReturn

from beamlime.v2 import HandlerRegistry, Service, StreamProcessor
from beamlime.v2.handlers.monitor_data_handler import create_monitor_data_handler
from beamlime.v2.kafka import consumer as kafka_consumer
from beamlime.v2.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.v2.kafka.source import KafkaMessageSource
from beamlime.v2.sinks import PlotToPngSink


def main() -> NoReturn:
    handler_config = {'sliding_window_seconds': 5}
    service_config = {}
    consumer = kafka_consumer.make_bare_consumer(
        topics=['monitors'], config=kafka_consumer.kafka_config
    )
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        ),
        sink=PlotToPngSink(),
        handler_registry=HandlerRegistry(
            config=handler_config, handler_cls=create_monitor_data_handler
        ),
    )
    service = Service(
        config=service_config, processor=processor, name="local_demo_da00"
    )
    service.start()


if __name__ == "__main__":
    main()
