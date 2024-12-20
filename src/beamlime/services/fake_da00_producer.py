# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
from dataclasses import replace
from typing import NoReturn

import scipp as sc

from beamlime import Handler, HandlerRegistry, Message, Service, StreamProcessor
from beamlime.config.config_loader import load_config
from beamlime.kafka.message_adapter import AdaptingMessageSource, MessageAdapter
from beamlime.kafka.sink import KafkaSink
from beamlime.services.fake_ev44_producer import FakeMonitorSource


class EventsToHistogramAdapter(
    MessageAdapter[Message[sc.Variable], Message[sc.DataArray]]
):
    def __init__(self, toa: sc.Variable):
        self._toa = toa

    def adapt(self, message: Message[sc.Variable]) -> Message[sc.DataArray]:
        return replace(message, value=message.value.hist({self._toa.dim: self._toa}))


class IdentityHandler(Handler[sc.DataArray, sc.DataArray]):
    def handle(self, message: Message[sc.DataArray]) -> list[Message[sc.DataArray]]:
        # We know the message does not originate from Kafka, so we can keep the key
        return [message]


def run_service(*, instrument: str, log_level: int = logging.INFO) -> NoReturn:
    service_name = f'{instrument}_fake_da00_producer'
    config = load_config(namespace='fake_da00')
    processor = StreamProcessor(
        source=AdaptingMessageSource(
            source=FakeMonitorSource(instrument=instrument),
            adapter=EventsToHistogramAdapter(
                toa=sc.linspace('toa', 0, 71_000_000, num=100, unit='ns')
            ),
        ),
        sink=KafkaSink(kafka_config=config['producer']['kafka']),
        handler_registry=HandlerRegistry(config={}, handler_cls=IdentityHandler),
    )
    service = Service(
        config=config['service'],
        processor=processor,
        name=service_name,
        log_level=log_level,
    )
    service.start()


def main() -> NoReturn:
    parser = Service.setup_arg_parser('Fake that publishes random da00 monitor data')
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
