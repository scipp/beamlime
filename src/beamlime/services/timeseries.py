# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes logdata into timeseries for plotting."""

import argparse
import logging
from collections.abc import Mapping
from contextlib import ExitStack
from functools import partial
from typing import Any, Literal, NoReturn

from beamlime import Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.handlers.timeseries_handler import LogdataHandlerFactory
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    ChainedAdapter,
    F144ToLogDataAdapter,
    KafkaToF144Adapter,
)
from beamlime.kafka.sink import KafkaSink, UnrollingSinkAdapter
from beamlime.service_factory import DataServiceBuilder
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Timeseries Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def make_timeseries_service_builder(
    *,
    instrument: str,
    log_level: int = logging.INFO,
    attribute_registry: Mapping[str, Mapping[str, Any]] | None = None,
) -> DataServiceBuilder:
    adapter = ChainedAdapter(first=KafkaToF144Adapter(), second=F144ToLogDataAdapter())
    handler_factory_cls = partial(
        LogdataHandlerFactory,
        instrument=instrument,
        attribute_registry=attribute_registry,
    )
    return DataServiceBuilder(
        instrument=instrument,
        name='timeseries',
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
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()
    sink = UnrollingSinkAdapter(sink)

    builder = make_timeseries_service_builder(
        instrument=instrument, log_level=log_level
    )

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=('motion',),
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='timeseries',
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
