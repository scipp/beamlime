# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that runs a data reduction workflow."""

import argparse
import logging
from contextlib import ExitStack
from typing import Literal, NoReturn

from beamlime import Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.raw_detectors import get_config
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.data_reduction_handler import ReductionHandlerFactory
from beamlime.handlers.workflow_manager import WorkflowManager
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import RouteByTopicAdapter
from beamlime.kafka.routes import (
    beam_monitor_route,
    beamlime_config_route,
    detector_route,
    logdata_route,
)
from beamlime.kafka.sink import KafkaSink, UnrollingSinkAdapter
from beamlime.kafka.source import MultiConsumer
from beamlime.service_factory import DataServiceBuilder
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Data Reduction Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def make_reduction_service_builder(
    *, instrument: str, log_level: int = logging.INFO
) -> DataServiceBuilder:
    adapter = RouteByTopicAdapter(
        routes={
            **beam_monitor_route(instrument),
            **detector_route(instrument),
            **logdata_route(instrument),
            **beamlime_config_route(instrument),
        }
    )
    instrument_config = get_config(instrument)
    workflow_manager = WorkflowManager(
        source_names=instrument_config.source_names,
        source_to_key=instrument_config.source_to_key,
    )
    service_name = 'data_reduction'
    config_handler = ConfigHandler(service_name=service_name)
    config_handler.register_action(
        key='workflow_name', action=workflow_manager.set_workflow_from_name
    )
    handler_factory = ReductionHandlerFactory(
        config_registry=config_handler,
        workflow_manager=workflow_manager,
        f144_attribute_registry=instrument_config.f144_attribute_registry,
    )
    builder = DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        adapter=adapter,
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
    config = load_config(namespace=config_names.data_reduction, env='')
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()
    sink = UnrollingSinkAdapter(sink)

    builder = make_reduction_service_builder(instrument=instrument, log_level=log_level)

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        data_consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='data_reduction',
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
