# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
from typing import NoReturn

from beamlime import Service
from beamlime.config.stream_mapping import get_stream_mapping
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.monitor_data_handler import MonitorHandlerFactory
from beamlime.kafka.routes import beam_monitor_route
from beamlime.service_factory import DataServiceBuilder, run_data_service


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Monitor Data Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


def make_monitor_service_builder(
    *, instrument: str, dev: bool = True, log_level: int = logging.INFO
) -> DataServiceBuilder:
    service_name = 'monitor_data'
    config_handler = ConfigHandler(service_name=service_name)
    handler_factory = MonitorHandlerFactory(config_registry=config_handler)
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    builder = DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        routes=beam_monitor_route(stream_mapping),
        handler_factory=handler_factory,
    )
    builder.add_handler(CONFIG_STREAM_ID, config_handler)
    return builder


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_data_service(
        **vars(parser.parse_args()), make_builder=make_monitor_service_builder
    )


if __name__ == "__main__":
    main()
