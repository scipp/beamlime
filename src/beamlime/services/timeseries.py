# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes logdata into timeseries for plotting."""

import argparse
import logging
from collections.abc import Mapping
from typing import Any, NoReturn

from beamlime import Service
from beamlime.handlers.timeseries_handler import LogdataHandlerFactory
from beamlime.kafka.routes import logdata_route
from beamlime.service_factory import DataServiceBuilder, run_data_service


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
    dev: bool = True,
    log_level: int = logging.INFO,
    attribute_registry: Mapping[str, Mapping[str, Any]] | None = None,
) -> DataServiceBuilder:
    handler_factory = LogdataHandlerFactory(
        instrument=instrument, attribute_registry=attribute_registry, config={}
    )
    return DataServiceBuilder(
        instrument=instrument,
        name='timeseries',
        log_level=log_level,
        routes=logdata_route(instrument),
        handler_factory=handler_factory,
    )


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_data_service(
        **vars(parser.parse_args()), make_builder=make_timeseries_service_builder
    )


if __name__ == "__main__":
    main()
