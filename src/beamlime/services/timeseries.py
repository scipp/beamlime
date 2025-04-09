# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes logdata into timeseries for plotting."""

import logging
from collections.abc import Mapping
from typing import Any, NoReturn

from beamlime.config.streams import get_stream_mapping
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.timeseries_handler import LogdataHandlerFactory
from beamlime.kafka.routes import RoutingAdapterBuilder
from beamlime.service_factory import DataServiceBuilder, DataServiceRunner


def make_timeseries_service_builder(
    *,
    instrument: str,
    dev: bool = True,
    log_level: int = logging.INFO,
    attribute_registry: Mapping[str, Mapping[str, Any]] | None = None,
) -> DataServiceBuilder:
    service_name = 'timeseries'
    config_handler = ConfigHandler(service_name=service_name)
    handler_factory = LogdataHandlerFactory(
        instrument=instrument,
        attribute_registry=attribute_registry,
        config_registry=config_handler,
    )
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    adapter = (
        RoutingAdapterBuilder(stream_mapping=stream_mapping)
        .with_logdata_route()
        .with_beamlime_config_route()
        .build()
    )
    builder = DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        adapter=adapter,
        handler_factory=handler_factory,
    )
    builder.add_handler(CONFIG_STREAM_ID, config_handler)
    return builder


def main() -> NoReturn:
    runner = DataServiceRunner(
        pretty_name='Logdata to Timeseries',
        make_builder=make_timeseries_service_builder,
    )
    runner.run()


if __name__ == "__main__":
    main()
