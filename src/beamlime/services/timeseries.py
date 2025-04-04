# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes logdata into timeseries for plotting."""

import logging
from collections.abc import Mapping
from typing import Any, NoReturn

from beamlime.handlers.timeseries_handler import LogdataHandlerFactory
from beamlime.kafka.routes import logdata_route
from beamlime.service_factory import DataServiceBuilder, DataServiceRunner


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
    runner = DataServiceRunner(
        pretty_name='Logdata to Timeseries',
        make_builder=make_timeseries_service_builder,
    )
    runner.run()


if __name__ == "__main__":
    main()
