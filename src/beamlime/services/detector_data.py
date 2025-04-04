# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes detector event data into 2-D data for plotting."""

import logging
from typing import NoReturn

from beamlime.config.stream_mapping import get_stream_mapping
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory
from beamlime.kafka.routes import detector_route
from beamlime.service_factory import DataServiceBuilder, DataServiceRunner


def make_detector_service_builder(
    *, instrument: str, dev: bool = True, log_level: int = logging.INFO
) -> DataServiceBuilder:
    service_name = 'detector_data'
    config_handler = ConfigHandler(service_name=service_name)
    handler_factory = DetectorHandlerFactory(
        instrument=instrument, config_registry=config_handler
    )
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    builder = DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        routes=detector_route(stream_mapping),
        handler_factory=handler_factory,
    )
    builder.add_handler(CONFIG_STREAM_ID, config_handler)
    return builder


def main() -> NoReturn:
    runner = DataServiceRunner(
        pretty_name='Detector Data', make_builder=make_detector_service_builder
    )
    runner.run()


if __name__ == "__main__":
    main()
