# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that processes detector event data into 2-D data for plotting."""

import logging
from typing import NoReturn

from ess.livedata.config import instrument_registry
from ess.livedata.config.instruments import get_config
from ess.livedata.config.streams import get_stream_mapping
from ess.livedata.core.orchestrating_processor import OrchestratingProcessor
from ess.livedata.handlers.detector_data_handler import DetectorHandlerFactory
from ess.livedata.kafka.routes import RoutingAdapterBuilder
from ess.livedata.service_factory import DataServiceBuilder, DataServiceRunner


def make_detector_service_builder(
    *, instrument: str, dev: bool = True, log_level: int = logging.INFO
) -> DataServiceBuilder:
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    adapter = (
        RoutingAdapterBuilder(stream_mapping=stream_mapping)
        .with_detector_route()
        .with_beamlime_config_route()
        .build()
    )
    _ = get_config(instrument)  # Load the module to register the instrument
    service_name = 'detector_data'
    handler_factory = DetectorHandlerFactory(instrument=instrument_registry[instrument])
    return DataServiceBuilder(
        instrument=instrument,
        name=service_name,
        log_level=log_level,
        adapter=adapter,
        handler_factory=handler_factory,
        processor_cls=OrchestratingProcessor,
    )


def main() -> NoReturn:
    runner = DataServiceRunner(
        pretty_name='Detector Data', make_builder=make_detector_service_builder
    )
    runner.run()


if __name__ == "__main__":
    main()
