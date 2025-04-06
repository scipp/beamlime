# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that runs a data reduction workflow."""

import logging
from typing import NoReturn

from beamlime.config.raw_detectors import get_config
from beamlime.config.streams import get_stream_mapping
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigHandler
from beamlime.handlers.data_reduction_handler import ReductionHandlerFactory
from beamlime.handlers.workflow_manager import WorkflowManager
from beamlime.kafka.routes import RoutingAdapterBuilder
from beamlime.service_factory import DataServiceBuilder, DataServiceRunner


def make_reduction_service_builder(
    *, instrument: str, dev: bool = True, log_level: int = logging.INFO
) -> DataServiceBuilder:
    stream_mapping = get_stream_mapping(instrument=instrument, dev=dev)
    adapter = (
        RoutingAdapterBuilder(stream_mapping=stream_mapping)
        .with_beam_monitor_route()
        .with_detector_route()
        .with_logdata_route()
        .with_beamlime_config_route()
        .build()
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
    builder.add_handler(CONFIG_STREAM_ID, config_handler)
    return builder


def main() -> NoReturn:
    runner = DataServiceRunner(
        pretty_name='Data Reduction', make_builder=make_reduction_service_builder
    )
    runner.run()


if __name__ == "__main__":
    main()
