# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Test for the monitor data service.

Note that this uses mostly the same logic as the data reduction service, so the tests
are similar to those in `tests/services/data_reduction_test.py`. Many tests are not
duplicated here.
"""

import logging

import pytest

from ess.livedata.config import instrument_registry, workflow_spec
from ess.livedata.config.models import ConfigKey
from ess.livedata.core.job import JobAction, JobCommand
from ess.livedata.services.monitor_data import make_monitor_service_builder
from tests.helpers.beamlime_app import LivedataApp


def _get_workflow_from_registry(
    instrument: str,
) -> tuple[str, workflow_spec.WorkflowSpec]:
    # Currently only one workflow for monitor data, so we can hardcode the name.
    namespace = 'monitor_data'
    instrument_config = instrument_registry[instrument]
    workflow_registry = instrument_config.workflow_factory
    for wid, spec in workflow_registry.items():
        if spec.namespace == namespace:
            return wid, spec
    raise ValueError(f"Namespace {namespace} not found in specs")


def make_monitor_app(instrument: str) -> LivedataApp:
    builder = make_monitor_service_builder(instrument=instrument)
    return LivedataApp.from_service_builder(builder)


first_monitor_source_name = {
    'dummy': 'monitor1',
    'dream': 'monitor1',
    'bifrost': 'monitor1',
    'loki': 'monitor1',
}


@pytest.mark.parametrize("instrument", ['bifrost', 'dummy', 'dream', 'loki'])
def test_can_configure_and_stop_monitor_workflow(
    instrument: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = make_monitor_app(instrument)
    sink = app.sink
    service = app.service
    workflow_id, _ = _get_workflow_from_registry(instrument)

    source_name = first_monitor_source_name[instrument]
    config_key = ConfigKey(
        source_name=source_name, service_name="monitor_data", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(identifier=workflow_id)
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    service.step()
    assert len(sink.messages) == 1  # Workflow status message
    sink.messages.clear()

    app.publish_monitor_events(size=2000, time=2)
    service.step()
    # Each workflow call returns two results, cumulative and current
    assert len(sink.messages) == 2
    assert sink.messages[-2].value.values.sum() == 2000
    assert sink.messages[-1].value.values.sum() == 2000
    # No data -> no data published
    service.step()
    assert len(sink.messages) == 2

    app.publish_monitor_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 4
    assert sink.messages[-2].value.values.sum() == 5000
    assert sink.messages[-1].value.values.sum() == 3000

    # More events but the same time
    app.publish_monitor_events(size=1000, time=4)
    # Later time
    app.publish_monitor_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 6
    assert sink.messages[-2].value.values.sum() == 7000
    assert sink.messages[-1].value.values.sum() == 2000

    # Stop workflow
    command = JobCommand(action=JobAction.stop)
    config_key = ConfigKey(key=command.key)
    stop = command.model_dump()
    app.publish_config_message(key=config_key, value=stop)
    app.publish_monitor_events(size=1000, time=10)
    service.step()
    app.publish_monitor_events(size=1000, time=20)
    service.step()
    assert len(sink.messages) == 6
