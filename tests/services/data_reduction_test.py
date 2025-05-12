# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import logging

import pytest

from beamlime.config import models
from beamlime.config.instruments import available_instruments
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigUpdate
from beamlime.services.data_reduction import make_reduction_service_builder
from tests.helpers.beamlime_app import BeamlimeApp


def _get_workflow_by_name(
    workflow_specs: models.WorkflowSpecs, name: str
) -> tuple[str, models.WorkflowSpec]:
    for wid, spec in workflow_specs.workflows.items():
        if spec.name == name:
            return wid, spec
    raise ValueError(f"Workflow {name} not found in specs")


def make_reduction_app(instrument: str) -> BeamlimeApp:
    builder = make_reduction_service_builder(instrument=instrument)
    return BeamlimeApp.from_service_builder(builder)


@pytest.mark.parametrize("instrument", available_instruments())
def test_publishes_workflow_specs_on_startup(instrument: str) -> None:
    app = make_reduction_app(instrument=instrument)
    sink = app.sink

    assert len(sink.messages) == 1
    message = sink.messages[0]
    assert message.stream == CONFIG_STREAM_ID
    assert isinstance(message.value, ConfigUpdate)
    assert isinstance(message.value.config_key, models.ConfigKey)
    assert isinstance(message.value.value, models.WorkflowSpecs)
    if instrument in ('bifrost', 'dummy', 'loki'):
        assert len(message.value.value.workflows) > 0


@pytest.mark.parametrize("instrument", ['bifrost', 'dummy'])
def test_can_configure_and_stop_workflow_with_detector(
    instrument: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument=instrument)
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_name = {'bifrost': 'spectrum-view', 'dummy': 'Total counts'}[instrument]
    workflow_id, spec = _get_workflow_by_name(workflow_specs, workflow_name)
    sink.messages.clear()  # Clear the initial message

    # Assume workflow is runnable for all source names
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())

    app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    # Events before workflow config was published should not be included
    assert sink.messages[0].value.values.sum() == 2000
    service.step()
    assert len(sink.messages) == 1
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2
    assert sink.messages[1].value.values.sum() == 5000

    # More events but the same time, should not publish again
    app.publish_events(size=1000, time=4)
    service.step()
    assert len(sink.messages) == 2

    # Later time should publish again, including the previous events with duplicate time
    app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 3
    assert sink.messages[2].value.values.sum() == 7000

    # Stop workflow
    app.publish_config_message(key=config_key, value=None)
    app.publish_events(size=1000, time=10)
    service.step()
    app.publish_events(size=1000, time=20)
    service.step()
    assert len(sink.messages) == 3


@pytest.mark.parametrize("instrument", ['loki'])
def test_can_configure_and_stop_workflow_with_detector_and_monitors(
    instrument: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument=instrument)
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'I(Q)')
    sink.messages.clear()  # Clear the initial message

    # Assume workflow is runnable for all source names
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())

    app.publish_events(size=2000, time=2)
    service.step()
    # No monitor data yet, so the workflow was not able to produce a result yet
    assert len(sink.messages) == 0

    # Monitor messages on their own do not trigger the workflow...
    app.publish_monitor_events(size=2000, time=3)
    service.step()
    assert len(sink.messages) == 0

    # ... trigger by detector events instead
    app.publish_events(size=0, time=3)
    service.step()
    assert len(sink.messages) == 1

    # Once we have monitor data the worklow works even if only detector data comes in.
    # There is currently no "smart" mechanism to check if we have monitor and detector
    # data for the same time (intervals).
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2

    # More events but the same time, should not publish again
    app.publish_events(size=1000, time=4)
    service.step()
    assert len(sink.messages) == 2

    # Later time should publish again
    app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 3

    # Stop workflow
    app.publish_config_message(key=config_key, value=None)
    app.publish_events(size=1000, time=10)
    service.step()
    app.publish_events(size=1000, time=20)
    service.step()
    assert len(sink.messages) == 3


def test_can_clear_workflow_via_config(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')

    app.publish_events(size=1000, time=0)
    service.step()

    # Assume workflow is runnable for all source names
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    app.publish_events(size=2000, time=1)
    app.publish_events(size=3000, time=2)
    service.step()
    assert len(sink.messages) == 2
    assert sink.messages[-1].value.values.sum() == 5000

    config_key = models.ConfigKey(key="start_time")
    model = models.StartTime(value=5, unit='s')
    app.publish_config_message(key=config_key, value=model.model_dump())

    app.publish_events(size=1000, time=4)
    service.step()
    assert sink.messages[-1].value.values.sum() == 1000
    app.publish_events(size=1000, time=6)
    service.step()
    assert sink.messages[-1].value.values.sum() == 2000

    config_key = models.ConfigKey(key="start_time")
    model = models.StartTime(value=8, unit='s')
    app.publish_config_message(key=config_key, value=model.model_dump())

    app.publish_events(size=100, time=9)
    service.step()
    assert sink.messages[-1].value.values.sum() == 100
    app.publish_events(size=100, time=10)
    service.step()
    assert sink.messages[-1].value.values.sum() == 200


def test_service_can_recover_after_bad_workflow_id_was_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')
    sink.messages.clear()  # Clear the initial message

    # Assume workflow is runnable for all source names
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    bad_workflow_id = models.WorkflowConfig(
        identifier='abcde12345',  # Invalid workflow ID
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_workflow_id.model_dump())

    app.publish_events(size=2000, time=2)
    service.step()
    service.step()
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 0  # Workflow not started

    bad_param_value = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())
    app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 1  # Service recovered and started the workflow


def test_service_can_recover_after_bad_workflow_param_was_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')
    sink.messages.clear()  # Clear the initial message

    # Assume workflow is runnable for all source names
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    defaults = {param.name: param.default for param in spec.parameters}
    defaults['does_not_exist'] = 1
    bad_param_value = models.WorkflowConfig(identifier=workflow_id, values=defaults)
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())

    app.publish_events(size=2000, time=2)
    service.step()
    service.step()
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 0  # Workflow not started

    bad_param_value = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())
    app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 1  # Service recovered and started the workflow


def test_active_workflow_keeps_running_when_bad_workflow_id_or_params_were_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')
    sink.messages.clear()  # Clear the initial message

    # Start a valid workflow first
    config_key = models.ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    service.step()

    # Add events and verify workflow is running
    app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000

    # Try to set an invalid workflow ID
    bad_workflow_id = models.WorkflowConfig(
        identifier='abcde12345',  # Invalid workflow ID
        values={},
    )
    app.publish_config_message(key=config_key, value=bad_workflow_id.model_dump())

    # Add more events and verify the original workflow is still running
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2
    assert sink.messages[1].value.values.sum() == 5000

    # Try to set a workflow with invalid parameters
    defaults = {param.name: param.default for param in spec.parameters}
    defaults['does_not_exist'] = 1
    bad_param_value = models.WorkflowConfig(identifier=workflow_id, values=defaults)
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())

    # Add more events and verify the original workflow is still running
    app.publish_events(size=1000, time=6)
    service.step()
    assert len(sink.messages) == 3
    assert sink.messages[2].value.values.sum() == 6000


@pytest.mark.parametrize(
    "data_before_config",
    [False, True],
    ids=["config_before_data", "data_before_config"],
)
@pytest.mark.parametrize(
    "all_source_names", [False, True], ids=["specific_source", "all_sources"]
)
def test_workflow_starts_with_specific_or_global_source_name(
    data_before_config: bool,
    all_source_names: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')
    source_name = None if all_source_names else spec.source_names[0]
    sink.messages.clear()  # Clear the initial message

    # This branch ensures that the service configures the workflow even for source names
    # it has not "seen" yet.
    if data_before_config:
        app.publish_events(size=1000, time=0)
        service.step()
        assert len(sink.messages) == 0

    config_key = models.ConfigKey(
        source_name=source_name, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = models.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    # Process config message before data arrives. Without calling step() the order of
    # processing of config vs data messages is not guaranteed.
    service.step()

    app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    # Events before workflow config was published should not be included
    assert sink.messages[0].value.values.sum() == 2000
