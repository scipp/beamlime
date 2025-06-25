# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import logging

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime.config import workflow_spec
from beamlime.config.instruments import available_instruments
from beamlime.config.models import ConfigKey, StartTime
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.handlers.config_handler import ConfigUpdate
from beamlime.services.data_reduction import make_reduction_service_builder
from tests.helpers.beamlime_app import BeamlimeApp


def _get_workflow_by_name(
    workflow_specs: workflow_spec.WorkflowSpecs, name: str
) -> tuple[str, workflow_spec.WorkflowSpec]:
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
    assert isinstance(message.value.config_key, ConfigKey)
    assert isinstance(message.value.value, workflow_spec.WorkflowSpecs)
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
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    service.step()
    assert len(sink.messages) == 1  # Workflow status message
    sink.messages.clear()

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
    assert len(sink.messages) == 3 + 1  # + 1 for the stop message


@pytest.mark.parametrize("instrument", ['dream', 'loki'])
@pytest.mark.skip(reason="DREAM workflow  is currently broken, requires upstream fixes")
def test_can_configure_and_stop_workflow_with_detector_and_monitors(
    instrument: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = make_reduction_app(instrument=instrument)
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_name = {
        'dream': 'Powder reduction',
        'loki': 'I(Q)',
    }[instrument]
    n_target = {'dream': 2, 'loki': 1}[instrument]
    workflow_id, spec = _get_workflow_by_name(workflow_specs, workflow_name)
    sink.messages.clear()  # Clear the initial message

    # Assume workflow is runnable for all source names
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    service.step()
    n_source = len(sink.messages)
    sink.messages.clear()  # Clear the workflow status message(s), one per source name.

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
    assert len(sink.messages) == 1 * n_target

    # Once we have monitor data the worklow works even if only detector data comes in.
    # There is currently no "smart" mechanism to check if we have monitor and detector
    # data for the same time (intervals).
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2 * n_target

    # More events but the same time, should not publish again
    app.publish_events(size=1000, time=4)
    service.step()
    assert len(sink.messages) == 2 * n_target

    # Later time should publish again
    app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 3 * n_target

    # Stop workflow
    app.publish_config_message(key=config_key, value=None)
    app.publish_events(size=1000, time=10)
    service.step()
    app.publish_events(size=1000, time=20)
    service.step()
    assert len(sink.messages) == 3 * n_target + n_source  # + n_source for stop message


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
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    app.publish_events(size=2000, time=1)
    app.publish_events(size=3000, time=2)
    service.step()
    assert len(sink.messages) == 2 + 1  # + 1 for the workflow start message
    assert sink.messages[-1].value.values.sum() == 5000

    config_key = ConfigKey(key="start_time")
    model = StartTime(value=5, unit='s')
    app.publish_config_message(key=config_key, value=model.model_dump())

    app.publish_events(size=1000, time=4)
    service.step()
    assert sink.messages[-1].value.values.sum() == 1000
    app.publish_events(size=1000, time=6)
    service.step()
    assert sink.messages[-1].value.values.sum() == 2000

    config_key = ConfigKey(key="start_time")
    model = StartTime(value=8, unit='s')
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
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    bad_workflow_id = workflow_spec.WorkflowConfig(
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

    assert len(sink.messages) == 1  # Workflow not started, just an error message
    status = sink.messages[0].value.value
    assert status.status == workflow_spec.WorkflowStatusType.STARTUP_ERROR
    sink.messages.clear()  # Clear the error message

    bad_param_value = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())
    app.publish_events(size=1000, time=5)
    service.step()
    # Service recovered and started the workflow, get status and data
    assert len(sink.messages) == 2


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
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    defaults = {param.name: param.default for param in spec.parameters}
    defaults['does_not_exist'] = 1
    bad_param_value = workflow_spec.WorkflowConfig(
        identifier=workflow_id, values=defaults
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())

    app.publish_events(size=2000, time=2)
    service.step()
    service.step()
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 1  # Workflow not started, just an error message

    bad_param_value = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())
    app.publish_events(size=1000, time=5)
    service.step()
    # Service recovered and started the workflow, get 2*status and data
    assert len(sink.messages) == 3


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
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    service.step()
    sink.messages.clear()  # Clear the workflow status message

    # Add events and verify workflow is running
    app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000

    # Try to set an invalid workflow ID
    bad_workflow_id = workflow_spec.WorkflowConfig(
        identifier='abcde12345',  # Invalid workflow ID
        values={},
    )
    app.publish_config_message(key=config_key, value=bad_workflow_id.model_dump())

    # Add more events and verify the original workflow is still running
    app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2 + 1  # + 1 for the workflow status message
    assert sink.messages[2].value.values.sum() == 5000

    # Try to set a workflow with invalid parameters
    defaults = {param.name: param.default for param in spec.parameters}
    defaults['does_not_exist'] = 1
    bad_param_value = workflow_spec.WorkflowConfig(
        identifier=workflow_id, values=defaults
    )
    app.publish_config_message(key=config_key, value=bad_param_value.model_dump())

    # Add more events and verify the original workflow is still running
    app.publish_events(size=1000, time=6)
    service.step()
    assert len(sink.messages) == 3 + 2  # + 2 for the workflow status messages
    assert sink.messages[4].value.values.sum() == 6000


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

    config_key = ConfigKey(
        source_name=source_name, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    # Process config message before data arrives. Without calling step() the order of
    # processing of config vs data messages is not guaranteed.
    service.step()
    sink.messages.clear()  # Clear workflow status message

    app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    # Events before workflow config was published should not be included
    assert sink.messages[0].value.values.sum() == 2000


@pytest.fixture
def configured_dummy_reduction() -> BeamlimeApp:
    app = make_reduction_app(instrument='dummy')
    sink = app.sink
    service = app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = _get_workflow_by_name(workflow_specs, 'Total counts')
    sink.messages.clear()  # Clear the initial message

    config_key = ConfigKey(
        source_name='panel_0', service_name="data_reduction", key="workflow_config"
    )
    workflow_config = workflow_spec.WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    # Trigger workflow start
    app.publish_config_message(key=config_key, value=workflow_config.model_dump())
    # Process config message before data arrives. Without calling step() the order of
    # processing of config vs data messages is not guaranteed.
    service.step()
    sink.messages.clear()  # Clear workflow start message
    return app


@pytest.mark.parametrize('n_msg', [0, 1, 10, 100, 1_234])
@pytest.mark.parametrize('n_event', [0, 1, 10, 100, 1_000, 10_000, 100_000])
def test_fully_consumes_long_chain_of_event_messages(
    n_msg: int,
    n_event: int,
    configured_dummy_reduction: BeamlimeApp,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = configured_dummy_reduction
    sink = app.sink

    for i in range(n_msg):
        app.publish_events(size=n_event, time=i, reuse_events=True)
    n_step = 0
    while n_step < n_msg:
        app.step()
        n_step += 1
        accumulated_counts = sink.messages[-1].value.values.sum()
        if accumulated_counts == n_msg * n_event:
            break
    # Fuzzy limit, depends on how many messages the service can consume in one step.
    # Currently it is configured to consume up to 100.
    assert n_step <= max(1, n_msg // 20)


def test_message_with_unknown_schema_is_ignored(
    configured_dummy_reduction: BeamlimeApp,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = configured_dummy_reduction
    sink = app.sink

    app.publish_events(size=1000, time=0, reuse_events=True)
    # Unknown schema, should be skipped
    app.publish_data(topic=app.detector_topic, time=1, data=b'corrupt data')
    app.publish_events(size=1000, time=1, reuse_events=True)

    app.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000

    # Check log messages for exceptions
    assert "has an unknown schema. Skipping." in caplog.text
    warning_records = [
        r
        for r in caplog.records
        if r.levelname == "WARNING" and "beamlime.kafka.message_adapter" in r.name
    ]
    assert any("has an unknown schema. Skipping." in r.message for r in warning_records)


def test_message_that_cannot_be_decoded_is_ignored(
    configured_dummy_reduction: BeamlimeApp,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = configured_dummy_reduction
    sink = app.sink

    app.publish_events(size=1000, time=0, reuse_events=True)
    # Correct schema but invalid data, should be skipped
    app.publish_data(topic=app.detector_topic, time=1, data=b'1234ev44data')
    app.publish_events(size=1000, time=1, reuse_events=True)

    app.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000

    # Check log messages for exceptions
    assert "Error adapting message" in caplog.text
    assert "unpack_from requires a buffer" in caplog.text
    error_records = [
        r
        for r in caplog.records
        if r.levelname == "ERROR" and "beamlime.kafka.message_adapter" in r.name
    ]
    assert any("unpack_from requires a buffer" in r.message for r in error_records)


def test_message_with_bad_ev44_is_ignored(
    configured_dummy_reduction: BeamlimeApp,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = configured_dummy_reduction
    sink = app.sink

    app.publish_events(size=1000, time=0, reuse_events=True)
    bad_events = eventdata_ev44.serialise_ev44(
        source_name='panel_0',
        message_id=0,
        reference_time=[],
        reference_time_index=0,
        time_of_flight=[1, 2],
        pixel_id=[1],  # Invalid, should be the same length as time_of_flight
    )
    app.publish_data(topic=app.detector_topic, time=1, data=bad_events)
    app.publish_events(size=1000, time=1, reuse_events=True)

    # We should have valid data in the *same* batch of messages, but only the bad
    # message should be skipped.
    app.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000


def test_message_with_bad_timestamp_is_ignored(
    configured_dummy_reduction: BeamlimeApp,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    app = configured_dummy_reduction
    sink = app.sink

    app.publish_events(size=1000, time=0, reuse_events=True)
    bad_events = eventdata_ev44.serialise_ev44(
        source_name='panel_0',
        message_id=0,
        reference_time=[],
        reference_time_index=0,
        time_of_flight=[1, 2],
        pixel_id=[1, 2],
    )

    app.publish_data(topic=app.detector_topic, time=np.array([1, 2]), data=bad_events)
    app.publish_events(size=1000, time=1, reuse_events=True)

    # We should have valid data in the *same* batch of messages, but only the bad
    # message should be skipped.
    app.step()
    assert len(sink.messages) == 1
    assert sink.messages[0].value.values.sum() == 2000
