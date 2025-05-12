# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import json
import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime import Service, StreamKind
from beamlime.config import models
from beamlime.config.instruments import available_instruments, get_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.fakes import FakeMessageSink
from beamlime.handlers.config_handler import ConfigUpdate
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.sink import UnrollingSinkAdapter
from beamlime.kafka.source import KafkaConsumer
from beamlime.services.data_reduction import make_reduction_service_builder


def _get_workflow_by_name(
    workflow_specs: models.WorkflowSpecs, name: str
) -> tuple[str, models.WorkflowSpec]:
    for wid, spec in workflow_specs.workflows.items():
        if spec.name == name:
            return wid, spec
    raise ValueError(f"Workflow {name} not found in specs")


class FakeConsumer(KafkaConsumer):
    def __init__(self) -> None:
        self._messages: list[KafkaMessage] = []

    def add_message(self, message: KafkaMessage) -> None:
        self._messages.append(message)

    def consume(self, num_messages: int, timeout: float) -> list[KafkaMessage]:
        _ = timeout  # Ignore timeout for the fake consumer
        result = self._messages[:num_messages]
        self._messages = self._messages[num_messages:]
        return result


@dataclass(kw_only=True)
class ReductionApp:
    """A testable "application" with a fake consumer and sink."""

    service: Service
    consumer: FakeConsumer
    sink: FakeMessageSink
    instrument: str

    def __post_init__(self) -> None:
        self._detector_topic = stream_kind_to_topic(
            instrument=self.instrument, kind=StreamKind.DETECTOR_EVENTS
        )
        self._monitor_topic = stream_kind_to_topic(
            instrument=self.instrument, kind=StreamKind.MONITOR_EVENTS
        )
        self._detector_config = get_config(self.instrument).detectors_config['fakes']
        self._rng = np.random.default_rng(seed=1234)  # Avoid test flakiness

    def publish_config_message(self, key: models.ConfigKey, value: Any) -> None:
        message = FakeKafkaMessage(
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            topic=stream_kind_to_topic(
                instrument=self.instrument, kind=StreamKind.BEAMLIME_CONFIG
            ),
            timestamp=0,
        )
        self.consumer.add_message(message)

    def publish_monitor_events(self, *, size: int, time: int) -> None:
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_ev44(name='monitor1', size=size, with_ids=False),
            topic=self._monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_ev44(name='monitor2', size=size, with_ids=False),
            topic=self._monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)

    def publish_events(self, *, size: int, time: int) -> None:
        message = FakeKafkaMessage(
            value=self.make_serialized_ev44(
                name=next(iter(self._detector_config)), size=size, with_ids=True
            ),
            topic=self._detector_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(message)

    def make_serialized_ev44(self, name: str, size: int, with_ids: bool) -> bytes:
        time_of_arrival = self._rng.uniform(0, 70_000_000, size).astype(np.int32)
        if with_ids:
            first, last = self._detector_config[name]
            pixel_id = self._rng.integers(first, last + 1, size, dtype=np.int32)
        else:
            pixel_id = np.zeros(size, dtype=np.int32)
        # Empty reference_time. KafkaToEv44Adapter falls back to message.timestamp().
        return eventdata_ev44.serialise_ev44(
            source_name=name,
            message_id=0,
            reference_time=[],
            reference_time_index=0,
            time_of_flight=time_of_arrival,
            pixel_id=pixel_id,
        )


def make_reduction_app(instrument: str) -> ReductionApp:
    builder = make_reduction_service_builder(instrument=instrument)
    sink = FakeMessageSink()
    consumer = FakeConsumer()
    service = builder.from_consumer(
        consumer=consumer, sink=UnrollingSinkAdapter(sink), raise_on_adapter_error=True
    )
    return ReductionApp(
        service=service, consumer=consumer, sink=sink, instrument=instrument
    )


@pytest.mark.parametrize("instrument", available_instruments())
def test_publishes_workflow_specs_on_startup(instrument: str) -> None:
    app = make_reduction_app(instrument=instrument)
    sink = app.sink
    instrument = app.instrument

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
