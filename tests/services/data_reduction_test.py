# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import json
import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pytest
from streaming_data_types import eventdata_ev44

from beamlime import StreamKind
from beamlime.config.instruments import available_instruments, get_config
from beamlime.config.models import ConfigKey, WorkflowConfig, WorkflowSpecs
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import CONFIG_STREAM_ID
from beamlime.fakes import FakeMessageSink
from beamlime.handlers.config_handler import ConfigUpdate
from beamlime.kafka.message_adapter import FakeKafkaMessage, KafkaMessage
from beamlime.kafka.sink import UnrollingSinkAdapter
from beamlime.kafka.source import KafkaConsumer
from beamlime.services.data_reduction import make_reduction_service_builder


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

    service: KafkaConsumer
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

    def publish_config_message(self, key: ConfigKey, value: Any) -> None:
        message = FakeKafkaMessage(
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            topic=stream_kind_to_topic(
                instrument=self.instrument, kind=StreamKind.BEAMLIME_CONFIG
            ),
            timestamp=0,
        )
        self.consumer.add_message(message)

    def publish_events(self, *, size: int, time: int) -> None:
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_monitor_ev44(name='monitor1', size=size),
            topic=self._monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)
        monitor_message = FakeKafkaMessage(
            value=self.make_serialized_monitor_ev44(name='monitor2', size=size),
            topic=self._monitor_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(monitor_message)
        message = FakeKafkaMessage(
            value=self.make_serialized_ev44(
                name=next(iter(self._detector_config)), size=size
            ),
            topic=self._detector_topic,
            timestamp=time * 1_000_000_000,
        )
        self.consumer.add_message(message)

    def make_serialized_ev44(self, name: str, size: int) -> bytes:
        time_of_arrival = self._rng.uniform(0, 70_000_000, size).astype(np.int32)
        first, last = self._detector_config[name]
        pixel_id = self._rng.integers(first, last + 1, size, dtype=np.int32)
        # Note empty reference_time. KafkaToEv44Adapter uses message.timestamp(), which
        # allows us to reuse the serialized content, to avoid seeing the cost in the
        # benchmarks.
        return eventdata_ev44.serialise_ev44(
            source_name=name,
            message_id=0,
            reference_time=[],
            reference_time_index=0,
            time_of_flight=time_of_arrival,
            pixel_id=pixel_id,
        )

    def make_serialized_monitor_ev44(self, name: str, size: int) -> bytes:
        time_of_arrival = self._rng.uniform(0, 70_000_000, size).astype(np.int32)
        pixel_id = np.zeros(size, dtype=np.int32)
        # Note empty reference_time. KafkaToEv44Adapter uses message.timestamp(), which
        # allows us to reuse the serialized content, to avoid seeing the cost in the
        # benchmarks.
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


@pytest.fixture(params=('bifrost', 'loki'))
def reduction_app(request) -> ReductionApp:
    """Create a testable service with a fake consumer and sink."""
    return make_reduction_app(instrument=request.param)


@pytest.mark.parametrize("instrument", available_instruments())
def test_publishes_workflow_specs_on_startup(instrument: str) -> None:
    app = make_reduction_app(instrument=instrument)
    sink = app.sink
    instrument = app.instrument

    assert len(sink.messages) == 1
    message = sink.messages[0]
    assert message.stream == CONFIG_STREAM_ID
    assert isinstance(message.value, ConfigUpdate)
    assert isinstance(message.value.config_key, ConfigKey)
    assert isinstance(message.value.value, WorkflowSpecs)
    if instrument in ('bifrost', 'loki'):
        assert len(message.value.value.workflows) > 0


def test_can_configure_and_stop_workflow(
    reduction_app: ReductionApp, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    sink = reduction_app.sink
    service = reduction_app.service
    workflow_specs = sink.messages[0].value.value
    workflow_id, spec = next(iter(workflow_specs.workflows.items()))
    sink.messages.clear()
    service.step()
    assert len(sink.messages) == 0

    reduction_app.publish_events(size=1000, time=0)
    service.step()
    assert len(sink.messages) == 0

    # Assume workflow is runnable for all source names
    config_key = ConfigKey(
        source_name=None, service_name="data_reduction", key="workflow_config"
    )
    workflow_config = WorkflowConfig(
        identifier=workflow_id,
        values={param.name: param.default for param in spec.parameters},
    )
    reduction_app.publish_config_message(
        key=config_key, value=workflow_config.model_dump()
    )
    reduction_app.publish_events(size=2000, time=2)
    service.step()
    assert len(sink.messages) == 1
    # Events before workflow config was published should not be included
    assert sink.messages[0].value.values.sum() == 2000
    service.step()
    assert len(sink.messages) == 1
    reduction_app.publish_events(size=3000, time=4)
    service.step()
    assert len(sink.messages) == 2
    assert sink.messages[1].value.values.sum() == 5000

    # More events but the same time, should not publish again
    reduction_app.publish_events(size=1000, time=4)
    service.step()
    assert len(sink.messages) == 2

    # Later time should publish again, including the previous events with duplicate time
    reduction_app.publish_events(size=1000, time=5)
    service.step()
    assert len(sink.messages) == 3
    assert sink.messages[2].value.values.sum() == 7000

    # Stop workflow
    reduction_app.publish_config_message(key=config_key, value=None)
    reduction_app.publish_events(size=1000, time=10)
    service.step()
    reduction_app.publish_events(size=1000, time=20)
    service.step()
    assert len(sink.messages) == 3
