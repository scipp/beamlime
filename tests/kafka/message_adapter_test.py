# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import json

import numpy as np
import pytest
import scipp as sc
from streaming_data_types import dataarray_da00, eventdata_ev44, logdata_f144
from streaming_data_types.exceptions import WrongSchemaException

from beamlime.core.message import (
    CONFIG_STREAM_ID,
    Message,
    MessageSource,
    StreamId,
    StreamKind,
)
from beamlime.handlers.accumulators import DetectorEvents
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    BeamlimeConfigMessageAdapter,
    ChainedAdapter,
    Da00ToScippAdapter,
    Ev44ToDetectorEventsAdapter,
    Ev44ToMonitorEventsAdapter,
    F144ToLogDataAdapter,
    FakeKafkaMessage,
    InputStreamKey,
    KafkaMessage,
    KafkaToDa00Adapter,
    KafkaToEv44Adapter,
    KafkaToF144Adapter,
    KafkaToMonitorEventsAdapter,
    RawConfigItem,
    RouteBySchemaAdapter,
    RouteByTopicAdapter,
)


def make_serialized_ev44() -> bytes:
    return eventdata_ev44.serialise_ev44(
        source_name="monitor1",
        message_id=0,
        reference_time=[1234],
        reference_time_index=0,
        time_of_flight=[123456],
        pixel_id=[1],
    )


class FakeKafkaMessageSource(MessageSource[KafkaMessage]):
    def get_messages(self) -> list[KafkaMessage]:
        ev44 = make_serialized_ev44()
        return [FakeKafkaMessage(value=ev44, topic="monitors")]


def make_serialized_f144() -> bytes:
    return logdata_f144.serialise_f144(
        source_name="temperature1", value=123.45, timestamp_unix_ns=9876543210
    )


class FakeF144KafkaMessageSource(MessageSource[KafkaMessage]):
    def get_messages(self) -> list[KafkaMessage]:
        f144 = make_serialized_f144()
        return [FakeKafkaMessage(value=f144, topic="sensors")]


def make_serialized_da00() -> bytes:
    """Create serialized DA00 message for testing."""
    return dataarray_da00.serialise_da00(
        source_name="instrument",
        timestamp_ns=5678,
        data=[
            dataarray_da00.Variable(name="signal", data=np.array([1.0]), unit="counts"),
            dataarray_da00.Variable(
                name="temperature", data=np.array([25.0]), unit="degC"
            ),
        ],
    )


class FakeDa00KafkaMessageSource(MessageSource[KafkaMessage]):
    def get_messages(self) -> list[KafkaMessage]:
        da00 = make_serialized_da00()
        return [FakeKafkaMessage(value=da00, topic="instrument")]


class TestFakeKafkaMessageSource:
    def test_source(self) -> None:
        source = FakeKafkaMessageSource()
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].topic() == "monitors"
        assert messages[0].value() == make_serialized_ev44()


class TestKafkaToMonitorEventsAdapter:
    def test_adapter(self) -> None:
        source = AdaptingMessageSource(
            source=FakeKafkaMessageSource(),
            adapter=KafkaToMonitorEventsAdapter(
                stream_lut={
                    InputStreamKey(
                        topic="monitors", source_name="monitor1"
                    ): "monitor_0"
                }
            ),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.MONITOR_EVENTS
        assert messages[0].stream.name == "monitor_0"
        assert messages[0].value.time_of_arrival == [123456]
        assert messages[0].timestamp == 1234

    def test_no_reference_time_uses_message_timestamp(self) -> None:
        """Test that when reference_time is empty, the message timestamp is used."""
        empty_ref_time_ev44 = eventdata_ev44.serialise_ev44(
            source_name="monitor1",
            message_id=0,
            reference_time=np.array([]),  # Empty reference time
            reference_time_index=0,
            time_of_flight=np.array([123456]),
            pixel_id=np.array([1]),
        )

        message = FakeKafkaMessage(
            value=empty_ref_time_ev44, topic="monitors", timestamp=9999
        )

        adapter = KafkaToMonitorEventsAdapter(
            stream_lut={
                InputStreamKey(topic="monitors", source_name="monitor1"): "monitor_0"
            }
        )
        result = adapter.adapt(message)

        assert result.timestamp == 9999

    def test_wrong_schema_raises_exception(self, monkeypatch) -> None:
        """Test that providing wrong schema raises exception."""

        def mock_check_schema(*args, **kwargs):
            raise WrongSchemaException("Wrong schema")

        monkeypatch.setattr(
            "streaming_data_types.eventdata_ev44.check_schema_identifier",
            mock_check_schema,
        )

        message = FakeKafkaMessage(value=b"fake_data", topic="monitors")

        adapter = KafkaToMonitorEventsAdapter(stream_lut={})

        with pytest.raises(WrongSchemaException, match="Wrong schema"):
            adapter.adapt(message)


class TestKafkaToF144Adapter:
    def test_adapter(self) -> None:
        source = AdaptingMessageSource(
            source=FakeF144KafkaMessageSource(),
            adapter=KafkaToF144Adapter(),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.LOG
        assert messages[0].stream.name == "temperature1"
        assert messages[0].value.value == 123.45
        assert messages[0].timestamp == 9876543210


class TestF144ToLogDataAdapter:
    def test_adapter(self) -> None:
        source = AdaptingMessageSource(
            source=FakeF144KafkaMessageSource(),
            adapter=ChainedAdapter(
                first=KafkaToF144Adapter(), second=F144ToLogDataAdapter()
            ),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.LOG
        assert messages[0].stream.name == "temperature1"
        assert messages[0].value.value == 123.45
        assert messages[0].value.time == 9876543210
        assert messages[0].timestamp == 9876543210


class TestKafkaToDa00Adapter:
    def test_adapter(self) -> None:
        source = AdaptingMessageSource(
            source=FakeDa00KafkaMessageSource(),
            adapter=KafkaToDa00Adapter(stream_kind=StreamKind.MONITOR_COUNTS),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.MONITOR_COUNTS
        assert messages[0].stream.name == "instrument"
        assert messages[0].timestamp == 5678
        assert len(messages[0].value) == 1
        assert messages[0].value[0].name == "temperature"
        assert messages[0].value[0].values.tolist() == [25.0]
        assert messages[0].value[0].unit == "degC"

    def test_adapter_with_stream_mapping(self) -> None:
        source = AdaptingMessageSource(
            source=FakeDa00KafkaMessageSource(),
            adapter=KafkaToDa00Adapter(
                stream_kind=StreamKind.MONITOR_COUNTS,
                stream_lut={
                    InputStreamKey(
                        topic="instrument", source_name="instrument"
                    ): "mapped_instrument"
                },
            ),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.MONITOR_COUNTS
        assert messages[0].stream.name == "mapped_instrument"


class TestDa00ToScippAdapter:
    def test_adapter(self) -> None:
        source = AdaptingMessageSource(
            source=FakeDa00KafkaMessageSource(),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(stream_kind=StreamKind.MONITOR_COUNTS),
                second=Da00ToScippAdapter(),
            ),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.MONITOR_COUNTS
        assert messages[0].stream.name == "instrument"
        assert isinstance(messages[0].value, sc.DataArray)
        assert messages[0].value.name == "temperature"
        assert messages[0].value.unit == sc.Unit("degC")
        assert messages[0].value.values.tolist() == [25.0]


class TestEv44ToDetectorEventsAdapter:
    def test_adapter(self) -> None:
        ev44_message = Message(
            timestamp=1234,
            stream=StreamId(kind=StreamKind.DETECTOR_EVENTS, name="detector1"),
            value=eventdata_ev44.EventData(
                source_name="detector1",
                message_id=0,
                reference_time=np.array([1234]),
                reference_time_index=0,
                time_of_flight=np.array([123456]),
                pixel_id=np.array([1]),
            ),
        )
        adapter = Ev44ToDetectorEventsAdapter()
        result = adapter.adapt(ev44_message)

        assert result.timestamp == 1234
        assert result.stream.kind == StreamKind.DETECTOR_EVENTS
        assert result.stream.name == "detector1"
        assert isinstance(result.value, DetectorEvents)
        assert result.value.time_of_arrival == [123456]
        assert result.value.pixel_id == [1]

    def test_adapter_merge_detectors(self) -> None:
        ev44_message = Message(
            timestamp=1234,
            stream=StreamId(kind=StreamKind.DETECTOR_EVENTS, name="detector2"),
            value=eventdata_ev44.EventData(
                source_name="detector2",
                message_id=0,
                reference_time=np.array([1234]),
                reference_time_index=0,
                time_of_flight=np.array([123456]),
                pixel_id=np.array([1]),
            ),
        )
        adapter = Ev44ToDetectorEventsAdapter(merge_detectors=True)
        result = adapter.adapt(ev44_message)

        assert result.stream.name == "unified_detector"
        assert isinstance(result.value, DetectorEvents)


def message_with_schema(schema: str) -> KafkaMessage:
    """
    Create a fake Kafka message with the given schema.

    The streaming_data_types library uses bytes 4:8 to store the schema.
    """
    return FakeKafkaMessage(value=f"xxxx{schema}".encode(), topic=schema)


class TestRouteBySchemaAdapter:
    def test_raises_KeyError_if_no_route_found(self) -> None:
        adapter = RouteBySchemaAdapter(routes={})
        with pytest.raises(KeyError, match="ev44"):
            adapter.adapt(message_with_schema("ev44"))

    def test_calls_adapter_based_on_route(self) -> None:
        class TestAdapter:
            def __init__(self, value: str):
                self._value = value

            def adapt(self, message: KafkaMessage) -> Message[str]:
                return fake_message_with_value(message, self._value)

        adapter = RouteBySchemaAdapter(
            routes={"ev44": TestAdapter('adapter1'), "da00": TestAdapter('adapter2')}
        )
        assert adapter.adapt(message_with_schema('ev44')).value == "adapter1"
        assert adapter.adapt(message_with_schema('da00')).value == "adapter2"


class TestRouteByTopicAdapter:
    def test_route_by_topic(self) -> None:
        class TestAdapter:
            def __init__(self, return_value: str):
                self.adapt_called = False
                self.last_message = None
                self.return_value = return_value

            def adapt(self, message: KafkaMessage) -> Message[str]:
                self.adapt_called = True
                self.last_message = message
                return fake_message_with_value(message, self.return_value)

        adapter1 = TestAdapter("adapter1")
        adapter2 = TestAdapter("adapter2")

        router = RouteByTopicAdapter(routes={"topic1": adapter1, "topic2": adapter2})

        assert router.topics == ["topic1", "topic2"]

        msg1 = FakeKafkaMessage(value=b"dummy", topic="topic1")
        result1 = router.adapt(msg1)
        assert adapter1.adapt_called is True
        assert adapter1.last_message == msg1
        assert result1.value == "adapter1"

        msg2 = FakeKafkaMessage(value=b"dummy", topic="topic2")
        result2 = router.adapt(msg2)
        assert adapter2.adapt_called is True
        assert adapter2.last_message == msg2
        assert result2.value == "adapter2"

    def test_unknown_topic_raises_key_error(self) -> None:
        router = RouteByTopicAdapter(routes={})
        msg = FakeKafkaMessage(value=b"dummy", topic="unknown")

        with pytest.raises(KeyError, match="unknown"):
            router.adapt(msg)


class TestKafkaToEv44Adapter:
    def test_no_reference_time_uses_message_timestamp(self) -> None:
        """Test that when reference_time is empty, the message timestamp is used."""
        empty_ref_time_ev44 = eventdata_ev44.serialise_ev44(
            source_name="monitor1",
            message_id=0,
            reference_time=np.array([]),  # Empty reference time
            reference_time_index=0,
            time_of_flight=np.array([123456]),
            pixel_id=np.array([1]),
        )

        message = FakeKafkaMessage(
            value=empty_ref_time_ev44, topic="monitors", timestamp=9999
        )

        adapter = KafkaToEv44Adapter(stream_kind=StreamKind.MONITOR_EVENTS)
        result = adapter.adapt(message)

        assert result.timestamp == 9999


class TestAdaptingMessageSource:
    def test_source(self) -> None:
        source = AdaptingMessageSource(
            source=FakeKafkaMessageSource(),
            adapter=ChainedAdapter(
                first=KafkaToEv44Adapter(stream_kind=StreamKind.MONITOR_EVENTS),
                second=Ev44ToMonitorEventsAdapter(),
            ),
        )
        messages = source.get_messages()
        assert len(messages) == 1
        assert messages[0].stream.kind == StreamKind.MONITOR_EVENTS
        assert messages[0].stream.name == "monitor1"
        assert messages[0].value.time_of_arrival == [123456]
        assert messages[0].timestamp == 1234

    def test_unknown_schema_is_logged_and_skipped(self) -> None:
        class FakeLogger:
            def __init__(self):
                self.warning_calls = []
                self.exception_calls = []

            def warning(self, message, *args, **kwargs):
                self.warning_calls.append((message, args, kwargs))

            def exception(self, message, *args, **kwargs):
                self.exception_calls.append((message, args, kwargs))

        fake_logger = FakeLogger()
        unknown_schema_message = FakeKafkaMessage(value=b"xxxx????", topic="unknown")

        class TestMessageSource(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [unknown_schema_message]

        adapting_source = AdaptingMessageSource(
            source=TestMessageSource(),
            adapter=KafkaToDa00Adapter(stream_kind=StreamKind.DETECTOR_EVENTS),
            logger=fake_logger,
        )

        messages = adapting_source.get_messages()

        assert len(messages) == 0
        assert len(fake_logger.warning_calls) == 1
        assert "unknown schema" in fake_logger.warning_calls[0][0].lower()

    def test_exception_during_adaptation_is_logged_and_raised(self) -> None:
        class FakeLogger:
            def __init__(self):
                self.warning_calls = []
                self.exception_calls = []

            def warning(self, message, *args, **kwargs):
                self.warning_calls.append((message, args, kwargs))

            def exception(self, message, *args, **kwargs):
                self.exception_calls.append((message, args, kwargs))

        fake_logger = FakeLogger()

        class TestMessageSource(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [FakeKafkaMessage(value=b"dummy", topic="test")]

        class TestAdapter:
            def adapt(self, message):
                raise ValueError("Test error")

        adapting_source = AdaptingMessageSource(
            source=TestMessageSource(), adapter=TestAdapter(), logger=fake_logger
        )

        with pytest.raises(ValueError, match="Test error"):
            adapting_source.get_messages()

        assert len(fake_logger.exception_calls) == 1
        assert "error adapting message" in fake_logger.exception_calls[0][0].lower()

    def test_close_calls_source_close(self) -> None:
        class TestMessageSource(MessageSource[KafkaMessage]):
            def __init__(self):
                self.close_called = False

            def get_messages(self):
                return []

            def close(self):
                self.close_called = True

        mock_source = TestMessageSource()

        class TestAdapter:
            def adapt(self, message):
                pass

        adapting_source = AdaptingMessageSource(
            source=mock_source, adapter=TestAdapter()
        )
        adapting_source.close()

        assert mock_source.close_called is True


def fake_message_with_value(message: KafkaMessage, value: str) -> Message[str]:
    return Message(timestamp=1234, stream=StreamId(name="dummy"), value=value)


class TestBeamlimeConfigMessageAdapter:
    def test_adapter(self) -> None:
        key = b'my_source/my_service/my_key'
        encoded = json.dumps('my_value').encode('utf-8')
        message = FakeKafkaMessage(
            key=key, value=encoded, topic="dummy_beamlime_commands"
        )
        adapter = BeamlimeConfigMessageAdapter()
        adapted_message = adapter.adapt(message)
        # So it gets routed to config handler
        assert adapted_message.stream == CONFIG_STREAM_ID
        assert adapted_message.value == RawConfigItem(key=key, value=encoded)
