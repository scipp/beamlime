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
    """Create serialized da00 message for testing."""
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
        message = FakeKafkaMessage(value=make_serialized_ev44(), topic="monitors")
        adapter = KafkaToMonitorEventsAdapter(
            stream_lut={
                InputStreamKey(topic="monitors", source_name="monitor1"): "monitor_0"
            }
        )
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.MONITOR_EVENTS
        assert result.stream.name == "monitor_0"
        assert result.value.time_of_arrival == [123456]
        assert result.timestamp == 1234

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
        message = FakeKafkaMessage(value=make_serialized_f144(), topic="sensors")
        adapter = KafkaToF144Adapter()
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.LOG
        assert result.stream.name == "temperature1"
        assert result.value.value == 123.45
        assert result.timestamp == 9876543210

    def test_adapter_with_stream_mapping(self) -> None:
        message = FakeKafkaMessage(value=make_serialized_f144(), topic="sensors")
        adapter = KafkaToF144Adapter(
            stream_lut={
                InputStreamKey(
                    topic="sensors", source_name="temperature1"
                ): "mapped_temperature"
            }
        )
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.LOG
        assert result.stream.name == "mapped_temperature"


class TestF144ToLogDataAdapter:
    def test_adapter(self) -> None:
        f144_adapter = KafkaToF144Adapter()
        message = FakeKafkaMessage(value=make_serialized_f144(), topic="sensors")
        adapted_f144 = f144_adapter.adapt(message)

        log_data_adapter = F144ToLogDataAdapter()
        result = log_data_adapter.adapt(adapted_f144)

        assert result.stream.kind == StreamKind.LOG
        assert result.stream.name == "temperature1"
        assert result.value.value == 123.45
        assert result.value.time == 9876543210
        assert result.timestamp == 9876543210


class TestKafkaToDa00Adapter:
    def test_adapter(self) -> None:
        message = FakeKafkaMessage(value=make_serialized_da00(), topic="instrument")
        adapter = KafkaToDa00Adapter(stream_kind=StreamKind.MONITOR_COUNTS)
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.MONITOR_COUNTS
        assert result.stream.name == "instrument"
        assert result.timestamp == 5678
        assert len(result.value) == 2  # signal and temperature
        assert {var.name for var in result.value} == {"signal", "temperature"}

    def test_adapter_with_stream_mapping(self) -> None:
        message = FakeKafkaMessage(value=make_serialized_da00(), topic="instrument")
        adapter = KafkaToDa00Adapter(
            stream_kind=StreamKind.MONITOR_COUNTS,
            stream_lut={
                InputStreamKey(
                    topic="instrument", source_name="instrument"
                ): "mapped_instrument"
            },
        )
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.MONITOR_COUNTS
        assert result.stream.name == "mapped_instrument"


class TestDa00ToScippAdapter:
    def test_adapter(self) -> None:
        da00_adapter = KafkaToDa00Adapter(stream_kind=StreamKind.MONITOR_COUNTS)
        message = FakeKafkaMessage(value=make_serialized_da00(), topic="instrument")
        adapted_da00 = da00_adapter.adapt(message)

        scipp_adapter = Da00ToScippAdapter()
        result = scipp_adapter.adapt(adapted_da00)

        assert result.stream.kind == StreamKind.MONITOR_COUNTS
        assert result.stream.name == "instrument"
        assert isinstance(result.value, sc.DataArray)
        assert result.value.unit == 'counts'
        assert result.value.values == [1.0]
        assert 'temperature' in result.value.coords


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
    def test_adapter_with_stream_mapping(self) -> None:
        message = FakeKafkaMessage(value=make_serialized_ev44(), topic="monitors")
        adapter = KafkaToEv44Adapter(
            stream_kind=StreamKind.MONITOR_EVENTS,
            stream_lut={
                InputStreamKey(
                    topic="monitors", source_name="monitor1"
                ): "mapped_monitor1"
            },
        )
        result = adapter.adapt(message)

        assert result.stream.kind == StreamKind.MONITOR_EVENTS
        assert result.stream.name == "mapped_monitor1"
        assert result.value.time_of_flight == [123456]
        assert result.timestamp == 1234

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
            raise_on_error=False,
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
            source=TestMessageSource(),
            adapter=TestAdapter(),
            logger=fake_logger,
            raise_on_error=True,  # Explicitly set to raise errors
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


class TestErrorHandling:
    """Tests for handling malformed or corrupt Kafka messages."""

    def setup_method(self):
        self.fake_logger = self._create_fake_logger()

    def _create_fake_logger(self):
        class FakeLogger:
            def __init__(self):
                self.warning_calls = []
                self.exception_calls = []
                self.info_calls = []

            def warning(self, message, *args, **kwargs):
                self.warning_calls.append((message, args, kwargs))

            def exception(self, message, *args, **kwargs):
                self.exception_calls.append((message, args, kwargs))

            def info(self, message, *args, **kwargs):
                self.info_calls.append((message, args, kwargs))

        return FakeLogger()

    def test_corrupt_ev44_message(self, monkeypatch):
        """Test handling of corrupt ev44 message."""
        # Create a message that has the ev44 schema identifier but corrupt content
        corrupt_ev44 = bytearray(make_serialized_ev44())
        # Corrupt the data after the schema identifier
        if len(corrupt_ev44) > 12:
            corrupt_ev44[12:] = b'corrupt' * 10

        class CorruptEv44Source(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [FakeKafkaMessage(value=bytes(corrupt_ev44), topic="monitors")]

        def mock_deserialize(*args, **kwargs):
            raise ValueError("Failed to deserialize corrupt ev44 data")

        monkeypatch.setattr(
            "streaming_data_types.eventdata_ev44.deserialise_ev44", mock_deserialize
        )

        source = AdaptingMessageSource(
            source=CorruptEv44Source(),
            adapter=KafkaToEv44Adapter(stream_kind=StreamKind.MONITOR_EVENTS),
            logger=self.fake_logger,
            raise_on_error=True,  # Explicitly set to raise errors
        )

        # The exception should be caught and logged, then re-raised
        with pytest.raises(ValueError, match="Failed to deserialize corrupt ev44 data"):
            source.get_messages()

        assert len(self.fake_logger.exception_calls) == 1
        assert (
            "error adapting message" in self.fake_logger.exception_calls[0][0].lower()
        )

    def test_corrupt_da00_message(self, monkeypatch):
        """Test handling of corrupt da00 message."""
        corrupt_da00 = bytearray(make_serialized_da00())
        # Corrupt the data after the schema identifier
        if len(corrupt_da00) > 12:
            corrupt_da00[12:] = b'corrupt' * 10

        class CorruptDa00Source(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [FakeKafkaMessage(value=bytes(corrupt_da00), topic="instrument")]

        def mock_deserialize(*args, **kwargs):
            raise ValueError("Failed to deserialize corrupt da00 data")

        monkeypatch.setattr(
            "streaming_data_types.dataarray_da00.deserialise_da00", mock_deserialize
        )

        source = AdaptingMessageSource(
            source=CorruptDa00Source(),
            adapter=KafkaToDa00Adapter(stream_kind=StreamKind.MONITOR_COUNTS),
            logger=self.fake_logger,
            raise_on_error=True,  # Explicitly set to raise errors
        )

        with pytest.raises(ValueError, match="Failed to deserialize corrupt da00 data"):
            source.get_messages()

        assert len(self.fake_logger.exception_calls) == 1
        assert (
            "error adapting message" in self.fake_logger.exception_calls[0][0].lower()
        )

    def test_corrupt_f144_message(self, monkeypatch):
        """Test handling of corrupt f144 message."""
        corrupt_f144 = bytearray(make_serialized_f144())
        # Corrupt the data after the schema identifier
        if len(corrupt_f144) > 12:
            corrupt_f144[12:] = b'corrupt' * 10

        class CorruptF144Source(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [FakeKafkaMessage(value=bytes(corrupt_f144), topic="sensors")]

        def mock_deserialize(*args, **kwargs):
            raise ValueError("Failed to deserialize corrupt f144 data")

        monkeypatch.setattr(
            "streaming_data_types.logdata_f144.deserialise_f144", mock_deserialize
        )

        source = AdaptingMessageSource(
            source=CorruptF144Source(),
            adapter=KafkaToF144Adapter(),
            logger=self.fake_logger,
            raise_on_error=True,  # Explicitly set to raise errors
        )

        with pytest.raises(ValueError, match="Failed to deserialize corrupt f144 data"):
            source.get_messages()

        assert len(self.fake_logger.exception_calls) == 1
        assert (
            "error adapting message" in self.fake_logger.exception_calls[0][0].lower()
        )

    def test_mixed_good_and_corrupt_messages(self, monkeypatch):
        """Test handling a mix of good and corrupt messages."""

        class MixedMessagesSource(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [
                    FakeKafkaMessage(
                        value=make_serialized_ev44(), topic="monitors"
                    ),  # Good
                    FakeKafkaMessage(
                        value=b"xxxx????", topic="unknown"
                    ),  # Unknown schema
                    FakeKafkaMessage(
                        value=make_serialized_f144(), topic="sensors"
                    ),  # Good
                ]

        # Mock to make the second good message fail
        original_deserialize_f144 = logdata_f144.deserialise_f144

        def mock_deserialize_f144(buffer):
            if buffer == make_serialized_f144():
                raise ValueError("Simulated failure for testing")
            return original_deserialize_f144(buffer)

        monkeypatch.setattr(
            "streaming_data_types.logdata_f144.deserialise_f144", mock_deserialize_f144
        )

        # Create a router that handles both ev44 and f144
        router = RouteBySchemaAdapter(
            {
                "ev44": KafkaToEv44Adapter(stream_kind=StreamKind.MONITOR_EVENTS),
                "f144": KafkaToF144Adapter(),
            }
        )

        source = AdaptingMessageSource(
            source=MixedMessagesSource(),
            adapter=router,
            logger=self.fake_logger,
            raise_on_error=False,
        )

        source.get_messages()

        assert len(self.fake_logger.exception_calls) == 2
        assert isinstance(self.fake_logger.exception_calls[0][1][1], KeyError)
        assert (
            "error adapting message" in self.fake_logger.exception_calls[1][0].lower()
        )

    def test_non_fatal_error_handling_option(self):
        """Test an option to make adapter errors non-fatal."""

        class FailingAdapter:
            def adapt(self, message):
                raise ValueError("Simulated adapter failure")

        class SimpleSource(MessageSource[KafkaMessage]):
            def get_messages(self):
                return [FakeKafkaMessage(value=b"any", topic="any")]

        # Test with raise_on_error=True (explicit setting)
        source_raising = AdaptingMessageSource(
            source=SimpleSource(),
            adapter=FailingAdapter(),
            logger=self.fake_logger,
            raise_on_error=True,
        )

        with pytest.raises(ValueError, match="Simulated adapter failure"):
            source_raising.get_messages()

        # Test with default behavior (raise_on_error=False)
        source_non_raising = AdaptingMessageSource(
            source=SimpleSource(), adapter=FailingAdapter(), logger=self.fake_logger
        )

        # Should not raise an exception, but should log it
        messages = source_non_raising.get_messages()
        assert len(messages) == 0  # No messages should be returned
        assert len(self.fake_logger.exception_calls) >= 1
        assert "Simulated adapter failure" in str(self.fake_logger.exception_calls[-1])
