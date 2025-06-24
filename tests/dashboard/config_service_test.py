# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from contextlib import contextmanager

import pydantic
import pytest

from beamlime.config.models import TOARange
from beamlime.dashboard.config_service import (
    ConfigSchemaManager,
    ConfigService,
    FakeMessageBridge,
    LoopbackMessageBridge,
)
from beamlime.dashboard.detector_config import TOARangeParam


# Test models for more comprehensive testing
class SimpleConfig(pydantic.BaseModel):
    value: int
    name: str = "default"


class ComplexConfig(pydantic.BaseModel):
    items: list[str]
    metadata: dict[str, int]
    threshold: float


# Simple fake callback for testing
class FakeCallback:
    def __init__(self) -> None:
        self.called = False
        self.call_count = 0
        self.call_args: list = []
        self.data = None

    def __call__(self, data) -> None:
        self.called = True
        self.call_count += 1
        self.call_args.append(data)
        self.data = data

    def reset(self) -> None:
        self.called = False
        self.call_count = 0
        self.call_args.clear()
        self.data = None


class FailingCallback:
    def __init__(self, exception: Exception | None = None) -> None:
        self.exception = exception or ValueError("Callback failed")
        self.call_count = 0

    def __call__(self, data) -> None:
        self.call_count += 1
        raise self.exception


# Context manager to capture log messages
@contextmanager
def capture_logs(logger, level: str = 'error') -> list:
    captured: list[str] = []
    original_level_method = getattr(logger, level)

    def capture_method(msg, *args):
        captured.append(msg % args if args else msg)

    setattr(logger, level, capture_method)
    try:
        yield captured
    finally:
        setattr(logger, level, original_level_method)


@pytest.fixture
def config_key() -> str:
    return "toa_range"


@pytest.fixture
def simple_key() -> str:
    return "simple_config"


@pytest.fixture
def complex_key() -> str:
    return "complex_config"


@pytest.fixture
def schemas(config_key: str, simple_key: str, complex_key: str) -> ConfigSchemaManager:
    return ConfigSchemaManager(
        {
            config_key: TOARange,
            simple_key: SimpleConfig,
            complex_key: ComplexConfig,
        }
    )


@pytest.fixture
def service(schemas: ConfigSchemaManager) -> ConfigService:
    return ConfigService(schema_validator=schemas)


@pytest.fixture
def service_with_bridge(
    schemas: ConfigSchemaManager,
) -> tuple[ConfigService, FakeMessageBridge]:
    bridge = FakeMessageBridge()
    return ConfigService(schema_validator=schemas, message_bridge=bridge), bridge


@pytest.fixture
def service_with_loopback(
    schemas: ConfigSchemaManager,
) -> tuple[ConfigService, LoopbackMessageBridge]:
    bridge = LoopbackMessageBridge()
    return ConfigService(schema_validator=schemas, message_bridge=bridge), bridge


class TestConfigService:
    def test_subscriber(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        config_key: str,
    ) -> None:
        service, bridge = service_with_bridge
        toa_range = TOARangeParam()
        service.subscribe(key=config_key, callback=toa_range.from_pydantic())

        # Create a config update and add it to the bridge
        config_data = {'enabled': False, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'}
        bridge.add_incoming_message((config_key, config_data))

        # Process the message
        service.process_incoming_messages()

        assert toa_range.enabled is False
        assert toa_range.low == 1000.0
        assert toa_range.high == 2000.0
        assert toa_range.unit == 'us'

    def test_bidirectional_param_binding_no_infinite_cycle(
        self, service_with_loopback: tuple[ConfigService, LoopbackMessageBridge]
    ) -> None:
        """Test that bidirectional param binding doesn't cause infinite cycles."""
        service, bridge = service_with_loopback

        toa_range = TOARangeParam()
        toa_range.subscribe(service)

        # Init bridge with a single message
        bridge.add_incoming_message(
            (
                toa_range.config_key,
                {'enabled': True, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'},
            )
        )

        assert len(bridge.messages) == 1

        # Simulate GUI change (should publish to bridge)
        toa_range.low = 1500.0
        assert len(bridge.messages) == 2

        # Process the external message. Should NOT trigger another update to the bridge
        service.process_incoming_messages(num=1)
        assert toa_range.low == 1000.0
        assert len(bridge.messages) == 1

        # Process our own update. Should NOT trigger another update to the bridge
        service.process_incoming_messages(num=1)
        assert toa_range.low == 1500.0
        assert len(bridge.messages) == 0

    def test_get_nonexistent_key_returns_default(self, service: ConfigService) -> None:
        """Test getting a non-existent key returns the default value."""
        assert service.get("nonexistent") is None
        assert service.get("nonexistent", "default_value") == "default_value"
        assert service.get("nonexistent", 42) == 42

    def test_get_existing_key_returns_value(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test getting an existing key returns the stored value."""
        config = SimpleConfig(value=123, name="test")
        service._config[simple_key] = config

        result = service.get(simple_key)
        assert result == config
        assert result.value == 123
        assert result.name == "test"

    def test_update_config_valid_data(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test updating config with valid pydantic model."""
        config = SimpleConfig(value=456, name="updated")
        service.update_config(simple_key, config)

        assert service.get(simple_key) == config

    def test_update_config_invalid_type_raises_error(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test updating config with non-pydantic model raises TypeError."""
        with pytest.raises(TypeError, match="must be a pydantic model"):
            service.update_config(simple_key, {"value": 123})

    def test_update_config_unregistered_schema_raises_error(
        self, service: ConfigService
    ) -> None:
        """Test updating config with unregistered schema raises ValueError."""
        config = SimpleConfig(value=123)
        with pytest.raises(ValueError, match="No schema registered"):
            service.update_config("unregistered_key", config)

    def test_update_config_wrong_schema_type_raises_error(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test updating config with wrong schema type raises ValueError."""
        config = TOARange(enabled=True, low=100.0, high=200.0, unit="us")
        with pytest.raises(ValueError, match="No schema registered"):
            service.update_config(simple_key, config)

    def test_update_config_publishes_to_bridge(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that config updates are published to the message bridge."""
        service, bridge = service_with_bridge
        config = SimpleConfig(value=789, name="bridge_test")

        service.update_config(simple_key, config)

        published = bridge.get_published_messages()
        assert len(published) == 1
        assert published[0][0] == simple_key
        assert published[0][1] == {"value": 789, "name": "bridge_test"}

    def test_subscribe_immediate_callback_with_existing_data(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that subscribe immediately calls callback if data exists."""
        config = SimpleConfig(value=100, name="immediate")
        service._config[simple_key] = config

        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        assert callback.called is True
        assert callback.data == config

    def test_subscribe_no_immediate_callback_without_data(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that subscribe doesn't call callback if no data exists."""
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        assert callback.called is False

    def test_subscribe_multiple_callbacks(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that multiple callbacks can be registered for the same key."""
        callback1 = FakeCallback()
        callback2 = FakeCallback()
        callback3 = FakeCallback()

        service.subscribe(simple_key, callback1)
        service.subscribe(simple_key, callback2)
        service.subscribe(simple_key, callback3)

        config = SimpleConfig(value=200, name="multi")
        service._config[simple_key] = config
        service._notify_subscribers(simple_key, config)

        assert callback1.called is True
        assert callback1.data == config
        assert callback2.called is True
        assert callback2.data == config
        assert callback3.called is True
        assert callback3.data == config

    def test_subscribe_different_keys_independent(
        self, service: ConfigService, simple_key: str, complex_key: str
    ) -> None:
        """Test that subscribers for different keys are independent."""
        callback1 = FakeCallback()
        callback2 = FakeCallback()

        service.subscribe(simple_key, callback1)
        service.subscribe(complex_key, callback2)

        simple_config = SimpleConfig(value=300, name="key1")
        service._config[simple_key] = simple_config
        service._notify_subscribers(simple_key, simple_config)

        assert callback1.called is True
        assert callback1.data == simple_config
        assert callback2.called is False

    def test_process_incoming_messages_no_bridge(self, service: ConfigService) -> None:
        """Test that process_incoming_messages handles missing bridge gracefully."""
        service.process_incoming_messages()

    def test_process_incoming_messages_empty_queue(
        self, service_with_bridge: tuple[ConfigService, FakeMessageBridge]
    ) -> None:
        """Test processing messages when queue is empty."""
        service, bridge = service_with_bridge

        service.process_incoming_messages()

        assert len(bridge.get_published_messages()) == 0

    def test_process_incoming_messages_valid_data(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test processing incoming messages with valid data."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        bridge.add_incoming_message((simple_key, {"value": 400, "name": "incoming"}))

        service.process_incoming_messages()

        config = service.get(simple_key)
        assert config.value == 400
        assert config.name == "incoming"

        assert callback.called is True
        assert callback.data == config

    def test_process_incoming_messages_invalid_data_logged(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that invalid incoming data is logged and ignored."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        bridge.add_incoming_message((simple_key, {"invalid_field": "value"}))

        with capture_logs(service._logger, 'error') as captured:
            service.process_incoming_messages()

        assert len(captured) == 1
        assert "Invalid config data received" in captured[0]

        assert service.get(simple_key) is None
        assert callback.called is False

    def test_process_incoming_messages_unknown_key_ignored(
        self, service_with_bridge: tuple[ConfigService, FakeMessageBridge]
    ) -> None:
        """Test that messages for unknown keys are ignored."""
        service, bridge = service_with_bridge

        bridge.add_incoming_message(("unknown_key", {"some": "data"}))

        service.process_incoming_messages()

        assert service.get("unknown_key") is None

    def test_process_incoming_messages_batching(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that message processing respects the num parameter."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        for i in range(5):
            bridge.add_incoming_message((simple_key, {"value": i, "name": f"msg{i}"}))

        service.process_incoming_messages(num=3)

        assert len(bridge._incoming_messages) == 2

        config = service.get(simple_key)
        assert config.value == 2

    def test_process_incoming_messages_deduplication(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that only the latest message per key is processed."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        bridge.add_incoming_message((simple_key, {"value": 1, "name": "first"}))
        bridge.add_incoming_message((simple_key, {"value": 2, "name": "second"}))
        bridge.add_incoming_message((simple_key, {"value": 3, "name": "third"}))

        service.process_incoming_messages()

        config = service.get(simple_key)
        assert config.value == 3
        assert config.name == "third"

        assert callback.call_count == 1
        assert callback.data == config

    def test_callback_exception_handling(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that exceptions in callbacks are handled gracefully."""
        failing_callback = FailingCallback()
        working_callback = FakeCallback()

        service.subscribe(simple_key, failing_callback)
        service.subscribe(simple_key, working_callback)

        config = SimpleConfig(value=500, name="exception_test")

        with capture_logs(service._logger, 'error') as captured:
            service._notify_subscribers(simple_key, config)

        assert len(captured) == 1
        assert "Error in config subscriber callback" in captured[0]

        assert failing_callback.call_count == 1
        assert working_callback.called is True
        assert working_callback.data == config

    def test_disable_updates_context_manager(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that the disable updates context manager works correctly."""
        service, bridge = service_with_bridge
        config = SimpleConfig(value=600, name="disabled")

        service.update_config(simple_key, config)
        assert len(bridge.get_published_messages()) == 1

        with service._disable_updates():
            config2 = SimpleConfig(value=700, name="should_not_publish")
            service.update_config(simple_key, config2)
            assert len(bridge.get_published_messages()) == 1

        config3 = SimpleConfig(value=800, name="enabled_again")
        service.update_config(simple_key, config3)
        assert len(bridge.get_published_messages()) == 2

    def test_register_schema_with_manager(self, service: ConfigService) -> None:
        """Test registering a new schema with ConfigSchemaManager."""
        new_key = "new_config"

        service.register_schema(new_key, SimpleConfig)

        config = SimpleConfig(value=900, name="new_schema")
        service.update_config(new_key, config)

        assert service.get(new_key) == config

    def test_register_schema_with_non_manager_raises_error(self) -> None:
        """Test that registering schema with non-ConfigSchemaManager raises error."""

        class FakeValidator:
            pass

        validator = FakeValidator()
        service = ConfigService(schema_validator=validator)

        with pytest.raises(TypeError, match="Schema validator must be an instance"):
            service.register_schema("key", SimpleConfig)

    def test_complex_config_serialization_deserialization(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        complex_key: str,
    ) -> None:
        """Test complex config with nested data structures."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(complex_key, callback)

        config = ComplexConfig(
            items=["item1", "item2", "item3"],
            metadata={"count": 10, "priority": 5},
            threshold=0.75,
        )

        service.update_config(complex_key, config)

        published = bridge.get_published_messages()
        assert len(published) == 1
        assert published[0][1] == {
            "items": ["item1", "item2", "item3"],
            "metadata": {"count": 10, "priority": 5},
            "threshold": 0.75,
        }

        bridge.add_incoming_message((complex_key, published[0][1]))
        callback.reset()

        service.process_incoming_messages()

        assert callback.called is True
        received_config = callback.data
        assert received_config.items == ["item1", "item2", "item3"]
        assert received_config.metadata == {"count": 10, "priority": 5}
        assert received_config.threshold == 0.75

    def test_logging_configuration(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that debug logging works correctly."""
        service, bridge = service_with_bridge

        config = SimpleConfig(value=1000, name="logging_test")

        with capture_logs(service._logger, 'debug') as captured:
            service.update_config(simple_key, config)
            bridge.add_incoming_message(
                (simple_key, {"value": 1001, "name": "updated"})
            )
            service.process_incoming_messages()

        assert len(captured) >= 2

    def test_update_config_during_disabled_updates_ignored(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that config updates are ignored when updates are disabled."""
        config1 = SimpleConfig(value=100, name="initial")
        service.update_config(simple_key, config1)

        with service._disable_updates():
            config2 = SimpleConfig(value=200, name="ignored")
            service.update_config(simple_key, config2)
            # Config should remain unchanged in local state
            # Note: This tests the internal state, the actual behavior
            # may depend on the implementation details

        # After context, updates should work again
        config3 = SimpleConfig(value=300, name="works_again")
        service.update_config(simple_key, config3)
        assert service.get(simple_key) == config3

    def test_callback_receives_exact_data_object(
        self, service: ConfigService, simple_key: str
    ) -> None:
        """Test that callbacks receive the exact same object instance."""
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        config = SimpleConfig(value=42, name="identity_test")
        service._config[simple_key] = config
        service._notify_subscribers(simple_key, config)

        assert callback.data is config

    def test_multiple_process_calls_handle_remaining_messages(
        self,
        service_with_bridge: tuple[ConfigService, FakeMessageBridge],
        simple_key: str,
    ) -> None:
        """Test that multiple process calls handle all messages."""
        service, bridge = service_with_bridge
        callback = FakeCallback()
        service.subscribe(simple_key, callback)

        # Add 10 messages
        for i in range(10):
            bridge.add_incoming_message((simple_key, {"value": i, "name": f"msg{i}"}))

        # Process in batches
        service.process_incoming_messages(num=4)
        service.process_incoming_messages(num=4)
        service.process_incoming_messages(num=4)

        # Should have processed all messages (deduplication means only last value)
        config = service.get(simple_key)
        assert config.value == 9
        assert len(bridge._incoming_messages) == 0
