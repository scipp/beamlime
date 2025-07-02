# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import pydantic
import pytest

from beamlime.config.schema_registry import FakeSchemaRegistry
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.controller_factory import (
    BinEdgeController,
    Controller,
    ControllerFactory,
)
from beamlime.dashboard.message_bridge import FakeMessageBridge
from beamlime.dashboard.schema_validator import (
    JSONSerialized,
    PydanticSchemaValidator,
    SchemaValidator,
)


# Test models
class SimpleConfig(pydantic.BaseModel):
    value: int = 42
    name: str = "default"
    enabled: bool = True


class RangeConfig(pydantic.BaseModel):
    """Config with range values and unit."""

    low: float = 0.0
    high: float = 1000.0
    unit: str = "us"


class ConfigWithDescription(pydantic.BaseModel):
    value: int = pydantic.Field(default=10, description="A simple integer value")
    name: str = pydantic.Field(default="test", description="A name field")
    factor: float = pydantic.Field(description="A required factor")


class ConfigWithDefaults(pydantic.BaseModel):
    items: list[str] = pydantic.Field(default_factory=list)
    metadata: dict[str, int] = pydantic.Field(default_factory=dict)
    threshold: float = 0.5


class FakeCallback:
    def __init__(self) -> None:
        self.called = False
        self.call_count = 0
        self.call_args: list = []
        self.data: dict | None = None

    def __call__(self, data: dict) -> None:
        self.called = True
        self.call_count += 1
        self.call_args.append(data)
        self.data = data

    def reset(self) -> None:
        self.called = False
        self.call_count = 0
        self.call_args.clear()
        self.data = None


@pytest.fixture
def schema_registry() -> FakeSchemaRegistry:
    return FakeSchemaRegistry(
        {
            "simple": SimpleConfig,
            "range": RangeConfig,
            "with_description": ConfigWithDescription,
            "with_defaults": ConfigWithDefaults,
        }
    )


@pytest.fixture
def schemas(
    schema_registry: FakeSchemaRegistry,
) -> SchemaValidator[str, JSONSerialized, pydantic.BaseModel]:
    return PydanticSchemaValidator(schema_registry=schema_registry)


@pytest.fixture
def config_service(
    schemas: SchemaValidator[str, JSONSerialized, pydantic.BaseModel],
) -> ConfigService:
    return ConfigService(schema_validator=schemas)


@pytest.fixture
def config_service_with_bridge(
    schemas: SchemaValidator[str, JSONSerialized, pydantic.BaseModel],
) -> tuple[ConfigService, FakeMessageBridge]:
    bridge = FakeMessageBridge()
    return ConfigService(schema_validator=schemas, message_bridge=bridge), bridge


@pytest.fixture
def controller_factory(
    config_service: ConfigService, schema_registry: FakeSchemaRegistry
) -> ControllerFactory:
    return ControllerFactory(
        config_service=config_service, schema_registry=schema_registry
    )


@pytest.fixture
def simple_controller(controller_factory: ControllerFactory) -> Controller:
    return controller_factory.create(config_key="simple")


class TestController:
    def test_get_defaults_returns_field_defaults(self) -> None:
        """Test that get_defaults returns the correct default values."""
        schema_registry = FakeSchemaRegistry({"simple": SimpleConfig})
        controller = Controller(
            config_key="simple",
            config_service=ConfigService(
                PydanticSchemaValidator(schema_registry=schema_registry)
            ),
            schema=SimpleConfig,
        )

        defaults = controller.get_defaults()

        assert defaults == {"value": 42, "name": "default", "enabled": True}

    def test_get_defaults_with_factory_defaults(self) -> None:
        """Test get_defaults with default_factory fields."""
        schema_registry = FakeSchemaRegistry({"with_defaults": ConfigWithDefaults})
        controller = Controller(
            config_key="with_defaults",
            config_service=ConfigService(
                PydanticSchemaValidator(schema_registry=schema_registry)
            ),
            schema=ConfigWithDefaults,
        )

        defaults = controller.get_defaults()

        # Factory defaults should be converted from PydanticUndefined to actual defaults
        assert defaults["items"] == []
        assert defaults["metadata"] == {}
        assert defaults["threshold"] == 0.5

    def test_get_defaults_with_none_defaults(self) -> None:
        """Test get_defaults when fields have no defaults."""
        schema_registry = FakeSchemaRegistry(
            {"with_description": ConfigWithDescription}
        )
        controller = Controller(
            config_key="with_description",
            config_service=ConfigService(
                PydanticSchemaValidator(schema_registry=schema_registry)
            ),
            schema=ConfigWithDescription,
        )

        defaults = controller.get_defaults()

        assert defaults["value"] == 10
        assert defaults["name"] == "test"
        # Required fields without defaults should be None
        assert defaults["factor"] is None

    def test_get_descriptions_returns_field_descriptions(self) -> None:
        """Test that get_descriptions returns field descriptions."""
        schema_registry = FakeSchemaRegistry(
            {"with_description": ConfigWithDescription}
        )
        controller = Controller(
            config_key="with_description",
            config_service=ConfigService(
                PydanticSchemaValidator(schema_registry=schema_registry)
            ),
            schema=ConfigWithDescription,
        )

        descriptions = controller.get_descriptions()

        assert descriptions["value"] == "A simple integer value"
        assert descriptions["name"] == "A name field"
        assert descriptions["factor"] == "A required factor"

    def test_get_descriptions_with_no_descriptions(self) -> None:
        """Test get_descriptions when fields have no descriptions."""
        schema_registry = FakeSchemaRegistry({"simple": SimpleConfig})
        controller = Controller(
            config_key="simple",
            config_service=ConfigService(
                PydanticSchemaValidator(schema_registry=schema_registry)
            ),
            schema=SimpleConfig,
        )

        descriptions = controller.get_descriptions()

        assert descriptions == {"value": None, "name": None, "enabled": None}

    def test_set_value_updates_config_service(
        self, simple_controller: Controller, config_service: ConfigService
    ) -> None:
        """Test that set_value updates the configuration service."""
        simple_controller.set_value(value=100, name="updated", enabled=False)

        config = config_service.get_config("simple")
        assert config.value == 100
        assert config.name == "updated"
        assert config.enabled is False

    def test_set_value_validates_data(self, simple_controller: Controller) -> None:
        """Test that set_value validates data against schema."""
        with pytest.raises(pydantic.ValidationError):
            simple_controller.set_value(value="not_an_int", name="test")

    def test_subscribe_registers_callback(self) -> None:
        """Test that subscribe registers a callback for config changes."""
        # Use config service with bridge for callback testing
        schema_registry = FakeSchemaRegistry({"simple": SimpleConfig})
        schemas = PydanticSchemaValidator(schema_registry=schema_registry)
        bridge = FakeMessageBridge()
        config_service = ConfigService(schema_validator=schemas, message_bridge=bridge)

        factory = ControllerFactory(
            config_service=config_service, schema_registry=schema_registry
        )
        controller = factory.create(config_key="simple")

        callback = FakeCallback()
        controller.subscribe(callback)

        # Simulate external config update through message bridge
        bridge.add_incoming_message(
            ("simple", {"value": 150, "name": "external", "enabled": True})
        )
        config_service.process_incoming_messages()

        # The callback should be triggered when external config is processed
        assert callback.called is True
        assert callback.data == {"value": 150, "name": "external", "enabled": True}

    def test_subscribe_callback_with_existing_config(
        self, simple_controller: Controller, config_service: ConfigService
    ) -> None:
        """Test subscribe callback is called immediately if config exists."""
        # Set up existing config through public interface
        config = SimpleConfig(value=75, name="existing", enabled=False)
        config_service.update_config("simple", config)

        callback = FakeCallback()
        simple_controller.subscribe(callback)

        assert callback.called is True
        assert callback.data == {"value": 75, "name": "existing", "enabled": False}

    def test_subscribe_prevents_update_cycles(
        self, simple_controller: Controller, config_service: ConfigService
    ) -> None:
        """Test that subscribe callbacks don't trigger update cycles."""
        cycle_detected = False

        def cycling_callback(data: dict) -> None:
            nonlocal cycle_detected
            if cycle_detected:
                pytest.fail("Update cycle was not prevented")
            cycle_detected = True
            # This would normally cause an infinite loop
            simple_controller.set_value(**data)
            cycle_detected = False

        simple_controller.subscribe(cycling_callback)

        # Simulate external config update
        config = SimpleConfig(value=300, name="cycle_test", enabled=True)
        config_service.update_config("simple", config)

        # Should not cause infinite recursion
        stored_config = config_service.get_config("simple")
        assert stored_config.value == 300

    def test_preprocess_value_default_implementation(
        self, simple_controller: Controller, config_service: ConfigService
    ) -> None:
        """Test that _preprocess_value can be tested through set_value behavior."""
        simple_controller.set_value(value=123, name="test", enabled=True)

        # If preprocessing worked correctly, the value should be set
        config = config_service.get_config("simple")
        assert config.value == 123
        assert config.name == "test"
        assert config.enabled is True

    def test_preprocess_config_default_implementation(self) -> None:
        """Test that _preprocess_config can be tested through callback behavior."""
        # Use config service with bridge for callback testing
        schema_registry = FakeSchemaRegistry({"simple": SimpleConfig})
        schemas = PydanticSchemaValidator(schema_registry=schema_registry)
        bridge = FakeMessageBridge()
        config_service = ConfigService(schema_validator=schemas, message_bridge=bridge)

        factory = ControllerFactory(
            config_service=config_service, schema_registry=schema_registry
        )
        controller = factory.create(config_key="simple")

        callback = FakeCallback()
        controller.subscribe(callback)

        # Simulate external update through message bridge
        bridge.add_incoming_message(
            ("simple", {"value": 456, "name": "test", "enabled": False})
        )
        config_service.process_incoming_messages()

        # Callback should be triggered with the updated config
        assert callback.called is True
        assert callback.data == {"value": 456, "name": "test", "enabled": False}

    def test_multiple_subscribes_replace_callback(
        self, controller_factory: ControllerFactory
    ) -> None:
        """Test that multiple subscribe calls replace the previous callback."""
        # Use config service with bridge for callback testing
        schema_registry = FakeSchemaRegistry({"simple": SimpleConfig})
        schemas = PydanticSchemaValidator(schema_registry=schema_registry)
        bridge = FakeMessageBridge()
        config_service = ConfigService(schema_validator=schemas, message_bridge=bridge)

        factory = ControllerFactory(
            config_service=config_service, schema_registry=schema_registry
        )
        controller = factory.create(config_key="simple")

        callback1 = FakeCallback()
        callback2 = FakeCallback()

        controller.subscribe(callback1)
        controller.subscribe(callback2)  # This should replace callback1

        # Simulate external update through message bridge
        bridge.add_incoming_message(
            ("simple", {"value": 999, "name": "multi", "enabled": False})
        )
        config_service.process_incoming_messages()

        # Only the last callback should be called
        assert callback1.called is False
        assert callback2.called is True
        assert callback2.data == {"value": 999, "name": "multi", "enabled": False}


class TestBinEdgeController:
    @pytest.fixture
    def bin_edge_controller(self, config_service: ConfigService) -> BinEdgeController:
        return BinEdgeController(
            config_key="range",
            config_service=config_service,
            schema=RangeConfig,
        )

    def test_unit_conversion_on_value_change(
        self, bin_edge_controller: BinEdgeController, config_service: ConfigService
    ) -> None:
        """Test unit conversion when unit changes."""
        # Set initial config with microseconds
        initial_config = RangeConfig(low=1000.0, high=2000.0, unit="us")
        config_service.update_config("range", initial_config)

        callback = FakeCallback()
        bin_edge_controller.subscribe(callback)
        callback.reset()  # Clear the initial callback from subscription

        # Change unit to milliseconds - values should be converted
        bin_edge_controller.set_value(low=1000.0, high=2000.0, unit="ms")

        stored_config = config_service.get_config("range")
        assert stored_config.unit == "ms"
        # 1000 us = 1 ms, 2000 us = 2 ms
        assert stored_config.low == 1.0
        assert stored_config.high == 2.0

    def test_no_conversion_same_unit(
        self, bin_edge_controller: BinEdgeController, config_service: ConfigService
    ) -> None:
        """Test no conversion when unit stays the same."""
        # Set initial config
        initial_config = RangeConfig(low=500.0, high=1500.0, unit="ms")
        config_service.update_config("range", initial_config)

        callback = FakeCallback()
        bin_edge_controller.subscribe(callback)
        callback.reset()  # Clear the initial callback

        # Update values with same unit
        bin_edge_controller.set_value(low=600.0, high=1600.0, unit="ms")

        stored_config = config_service.get_config("range")
        assert stored_config.low == 600.0
        assert stored_config.high == 1600.0
        assert stored_config.unit == "ms"

    def test_unit_conversion_precision(
        self, bin_edge_controller: BinEdgeController, config_service: ConfigService
    ) -> None:
        """Test unit conversion handles floating point precision correctly."""
        # Set initial config with microseconds
        initial_config = RangeConfig(low=1000.0, high=2000.0, unit="us")
        config_service.update_config("range", initial_config)

        callback = FakeCallback()
        bin_edge_controller.subscribe(callback)
        callback.reset()  # Clear initial callback

        # Convert to milliseconds
        bin_edge_controller.set_value(low=1000.0, high=2000.0, unit="ms")

        stored_config = config_service.get_config("range")
        # 1000 us = 1.0 ms, 2000 us = 2.0 ms
        assert stored_config.low == 1.0
        assert stored_config.high == 2.0

    def test_initial_unit_handling(
        self, bin_edge_controller: BinEdgeController, config_service: ConfigService
    ) -> None:
        """Test handling when no previous unit exists."""
        # First set_value call with no previous config
        bin_edge_controller.set_value(low=500.0, high=1500.0, unit="ns")

        stored_config = config_service.get_config("range")
        assert stored_config.low == 500.0
        assert stored_config.high == 1500.0
        assert stored_config.unit == "ns"


class TestControllerFactory:
    def test_create_controller_with_valid_key(
        self, controller_factory: ControllerFactory
    ) -> None:
        """Test creating a controller with a valid configuration key."""
        controller = controller_factory.create(config_key="simple")

        assert isinstance(controller, Controller)

    def test_create_controller_with_custom_class(
        self, controller_factory: ControllerFactory
    ) -> None:
        """Test creating a controller with a custom controller class."""
        controller = controller_factory.create(
            config_key="range", controller_cls=BinEdgeController
        )

        assert isinstance(controller, BinEdgeController)

    def test_create_controller_with_invalid_key_raises_error(
        self, controller_factory: ControllerFactory
    ) -> None:
        """Test that creating a controller with invalid key raises KeyError."""
        with pytest.raises(KeyError, match="No schema registered for invalid_key"):
            controller_factory.create(config_key="invalid_key")

    def test_create_multiple_controllers(
        self, controller_factory: ControllerFactory
    ) -> None:
        """Test creating multiple controllers for different keys."""
        controller1 = controller_factory.create(config_key="simple")
        controller2 = controller_factory.create(config_key="with_defaults")

        # They should be different instances
        assert controller1 is not controller2

    def test_created_controller_integration(
        self, controller_factory: ControllerFactory, config_service: ConfigService
    ) -> None:
        """Test that created controllers integrate properly with config service."""
        controller = controller_factory.create(config_key="simple")
        callback = FakeCallback()
        controller.subscribe(callback)

        # Update through controller
        controller.set_value(value=777, name="integration", enabled=True)

        # Check config service has the update
        config = config_service.get_config("simple")
        assert config.value == 777
        assert config.name == "integration"
        assert config.enabled is True

    def test_factory_uses_same_config_service(
        self, controller_factory: ControllerFactory, config_service: ConfigService
    ) -> None:
        """Test that all controllers from factory use the same config service."""
        controller1 = controller_factory.create(config_key="simple")
        controller2 = controller_factory.create(config_key="with_defaults")

        # Test they work with same service by checking they can see each other's updates
        controller1.set_value(value=111, name="test1", enabled=True)
        controller2.set_value(items=["item1"], metadata={"key": 1}, threshold=0.8)

        config1 = config_service.get_config("simple")
        config2 = config_service.get_config("with_defaults")

        assert config1.value == 111
        assert config2.threshold == 0.8


class TestIntegration:
    def test_controller_with_message_bridge_integration(
        self, config_service_with_bridge: tuple[ConfigService, FakeMessageBridge]
    ) -> None:
        """Test controller integration with message bridge."""
        config_service, bridge = config_service_with_bridge

        # Create factory and controller
        registry = FakeSchemaRegistry({"simple": SimpleConfig})
        factory = ControllerFactory(
            config_service=config_service, schema_registry=registry
        )
        controller = factory.create(config_key="simple")

        # Subscribe to changes
        callback = FakeCallback()
        controller.subscribe(callback)

        # Update through controller - should publish to bridge
        controller.set_value(value=888, name="bridge_test", enabled=False)

        published = bridge.get_published_messages()
        assert len(published) == 1
        assert published[0][0] == "simple"
        assert published[0][1] == {
            "value": 888,
            "name": "bridge_test",
            "enabled": False,
        }

    def test_end_to_end_config_flow(
        self, config_service_with_bridge: tuple[ConfigService, FakeMessageBridge]
    ) -> None:
        """Test complete end-to-end configuration flow."""
        config_service, bridge = config_service_with_bridge

        # Set up controller
        registry = FakeSchemaRegistry({"simple": SimpleConfig})
        factory = ControllerFactory(
            config_service=config_service, schema_registry=registry
        )
        controller = factory.create(config_key="simple")

        callback = FakeCallback()
        controller.subscribe(callback)

        # 1. Local update through controller
        controller.set_value(value=111, name="local", enabled=True)

        # 2. External update through bridge
        bridge.add_incoming_message(
            ("simple", {"value": 222, "name": "external", "enabled": False})
        )
        config_service.process_incoming_messages()

        # Check final state
        config = config_service.get_config("simple")
        assert config.value == 222
        assert config.name == "external"
        assert config.enabled is False

        # Callback should have been triggered for external update
        assert callback.call_count >= 1
        assert callback.data == {"value": 222, "name": "external", "enabled": False}
