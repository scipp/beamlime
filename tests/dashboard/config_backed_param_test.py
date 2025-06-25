# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import param
import pydantic
import pytest

from beamlime.config import models
from beamlime.dashboard.config_backed_param import (
    ConfigBackedParam,
    DataReductionParam,
    DetectorDataParam,
    MonitorDataParam,
)
from beamlime.dashboard.config_service import (
    ConfigSchemaManager,
    ConfigService,
    LoopbackMessageBridge,
)


class MySchema(pydantic.BaseModel):
    value: int
    name: str = "test"


class ConcreteConfigBackedParam(ConfigBackedParam):
    value = param.Integer(default=42)
    name = param.String(default="test")

    @property
    def service_name(self) -> str:
        return "test_service"

    @property
    def config_key_name(self) -> str:
        return "test_key"

    @property
    def schema(self) -> type[pydantic.BaseModel]:
        return MySchema

    def panel(self):
        return "mock_panel"


class AbstractConfigBackedParam(ConfigBackedParam):
    pass


class TestConfigBackedParam:
    def test_from_pydantic_returns_callable(self):
        param_obj = ConcreteConfigBackedParam()
        callback = param_obj.from_pydantic()
        assert callable(callback)

    def test_from_pydantic_callback_updates_params(self):
        param_obj = ConcreteConfigBackedParam()
        callback = param_obj.from_pydantic()

        model = MySchema(value=100, name="updated")
        callback(model)

        assert param_obj.value == 100
        assert param_obj.name == "updated"

    def test_config_key_property(self):
        param_obj = ConcreteConfigBackedParam()
        key = param_obj.config_key

        assert isinstance(key, models.ConfigKey)
        assert key.service_name == "test_service"
        assert key.key == "test_key"

    def test_abstract_methods_raise_not_implemented(self):
        param_obj = AbstractConfigBackedParam()

        with pytest.raises(NotImplementedError, match="service_name"):
            _ = param_obj.service_name

        with pytest.raises(NotImplementedError, match="config_key_name"):
            _ = param_obj.config_key_name

        with pytest.raises(NotImplementedError, match="schema"):
            _ = param_obj.schema

        with pytest.raises(NotImplementedError, match="panel"):
            param_obj.panel()

    def test_subscribe_registers_schema_and_callback(self):
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        config_service = ConfigService(schema_manager)

        param_obj.subscribe(config_service)

        key = param_obj.config_key
        assert key in schema_manager
        assert schema_manager[key] == MySchema

    def test_subscribe_updates_from_config_service(self):
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        bridge = LoopbackMessageBridge()
        config_service = ConfigService(schema_manager, bridge)

        param_obj.subscribe(config_service)

        # Update via config service
        key = param_obj.config_key
        model = MySchema(value=200, name="from_service")
        config_service.update_config(key, model)
        config_service.process_incoming_messages()

        assert param_obj.value == 200
        assert param_obj.name == "from_service"

    def test_subscribe_publishes_param_changes_to_config_service(self):
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        config_service = ConfigService(schema_manager)

        param_obj.subscribe(config_service)

        # Change param values
        param_obj.value = 300
        param_obj.name = "changed"

        # Check config service was updated
        key = param_obj.config_key
        stored_config = config_service.get(key)
        assert stored_config is not None
        assert stored_config.value == 300
        assert stored_config.name == "changed"

    def test_subscribe_with_existing_config_updates_params(self):
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        config_service = ConfigService(schema_manager)

        # Set config before subscribing
        key = param_obj.config_key
        schema_manager[key] = MySchema
        model = MySchema(value=999, name="existing")
        config_service._config[key] = model

        param_obj.subscribe(config_service)

        assert param_obj.value == 999
        assert param_obj.name == "existing"

    def test_from_pydantic_with_partial_model_data(self):
        param_obj = ConcreteConfigBackedParam()
        callback = param_obj.from_pydantic()

        # Model with only some fields (using default for name)
        model = MySchema(value=50)
        callback(model)

        assert param_obj.value == 50
        assert param_obj.name == "test"  # default value

    def test_multiple_param_changes_trigger_config_updates(self):
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        config_service = ConfigService(schema_manager)

        param_obj.subscribe(config_service)

        # Multiple changes
        param_obj.value = 111
        param_obj.name = "first"
        param_obj.value = 222
        param_obj.name = "second"

        # Should have the latest values
        key = param_obj.config_key
        stored_config = config_service.get(key)
        assert stored_config.value == 222
        assert stored_config.name == "second"


class TestMonitorDataParam:
    def test_service_name(self):
        class ConcreteMonitorParam(MonitorDataParam):
            @property
            def config_key_name(self) -> str:
                return "test"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                return MySchema

            def panel(self):
                return "panel"

        param_obj = ConcreteMonitorParam()
        assert param_obj.service_name == "monitor_data"


class TestDetectorDataParam:
    def test_service_name(self):
        class ConcreteDetectorParam(DetectorDataParam):
            @property
            def config_key_name(self) -> str:
                return "test"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                return MySchema

            def panel(self):
                return "panel"

        param_obj = ConcreteDetectorParam()
        assert param_obj.service_name == "detector_data"


class TestDataReductionParam:
    def test_service_name(self):
        class ConcreteReductionParam(DataReductionParam):
            @property
            def config_key_name(self) -> str:
                return "test"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                return MySchema

            def panel(self):
                return "panel"

        param_obj = ConcreteReductionParam()
        assert param_obj.service_name == "data_reduction"


class TestConfigBackedParamIntegration:
    def test_bidirectional_sync_between_param_and_config_service(self):
        """Test that changes flow both ways between param and config service."""
        param_obj = ConcreteConfigBackedParam()
        schema_manager = ConfigSchemaManager()
        bridge = LoopbackMessageBridge()
        config_service = ConfigService(schema_manager, bridge)

        param_obj.subscribe(config_service)
        key = param_obj.config_key

        # Change param -> should update config service
        param_obj.value = 500
        stored = config_service.get(key)
        assert stored.value == 500

        # Change config service -> should update param
        new_model = MySchema(value=600, name="bidirectional")
        config_service.update_config(key, new_model)
        config_service.process_incoming_messages()
        assert param_obj.value == 600
        assert param_obj.name == "bidirectional"

    def test_config_key_uniqueness_across_subclasses(self):
        """Test that different subclasses generate different config keys."""

        class MonitorParam(MonitorDataParam):
            @property
            def config_key_name(self) -> str:
                return "monitor_key"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                return MySchema

            def panel(self):
                return "panel"

        class DetectorParam(DetectorDataParam):
            @property
            def config_key_name(self) -> str:
                return "detector_key"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                return MySchema

            def panel(self):
                return "panel"

        monitor = MonitorParam()
        detector = DetectorParam()

        assert monitor.config_key.service_name == "monitor_data"
        assert detector.config_key.service_name == "detector_data"
        assert monitor.config_key != detector.config_key

    def test_error_handling_in_callback(self):
        """Test that errors in from_pydantic callback don't break the system."""

        class ErrorProneParam(ConfigBackedParam):
            bad_value = param.Integer(default=0)

            @property
            def service_name(self) -> str:
                return "error_service"

            @property
            def config_key_name(self) -> str:
                return "error_key"

            @property
            def schema(self) -> type[pydantic.BaseModel]:
                class BadSchema(pydantic.BaseModel):
                    bad_value: int

                    @pydantic.field_validator('bad_value')
                    @classmethod
                    def validate_bad_value(cls, v):
                        if v < 0:
                            raise ValueError("bad_value must be non-negative")
                        return v

                return BadSchema

            def panel(self):
                return "panel"

        param_obj = ErrorProneParam()
        callback = param_obj.from_pydantic()

        # This should not raise, even with invalid data
        # The validation happens at the pydantic level before reaching callback
        valid_model = param_obj.schema(bad_value=10)
        callback(valid_model)
        assert param_obj.bad_value == 10
