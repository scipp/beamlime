# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pydantic
import pytest

from beamlime.config.models import TOARange
from beamlime.dashboard.config_service import ConfigSchemaManager


class SimpleModel(pydantic.BaseModel):
    name: str
    value: int


class AnotherModel(pydantic.BaseModel):
    enabled: bool
    data: str


@pytest.fixture
def empty_manager():
    return ConfigSchemaManager()


@pytest.fixture
def populated_manager():
    return ConfigSchemaManager(
        {'simple': SimpleModel, 'toa_range': TOARange, 'another': AnotherModel}
    )


class TestConfigSchemaManager:
    def test_init_empty(self, empty_manager):
        """Test initialization with empty schema dictionary."""
        assert len(empty_manager) == 0
        assert list(empty_manager.keys()) == []

    def test_init_with_schemas(self, populated_manager):
        """Test initialization with predefined schemas."""
        assert len(populated_manager) == 3
        assert 'simple' in populated_manager
        assert 'toa_range' in populated_manager
        assert 'another' in populated_manager
        assert populated_manager['simple'] == SimpleModel
        assert populated_manager['toa_range'] == TOARange

    def test_userdict_functionality(self, empty_manager):
        """Test that it behaves like a dictionary."""
        # Add schemas
        empty_manager['key1'] = SimpleModel
        empty_manager['key2'] = AnotherModel

        assert len(empty_manager) == 2
        assert empty_manager['key1'] == SimpleModel
        assert empty_manager.get('key1') == SimpleModel
        assert empty_manager.get('nonexistent') is None

        # Delete schema
        del empty_manager['key1']
        assert len(empty_manager) == 1
        assert 'key1' not in empty_manager

    def test_validate_with_registered_schema(self, populated_manager):
        """Test validate method with registered schemas and valid models."""
        simple_instance = SimpleModel(name='test', value=42)
        toa_instance = TOARange(enabled=True, low=1000.0, high=2000.0, unit='us')

        assert populated_manager.validate('simple', simple_instance) is True
        assert populated_manager.validate('toa_range', toa_instance) is True

    def test_validate_with_wrong_model_type(self, populated_manager):
        """Test validate method with wrong model type."""
        simple_instance = SimpleModel(name='test', value=42)
        toa_instance = TOARange(enabled=True, low=1000.0, high=2000.0, unit='us')

        # Wrong model for key
        assert populated_manager.validate('simple', toa_instance) is False
        assert populated_manager.validate('toa_range', simple_instance) is False

    def test_validate_with_unregistered_key(self, populated_manager):
        """Test validate method with unregistered key."""
        simple_instance = SimpleModel(name='test', value=42)

        assert populated_manager.validate('nonexistent', simple_instance) is False

    def test_validate_with_non_pydantic_model(self, populated_manager):
        """Test validate method with non-pydantic model."""
        non_model = {'name': 'test', 'value': 42}

        assert populated_manager.validate('simple', non_model) is False

    def test_deserialize_valid_data(self, populated_manager):
        """Test deserialize method with valid data."""
        simple_data = {'name': 'test', 'value': 42}
        toa_data = {'enabled': True, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'}

        simple_result = populated_manager.deserialize('simple', simple_data)
        toa_result = populated_manager.deserialize('toa_range', toa_data)

        assert isinstance(simple_result, SimpleModel)
        assert simple_result.name == 'test'
        assert simple_result.value == 42

        assert isinstance(toa_result, TOARange)
        assert toa_result.enabled is True
        assert toa_result.low == 1000.0
        assert toa_result.high == 2000.0
        assert toa_result.unit == 'us'

    def test_deserialize_invalid_data(self, populated_manager):
        """Test deserialize method with invalid data that doesn't match schema."""
        # Missing required field
        invalid_data = {'name': 'test'}  # missing 'value' field

        with pytest.raises(pydantic.ValidationError):
            populated_manager.deserialize('simple', invalid_data)

    def test_deserialize_wrong_data_types(self, populated_manager):
        """Test deserialize method with wrong data types."""
        # Wrong type for value field
        invalid_data = {'name': 'test', 'value': 'not_an_int'}

        with pytest.raises(pydantic.ValidationError):
            populated_manager.deserialize('simple', invalid_data)

    def test_deserialize_unregistered_key(self, populated_manager):
        """Test deserialize method with unregistered key."""
        data = {'name': 'test', 'value': 42}

        result = populated_manager.deserialize('nonexistent', data)

        assert result is None

    def test_serialize_valid_model(self, populated_manager):
        """Test serialize method with valid pydantic model."""
        simple_model = SimpleModel(name='test', value=42)
        toa_model = TOARange(enabled=False, low=500.0, high=1500.0, unit='ns')

        simple_result = populated_manager.serialize(simple_model)
        toa_result = populated_manager.serialize(toa_model)

        assert simple_result == {'name': 'test', 'value': 42}
        assert toa_result == {
            'enabled': False,
            'low': 500.0,
            'high': 1500.0,
            'unit': 'ns',
        }

    def test_serialize_model_with_complex_types(self):
        """Test serialize method with model containing complex types."""

        class ComplexModel(pydantic.BaseModel):
            items: list[str]
            metadata: dict[str, int]
            optional_field: str | None = None

        manager = ConfigSchemaManager({'complex': ComplexModel})
        model = ComplexModel(
            items=['a', 'b', 'c'],
            metadata={'count': 3, 'total': 100},
            optional_field=None,
        )

        result = manager.serialize(model)

        assert result == {
            'items': ['a', 'b', 'c'],
            'metadata': {'count': 3, 'total': 100},
            'optional_field': None,
        }

    def test_roundtrip_serialize_deserialize(self, populated_manager):
        """Test that serialize/deserialize roundtrip preserves data."""
        original_model = SimpleModel(name='roundtrip_test', value=123)

        # Serialize to dict
        serialized = populated_manager.serialize(original_model)

        # Deserialize back to model
        deserialized = populated_manager.deserialize('simple', serialized)

        assert deserialized == original_model
        assert deserialized.name == original_model.name
        assert deserialized.value == original_model.value

    def test_schema_replacement(self, empty_manager):
        """Test that schemas can be replaced."""
        empty_manager['key'] = SimpleModel

        # Replace with different schema
        empty_manager['key'] = AnotherModel

        assert empty_manager['key'] == AnotherModel

        # Old schema should not validate
        simple_instance = SimpleModel(name='test', value=42)
        assert empty_manager.validate('key', simple_instance) is False

        # New schema should validate
        another_instance = AnotherModel(enabled=True, data='test')
        assert empty_manager.validate('key', another_instance) is True
