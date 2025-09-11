# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pydantic
import pytest

from ess.livedata.config.schema_registry import FakeSchemaRegistry
from ess.livedata.dashboard.schema_validator import PydanticSchemaValidator
from ess.livedata.parameter_models import TOARange


class SimpleModel(pydantic.BaseModel):
    name: str
    value: int


class AnotherModel(pydantic.BaseModel):
    enabled: bool
    data: str


@pytest.fixture
def empty_validator():
    registry = FakeSchemaRegistry()
    return PydanticSchemaValidator(registry)


@pytest.fixture
def populated_validator():
    registry = FakeSchemaRegistry(
        {'simple': SimpleModel, 'toa_range': TOARange, 'another': AnotherModel}
    )
    return PydanticSchemaValidator(registry)


class TestSchemaValidator:
    def test_validate_with_registered_schema(self, populated_validator):
        """Test validate method with registered schemas and valid models."""
        simple_instance = SimpleModel(name='test', value=42)
        toa_instance = TOARange(enabled=True, start=1000.0, stop=2000.0, unit='us')

        assert populated_validator.validate('simple', simple_instance) is True
        assert populated_validator.validate('toa_range', toa_instance) is True

    def test_validate_with_wrong_model_type(self, populated_validator):
        """Test validate method with wrong model type."""
        simple_instance = SimpleModel(name='test', value=42)
        toa_instance = TOARange(enabled=True, start=1000.0, stop=2000.0, unit='us')

        # Wrong model for key
        assert populated_validator.validate('simple', toa_instance) is False
        assert populated_validator.validate('toa_range', simple_instance) is False

    def test_validate_with_unregistered_key(self, populated_validator):
        """Test validate method with unregistered key."""
        simple_instance = SimpleModel(name='test', value=42)

        assert populated_validator.validate('nonexistent', simple_instance) is False

    def test_validate_with_non_pydantic_model(self, populated_validator):
        """Test validate method with non-pydantic model."""
        non_model = {'name': 'test', 'value': 42}

        assert populated_validator.validate('simple', non_model) is False

    def test_deserialize_valid_data(self, populated_validator):
        """Test deserialize method with valid data."""
        simple_data = {'name': 'test', 'value': 42}
        toa_data = {'enabled': True, 'start': 1000.0, 'stop': 2000.0, 'unit': 'us'}

        simple_result = populated_validator.deserialize('simple', simple_data)
        toa_result = populated_validator.deserialize('toa_range', toa_data)

        assert isinstance(simple_result, SimpleModel)
        assert simple_result.name == 'test'
        assert simple_result.value == 42

        assert isinstance(toa_result, TOARange)
        assert toa_result.enabled is True
        assert toa_result.start == 1000.0
        assert toa_result.stop == 2000.0
        assert toa_result.unit == 'us'

    def test_deserialize_invalid_data(self, populated_validator):
        """Test deserialize method with invalid data that doesn't match schema."""
        # Missing required field
        invalid_data = {'name': 'test'}  # missing 'value' field

        with pytest.raises(pydantic.ValidationError):
            populated_validator.deserialize('simple', invalid_data)

    def test_deserialize_wrong_data_types(self, populated_validator):
        """Test deserialize method with wrong data types."""
        # Wrong type for value field
        invalid_data = {'name': 'test', 'value': 'not_an_int'}

        with pytest.raises(pydantic.ValidationError):
            populated_validator.deserialize('simple', invalid_data)

    def test_deserialize_unregistered_key(self, populated_validator):
        """Test deserialize method with unregistered key."""
        data = {'name': 'test', 'value': 42}

        result = populated_validator.deserialize('nonexistent', data)

        assert result is None

    def test_serialize_valid_model(self, populated_validator):
        """Test serialize method with valid pydantic model."""
        simple_model = SimpleModel(name='test', value=42)
        toa_model = TOARange(enabled=False, start=500.0, stop=1500.0, unit='ns')

        simple_result = populated_validator.serialize(simple_model)
        toa_result = populated_validator.serialize(toa_model)

        assert simple_result == {'name': 'test', 'value': 42}
        assert toa_result == {
            'enabled': False,
            'start': 500.0,
            'stop': 1500.0,
            'unit': 'ns',
        }

    def test_serialize_model_with_complex_types(self):
        """Test serialize method with model containing complex types."""

        class ComplexModel(pydantic.BaseModel):
            items: list[str]
            metadata: dict[str, int]
            optional_field: str | None = None

        registry = FakeSchemaRegistry({'complex': ComplexModel})
        validator = PydanticSchemaValidator(registry)
        model = ComplexModel(
            items=['a', 'b', 'c'],
            metadata={'count': 3, 'total': 100},
            optional_field=None,
        )

        result = validator.serialize(model)

        assert result == {
            'items': ['a', 'b', 'c'],
            'metadata': {'count': 3, 'total': 100},
            'optional_field': None,
        }

    def test_roundtrip_serialize_deserialize(self, populated_validator):
        """Test that serialize/deserialize roundtrip preserves data."""
        original_model = SimpleModel(name='roundtrip_test', value=123)

        # Serialize to dict
        serialized = populated_validator.serialize(original_model)

        # Deserialize back to model
        deserialized = populated_validator.deserialize('simple', serialized)

        assert deserialized == original_model
        assert deserialized.name == original_model.name
        assert deserialized.value == original_model.value
