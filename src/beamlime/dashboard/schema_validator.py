# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import pydantic

from beamlime.config.schema_registry import SchemaRegistryBase

K = TypeVar('K')
V = TypeVar('V')
Serialized = TypeVar('Serialized')


class ModelSerializer(ABC, Generic[V, Serialized]):
    """
    Base for serializing and deserializing models.

    This allows for keeping SchemaValidator generic over the serialization format and
    model type.
    """

    @abstractmethod
    def serialize(self, data: V) -> Serialized:
        """Serialize a model to its serialized representation."""

    @abstractmethod
    def deserialize(self, *, data: Serialized, model: type[V]) -> V:
        """Deserialize data to a model."""


JSONSerialized = dict[str, Any]


class PydanticSerializer(ModelSerializer[pydantic.BaseModel, JSONSerialized]):
    """Serializer for pydantic models to/from JSON dictionaries."""

    def serialize(self, data: pydantic.BaseModel) -> JSONSerialized:
        """Serialize a pydantic model to a dictionary."""
        return data.model_dump(mode='json')

    def deserialize(
        self, *, data: JSONSerialized, model: type[pydantic.BaseModel]
    ) -> pydantic.BaseModel:
        """Deserialize JSON data to a pydantic model."""
        return model.model_validate(data)


class SchemaValidator(Generic[K, Serialized, V]):
    """Validator for configuration data against schemas."""

    def __init__(
        self,
        *,
        schema_registry: SchemaRegistryBase[K, V],
        serializer: ModelSerializer[V, Serialized],
    ) -> None:
        self._schema_registry = schema_registry
        self._serializer = serializer

    def validate(self, key: K, value: V) -> bool:
        """Check if a schema is registered for the given key."""
        model = self._schema_registry.get_model(key)
        if model is None:
            return False
        return isinstance(value, model)

    def deserialize(self, key: K, data: Serialized) -> V | None:
        """Validate configuration data."""
        model = self._schema_registry.get_model(key)
        if model is None:
            return None
        return self._serializer.deserialize(data=data, model=model)

    def serialize(self, data: V) -> Serialized:
        """Serialize a model to its serialized representation."""
        return self._serializer.serialize(data)


def PydanticSchemaValidator(
    schema_registry: SchemaRegistryBase[K, pydantic.BaseModel],
) -> SchemaValidator[K, JSONSerialized, pydantic.BaseModel]:
    """Helper function to create a SchemaValidator for pydantic models."""
    return SchemaValidator(
        schema_registry=schema_registry, serializer=PydanticSerializer()
    )
