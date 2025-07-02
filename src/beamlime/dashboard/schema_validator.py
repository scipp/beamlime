# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any, Generic, Protocol, TypeVar

import pydantic

from beamlime.config import ConfigKey
from beamlime.config.schema_registry import SchemaRegistry

K = TypeVar('K')
V = TypeVar('V')
Serialized = TypeVar('Serialized')


class ConfigSchemaValidator(Protocol, Generic[K, Serialized, V]):
    """Protocol for validating configuration data against schemas."""

    def validate(self, key: K, value: V) -> bool:
        """Check if a schema is registered for the given key."""

    def deserialize(self, key: K, data: Serialized) -> V | None:
        """Validate configuration data."""

    def serialize(self, data: V) -> Serialized:
        """Serialize a pydantic model to a dictionary."""


JSONSerialized = dict[str, Any]


class SchemaValidator(
    ConfigSchemaValidator[ConfigKey, JSONSerialized, pydantic.BaseModel],
):
    def __init__(self, schema_registry: SchemaRegistry) -> None:
        self._schema_registry = schema_registry

    def validate(self, key: ConfigKey, value: pydantic.BaseModel) -> bool:
        model = self._schema_registry.get_model(key)
        if model is None:
            return False
        return isinstance(value, model)

    def deserialize(
        self, key: ConfigKey, data: JSONSerialized
    ) -> pydantic.BaseModel | None:
        """Validate configuration data."""
        model = self._schema_registry.get_model(key)
        if model is None:
            return None
        return model.model_validate(data)

    def serialize(self, data: pydantic.BaseModel) -> JSONSerialized:
        """Serialize a pydantic model to a dictionary."""
        return data.model_dump(mode='json')
