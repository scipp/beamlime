# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause

"""Registry for configuration keys with type safety and documentation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass, field
from typing import Generic, TypeVar

import pydantic

from .models import ConfigKey

KeyType = TypeVar('KeyType')
SchemaType = TypeVar('SchemaType')


@dataclass(frozen=True)
class ConfigItemSpec(Generic[SchemaType]):
    """
    A configuration key specification with associated type and metadata.
    """

    key: str
    service_name: str | None
    model: type[SchemaType]
    description: str = ""
    produced_by: set[str] = field(default_factory=set)
    consumed_by: set[str] = field(default_factory=set)

    def create_key(self, source_name: str | None = None) -> ConfigKey:
        """Create a ConfigKey instance for a specific source."""
        return ConfigKey(
            source_name=source_name, service_name=self.service_name, key=self.key
        )


class SchemaRegistryBase(ABC, Generic[KeyType, SchemaType]):
    """Interface needed by SchemaValidator."""

    @abstractmethod
    def get_model(self, config_key: KeyType) -> type[SchemaType] | None:
        """Get the model type for a given configuration key."""


class SchemaRegistry(SchemaRegistryBase[ConfigKey, SchemaType]):
    """Central registry for all configuration keys in the application."""

    def __init__(self) -> None:
        self._keys: dict[tuple[str | None, str], ConfigItemSpec] = {}

    def create(
        self,
        key: str,
        service_name: str | None,
        model: type[SchemaType],
        description: str = "",
        produced_by: set[str] | None = None,
        consumed_by: set[str] | None = None,
    ) -> ConfigItemSpec[SchemaType]:
        """
        Create and register a configuration key specification in one step.
        """
        spec = ConfigItemSpec(
            key=key,
            service_name=service_name,
            model=model,
            description=description,
            produced_by=produced_by or set(),
            consumed_by=consumed_by or set(),
        )
        return self.register(spec)

    def register(self, spec: ConfigItemSpec) -> ConfigItemSpec:
        """Register a configuration key specification."""
        key_id = (spec.service_name, spec.key)
        if key_id in self._keys:
            raise ValueError(f"Key {spec.service_name}/{spec.key} already registered.")
        self._keys[key_id] = spec
        return spec

    def get_spec(self, service_name: str | None, key: str) -> ConfigItemSpec | None:
        """Get a registered key specification."""
        return self._keys.get((service_name, key))

    def get_model(self, config_key: ConfigKey) -> type[SchemaType] | None:
        """Get the model type for a given configuration key."""
        spec = self.get_spec(config_key.service_name, config_key.key)
        return spec.model if spec else None

    def list_specs(self, service_name: str | None = None) -> list[ConfigItemSpec]:
        """List all registered key specifications, optionally filtered by service."""
        if service_name is None:
            return list(self._keys.values())
        return [
            spec for spec in self._keys.values() if spec.service_name == service_name
        ]

    def get_produced_keys(self, service_name: str) -> list[ConfigItemSpec]:
        """Get all key specifications for keys produced by a service."""
        return [
            spec for spec in self._keys.values() if service_name in spec.produced_by
        ]

    def get_consumed_keys(self, service_name: str) -> list[ConfigItemSpec]:
        """Get all key specifications for keys consumed by a service."""
        return [
            spec for spec in self._keys.values() if service_name in spec.consumed_by
        ]


# Global registry instance
_registry = SchemaRegistry()


def get_schema_registry() -> SchemaRegistry:
    """Get the global configuration key registry."""
    return _registry


class FakeSchemaRegistry(
    UserDict[str, type[pydantic.BaseModel]], SchemaRegistryBase[str, pydantic.BaseModel]
):
    """A simple schema registry for testing purposes."""

    def get_model(self, config_key: str) -> type[pydantic.BaseModel] | None:
        return self.get(config_key)
