# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause

"""Registry for configuration keys with type safety and documentation."""

from __future__ import annotations

from typing import TypeVar, Generic
from dataclasses import dataclass, field

from .models import ConfigKey

T = TypeVar('T')


@dataclass(frozen=True)
class TypedConfigKey(Generic[T]):
    """
    A configuration key with associated type information and metadata.

    This provides type safety and serves as documentation for what
    services produce/consume specific configuration values.
    """

    key: str
    service_name: str
    model: type[T]
    description: str = ""
    produced_by: set[str] = field(default_factory=set)
    consumed_by: set[str] = field(default_factory=set)

    def create_key(self, source_name: str | None = None) -> ConfigKey:
        """Create a ConfigKey instance for a specific source."""
        return ConfigKey(
            source_name=source_name, service_name=self.service_name, key=self.key
        )

    def __str__(self) -> str:
        return f"*/{self.service_name}/{self.key}"


class ConfigKeyRegistry:
    """Central registry for all configuration keys in the application."""

    def __init__(self) -> None:
        self._keys: dict[str, TypedConfigKey] = {}

    def register(self, typed_key: TypedConfigKey) -> TypedConfigKey:
        """Register a typed configuration key."""
        key_id = f"{typed_key.service_name}/{typed_key.key}"
        if key_id in self._keys:
            existing = self._keys[key_id]
            if existing.model != typed_key.model:
                raise ValueError(
                    f"Key {key_id} already registered with different type: "
                    f"{existing.model} vs {typed_key.model}"
                )
        self._keys[key_id] = typed_key
        return typed_key

    def get(self, service_name: str, key: str) -> TypedConfigKey | None:
        """Get a registered key."""
        return self._keys.get(f"{service_name}/{key}")

    def list_keys(self, service_name: str | None = None) -> list[TypedConfigKey]:
        """List all registered keys, optionally filtered by service."""
        if service_name is None:
            return list(self._keys.values())
        return [k for k in self._keys.values() if k.service_name == service_name]

    def get_producers(self, service_name: str) -> list[TypedConfigKey]:
        """Get all keys produced by a service."""
        return [k for k in self._keys.values() if service_name in k.produced_by]

    def get_consumers(self, service_name: str) -> list[TypedConfigKey]:
        """Get all keys consumed by a service."""
        return [k for k in self._keys.values() if service_name in k.consumed_by]


# Global registry instance
_registry = ConfigKeyRegistry()


def register_key(
    key: str,
    service_name: str,
    model: type[T],
    *,
    description: str = "",
    produced_by: set[str] | None = None,
    consumed_by: set[str] | None = None,
) -> TypedConfigKey[T]:
    """Register a configuration key with the global registry."""
    typed_key = TypedConfigKey(
        key=key,
        service_name=service_name,
        model=model,
        description=description,
        produced_by=produced_by or set(),
        consumed_by=consumed_by or set(),
    )
    return _registry.register(typed_key)


def get_registry() -> ConfigKeyRegistry:
    """Get the global configuration key registry."""
    return _registry
