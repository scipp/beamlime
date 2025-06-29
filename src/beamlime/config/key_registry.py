# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause

"""Registry for configuration keys with type safety and documentation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from .models import ConfigKey

T = TypeVar('T')


@dataclass(frozen=True)
class ConfigKeySpec(Generic[T]):
    """
    A configuration key specification with associated type and metadata.

    Instances automatically register themselves with the global registry
    upon creation, ensuring all specs are discoverable and preventing
    duplicate registrations.
    """

    key: str
    service_name: str
    model: type[T]
    description: str = ""
    produced_by: set[str] = field(default_factory=set)
    consumed_by: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        """Register this spec with the global registry after creation."""
        _registry.register(self)

    def create_key(self, source_name: str | None = None) -> ConfigKey:
        """Create a ConfigKey instance for a specific source."""
        return ConfigKey(
            source_name=source_name, service_name=self.service_name, key=self.key
        )

    @property
    def full_name(self) -> str:
        """Get the full key name in service/key format."""
        return f"{self.service_name}/{self.key}"

    def __str__(self) -> str:
        return f"*/{self.service_name}/{self.key}"


class ConfigKeyRegistry:
    """Central registry for all configuration keys in the application."""

    def __init__(self) -> None:
        self._keys: dict[str, ConfigKeySpec] = {}
        self._model_to_spec: dict[type, ConfigKeySpec] = {}

    def register(self, spec: ConfigKeySpec) -> ConfigKeySpec:
        """
        Register a configuration key specification.

        This is typically called automatically by ConfigKeySpec.__post_init__,
        but can be used directly for testing or special cases.
        """
        key_id = f"{spec.service_name}/{spec.key}"
        if key_id in self._keys:
            existing = self._keys[key_id]
            if existing.model != spec.model:
                raise ValueError(
                    f"Key {key_id} already registered with different type: "
                    f"{existing.model} vs {spec.model}"
                )

        if spec.model in self._model_to_spec:
            existing_spec = self._model_to_spec[spec.model]
            existing_key_id = f"{existing_spec.service_name}/{existing_spec.key}"
            if existing_key_id != key_id:
                raise ValueError(
                    f"Model {spec.model} already registered for key {existing_key_id}, "
                    f"cannot register for {key_id}"
                )

        self._keys[key_id] = spec
        self._model_to_spec[spec.model] = spec
        return spec

    def get_spec(self, service_name: str, key: str) -> ConfigKeySpec | None:
        """Get a registered key specification."""
        return self._keys.get(f"{service_name}/{key}")

    def list_specs(self, service_name: str | None = None) -> list[ConfigKeySpec]:
        """List all registered key specifications, optionally filtered by service."""
        if service_name is None:
            return list(self._keys.values())
        return [
            spec for spec in self._keys.values() if spec.service_name == service_name
        ]

    def get_produced_keys(self, service_name: str) -> list[ConfigKeySpec]:
        """Get all key specifications for keys produced by a service."""
        return [
            spec for spec in self._keys.values() if service_name in spec.produced_by
        ]

    def get_consumed_keys(self, service_name: str) -> list[ConfigKeySpec]:
        """Get all key specifications for keys consumed by a service."""
        return [
            spec for spec in self._keys.values() if service_name in spec.consumed_by
        ]


# Global registry instance
_registry = ConfigKeyRegistry()


def get_registry() -> ConfigKeyRegistry:
    """Get the global configuration key registry."""
    return _registry
