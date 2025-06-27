# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Schema registry optimized for Pydantic model inheritance."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, ValidationError

from .models import ConfigKey

T = TypeVar('T', bound=BaseModel)


class CompatibilityMode(Enum):
    """Schema compatibility modes."""

    NONE = "none"
    BACKWARD = "backward"  # New schema can read old data
    FORWARD = "forward"  # Old schema can read new data
    FULL = "full"  # Both backward and forward compatible


@dataclass(frozen=True)
class SchemaVersion:
    """A versioned Pydantic schema definition."""

    version: int
    model_class: type[BaseModel]
    compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD
    deprecated: bool = False

    def validate_data(self, data: dict) -> BaseModel:
        """Validate data against this schema version."""
        return self.model_class.model_validate(data)

    def is_compatible_with(self, other: SchemaVersion) -> bool:
        """Check if this schema is compatible with another."""
        if self.compatibility_mode == CompatibilityMode.NONE:
            return False

        # With inheritance, newer versions should be backward compatible
        # by design if they only add optional fields
        if self.version > other.version:
            return issubclass(self.model_class, other.model_class)
        elif self.version < other.version:
            return issubclass(other.model_class, self.model_class)

        return True


SchemaId = tuple[str, str]


@dataclass(frozen=True)
class TypedConfigKey(Generic[T]):
    """A configuration key with versioned Pydantic schema support."""

    key: str
    service_name: str
    current_model: type[T]
    description: str = ""
    produced_by: set[str] = field(default_factory=set)
    consumed_by: set[str] = field(default_factory=set)

    def create_key(self, source_name: str | None = None) -> ConfigKey:
        """Create a ConfigKey instance for a specific source."""
        return ConfigKey(
            source_name=source_name, service_name=self.service_name, key=self.key
        )

    @property
    def schema_id(self) -> SchemaId:
        """Unique identifier for this schema."""
        return (self.service_name, self.key)


class SchemaRegistry:
    """Registry for versioned Pydantic configuration schemas."""

    def __init__(self) -> None:
        self._schemas: dict[SchemaId, dict[int, SchemaVersion]] = defaultdict(dict)
        self._keys: dict[SchemaId, TypedConfigKey] = {}

    def register_schema_version(
        self,
        key: TypedConfigKey,
        version: int,
        model_class: type[BaseModel],
        *,
        compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD,
        deprecated: bool = False,
    ) -> None:
        """Register a new version of a Pydantic schema."""
        schema_id = key.schema_id

        schema_version = SchemaVersion(
            version=version,
            model_class=model_class,
            compatibility_mode=compatibility_mode,
            deprecated=deprecated,
        )

        # Validate inheritance chain
        existing_versions = self._schemas[schema_id]
        if existing_versions:
            self._validate_inheritance(schema_id, schema_version, existing_versions)

        self._schemas[schema_id][version] = schema_version

        # Update key with latest version
        if not existing_versions or version > max(existing_versions.keys()):
            updated_key = TypedConfigKey(
                key=key.key,
                service_name=key.service_name,
                current_model=model_class,
                description=key.description,
                produced_by=key.produced_by,
                consumed_by=key.consumed_by,
            )
            self._keys[schema_id] = updated_key

    def _validate_inheritance(
        self,
        schema_id: SchemaId,
        new_version: SchemaVersion,
        existing_versions: dict[int, SchemaVersion],
    ) -> None:
        """Validate that new schema follows inheritance pattern."""
        latest_version = max(existing_versions.keys())

        if new_version.version <= latest_version:
            raise ValueError(
                f"Schema version {new_version.version} for {schema_id} "
                f"must be greater than existing version {latest_version}"
            )

        # Check inheritance relationship
        latest_schema = existing_versions[latest_version]
        if not issubclass(new_version.model_class, latest_schema.model_class):
            raise ValueError(
                f"Schema version {new_version.version} for {schema_id} "
                f"must inherit from version {latest_version}"
            )

    def get_schema(
        self, schema_id: str, version: int | None = None
    ) -> SchemaVersion | None:
        """Get a specific schema version."""
        if schema_id not in self._schemas:
            return None

        versions = self._schemas[schema_id]
        if version is None:
            version = max(versions.keys())

        return versions.get(version)

    def get_key(self, service_name: str, key: str) -> TypedConfigKey | None:
        """Get a registered key."""
        schema_id = f"{service_name}/{key}"
        return self._keys.get(schema_id)

    def validate_and_upgrade(
        self, schema_id: str, data: dict, target_version: int | None = None
    ) -> BaseModel:
        """Validate data and upgrade to target version."""
        if schema_id not in self._schemas:
            raise ValueError(f"Unknown schema: {schema_id}")

        versions = self._schemas[schema_id]
        if target_version is None:
            target_version = max(versions.keys())

        target_schema = versions[target_version]

        # Try to validate with target version first (most common case)
        try:
            return target_schema.validate_data(data)
        except ValidationError:
            pass

        # Try older versions and upgrade
        for version in sorted(versions.keys(), reverse=True):
            if version >= target_version:
                continue

            schema = versions[version]
            try:
                validated_data = schema.validate_data(data)
                # Convert to dict and re-validate with target schema
                # Pydantic inheritance handles the upgrade automatically
                return target_schema.validate_data(validated_data.model_dump())
            except ValidationError:
                continue

        # If all else fails, try direct validation one more time
        # This will raise the appropriate ValidationError
        return target_schema.validate_data(data)


# Global registry instance
_registry = SchemaRegistry()


def register_schema_version(
    key: TypedConfigKey,
    version: int,
    model_class: type[BaseModel],
    *,
    compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD,
    deprecated: bool = False,
) -> None:
    """Register a schema version with the global registry."""
    _registry.register_schema_version(
        key=key,
        version=version,
        model_class=model_class,
        compatibility_mode=compatibility_mode,
        deprecated=deprecated,
    )


def get_schema_registry() -> SchemaRegistry:
    """Get the global schema registry."""
    return _registry


# Usage example
class WorkflowConfigV1(BaseModel):
    """Version 1 of workflow configuration."""

    identifier: str | None = None
    values: dict[str, str] = Field(default_factory=dict)


class WorkflowConfigV2(WorkflowConfigV1):
    """Version 2 adds validation and timeout settings."""

    validation_enabled: bool = Field(default=True)
    timeout_seconds: int = Field(default=300)


class WorkflowConfigV3(WorkflowConfigV2):
    """Version 3 adds retry configuration."""

    max_retries: int = Field(default=3)
    retry_delay_seconds: int = Field(default=5)


# Define the key
WORKFLOW_CONFIG = TypedConfigKey(
    key="workflow_config",
    service_name="data_reduction",
    current_model=WorkflowConfigV3,
    description="Configuration for a workflow",
    produced_by={"dashboard"},
    consumed_by={"data_reduction"},
)

# Register all versions
register_schema_version(WORKFLOW_CONFIG, version=1, model_class=WorkflowConfigV1)
register_schema_version(WORKFLOW_CONFIG, version=2, model_class=WorkflowConfigV2)
register_schema_version(WORKFLOW_CONFIG, version=3, model_class=WorkflowConfigV3)
