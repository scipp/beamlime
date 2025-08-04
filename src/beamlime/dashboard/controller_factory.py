# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, Generic, TypeVar

import pydantic
import scipp as sc
from pydantic_core import PydanticUndefined

from beamlime.config.schema_registry import SchemaRegistryBase

from .config_service import ConfigService

K = TypeVar('K')


class Controller(Generic[K]):
    """Controller for linking widgets to configuration values."""

    def __init__(
        self,
        *,
        config_key: K,
        config_service: ConfigService[K, Any, pydantic.BaseModel],
        schema: type[pydantic.BaseModel],
    ) -> None:
        self._config_key = config_key
        self._config_service = config_service
        self._schema = schema
        self._updating = False
        self._callback: Callable[[dict[str, Any]], None] | None = None

    def get_defaults(self) -> dict[str, Any]:
        """Get the default values for the configuration fields."""
        defaults = {}
        for field, field_info in self._schema.model_fields.items():
            if field_info.default is not PydanticUndefined:
                defaults[field] = field_info.default
            elif field_info.default_factory is not None:
                defaults[field] = field_info.default_factory()
            else:
                defaults[field] = None
        return defaults

    def get_descriptions(self) -> dict[str, str | None]:
        """Get descriptions for the configuration fields."""
        return {
            field: field_info.description
            for field, field_info in self._schema.model_fields.items()
        }

    @contextmanager
    def _disable_updates(self) -> Generator[None, None, None]:
        """Context manager to disable set_value calls during config updates."""
        old_updating = self._updating
        self._updating = True
        try:
            yield
        finally:
            self._updating = old_updating

    def _preprocess_value(self, value: dict[str, Any]) -> dict[str, Any]:
        """
        Preprocess the value before setting it.

        This method can be overridden to apply any necessary transformations
        to the value before it is set in the configuration service.
        """
        return value

    def _preprocess_config(self, value: dict[str, Any]) -> dict[str, Any]:
        """
        Preprocess the value from the configuration service before using it.

        This method can be overridden to apply any necessary transformations
        to the value retrieved from the configuration service.
        """
        return value

    def set_value(self, **value: Any) -> None:
        """Set the configuration value from a dictionary."""

        if self._updating:
            return
        preprocessed = self._preprocess_value(value)
        model_instance = self._schema.model_validate(preprocessed)
        if preprocessed != value:
            self._trigger_callback(model_instance)
        self._config_service.update_config(self._config_key, model_instance)

    def _trigger_callback(self, model: pydantic.BaseModel) -> None:
        """Trigger the callback with the model's data."""
        if self._callback:
            with self._disable_updates():
                value = self._preprocess_config(model.model_dump())
                self._callback(value)

    def subscribe(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to configuration value changes."""
        self._callback = callback
        self._config_service.subscribe(self._config_key, self._trigger_callback)


class BinEdgeController(Controller[K]):
    def __init__(
        self,
        *,
        config_key: K,
        config_service: ConfigService[K, Any, pydantic.BaseModel],
        schema: type[pydantic.BaseModel],
    ) -> None:
        super().__init__(
            config_key=config_key, config_service=config_service, schema=schema
        )
        self._old_unit: str | None = None

    def _preprocess_config(self, value: dict[str, Any]) -> dict[str, Any]:
        self._old_unit = value['unit']
        return value

    def _preprocess_value(self, value: dict[str, Any]) -> dict[str, Any]:
        unit = value['unit']
        if self._old_unit is None:
            self._old_unit = unit
        elif unit != self._old_unit:
            preprocessed = value.copy()
            preprocessed['low'] = self._to_unit(value['low'], self._old_unit, unit)
            preprocessed['high'] = self._to_unit(value['high'], self._old_unit, unit)
            return preprocessed
        return value

    def _to_unit(self, value: float, old_unit: str, new_unit: str) -> float:
        # We round the result to avoid floating-point precision issues when switching
        # units. Otherwise, the value might not match the original value when converted
        # back to the old unit.
        return round(
            sc.scalar(value, unit=old_unit).to(unit=new_unit).value, ndigits=12
        )


class RangeController(Controller[K]):
    """Controller for range settings that converts between center/width and low/high."""

    def __init__(
        self,
        *,
        config_key: K,
        config_service: ConfigService[K, Any, pydantic.BaseModel],
        schema: type[pydantic.BaseModel],
    ) -> None:
        super().__init__(
            config_key=config_key, config_service=config_service, schema=schema
        )
        self._old_unit: str | None = None

    def _preprocess_config(self, value: dict[str, Any]) -> dict[str, Any]:
        """Convert from low/high to center/width representation."""
        self._old_unit = value.get('unit')
        if 'low' in value and 'high' in value:
            low = value['low']
            high = value['high']
            center = (low + high) / 2
            width = high - low

            # Create a copy and add center/width while keeping other fields
            preprocessed = value.copy()
            preprocessed['center'] = center
            preprocessed['width'] = width
            return preprocessed
        return value

    def _preprocess_value(self, value: dict[str, Any]) -> dict[str, Any]:
        """Convert from center/width to low/high representation."""
        unit = value.get('unit')
        if self._old_unit is None:
            self._old_unit = unit
        elif unit and unit != self._old_unit:
            # Convert center and width to new unit
            if 'center' in value and 'width' in value:
                preprocessed = value.copy()
                preprocessed['center'] = self._to_unit(
                    value['center'], self._old_unit, unit
                )
                preprocessed['width'] = self._to_unit(
                    value['width'], self._old_unit, unit
                )
                value = preprocessed

        if 'center' in value and 'width' in value:
            center = value['center']
            width = value['width']
            low = center - width / 2
            high = center + width / 2

            # Create a copy and add low/high while removing center/width
            preprocessed = value.copy()
            preprocessed['low'] = low
            preprocessed['high'] = high
            # Remove center/width as they're not part of the schema
            preprocessed.pop('center', None)
            preprocessed.pop('width', None)
            return preprocessed
        return value

    def _to_unit(self, value: float, old_unit: str, new_unit: str) -> float:
        # We round the result to avoid floating-point precision issues when switching
        # units. Otherwise, the value might not match the original value when converted
        # back to the old unit.
        return round(
            sc.scalar(value, unit=old_unit).to(unit=new_unit).value, ndigits=12
        )


class ControllerFactory(Generic[K]):
    """
    Factory for creating controllers linked to a config value.

    Controllers created by this factory will subscribe to changes of values of a
    specific configuration value. The controller can also update the value. Controllers
    isolate widgets from the underlying (pydantic) configuration values.
    """

    def __init__(
        self,
        *,
        config_service: ConfigService[K, Any, pydantic.BaseModel],
        schema_registry: SchemaRegistryBase[K, pydantic.BaseModel],
    ) -> None:
        """
        Initialize the controller factory with a configuration service.

        Parameters
        ----------
        config_service:
            The configuration service controllers will subscribe to.
        schema_registry:
            Registry of schemas corresponding to possible controllers.
        """
        self._config_service = config_service
        self._schema_registry = schema_registry

    def create(
        self, *, config_key: K, controller_cls: type[Controller[K]] | None = None
    ) -> Controller[K]:
        """
        Create a controller for the given configuration key.

        Parameters
        ----------
        config_key:
            The configuration key to create a controller for.
        controller_cls:
            Optional custom controller class to use instead of the default `Controller`.

        Returns
        -------
        A controller instance linked to the configuration value.

        Raises
        ------
        KeyError:
            If no schema is registered for the given key.
        """
        schema = self._schema_registry.get_model(config_key)
        if schema is None:
            raise KeyError(f"No schema registered for {config_key}.")
        return (controller_cls or Controller)(
            config_key=config_key, config_service=self._config_service, schema=schema
        )
