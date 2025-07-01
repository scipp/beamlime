# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pydantic
import scipp as sc

from .config_service import ConfigService


@dataclass(frozen=True)
class ConfigKey:
    key: str
    source_name: str | None = None


SchemaRegistry = dict[ConfigKey, type[pydantic.BaseModel]]


class Controller:
    """Controller for linking widgets to configuration values."""

    def __init__(
        self,
        config_key: ConfigKey,
        config_service: ConfigService,
        schema: type[pydantic.BaseModel],
    ) -> None:
        self._config_key = config_key
        self._config_service = config_service
        self._schema = schema
        self._updating = False
        self._callback: Callable[[dict[str, Any]], None] | None = None

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

    def _preprocesses_config(self, value: dict[str, Any]) -> dict[str, Any]:
        """
        Preprocess the value from the configuration service before using it.

        This method can be overridden to apply any necessary transformations
        to the value retrieved from the configuration service.
        """
        return value

    def set_value(self, value: dict[str, Any]) -> None:
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
                value = self._preprocesses_config(model.model_dump())
                self._callback(value)

    def subscribe(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to configuration value changes."""
        self._callback = callback
        self._config_service.subscribe(self._config_key, self._trigger_callback)


class BinEdgeController(Controller):
    def __init__(
        self,
        config_key: ConfigKey,
        config_service: ConfigService,
        schema: type[pydantic.BaseModel],
    ) -> None:
        super().__init__(config_key, config_service, schema)
        self._old_unit: str = ''

    def _preprocesses_config(self, value: dict[str, Any]) -> dict[str, Any]:
        self._old_unit = value['unit']
        return value

    def _preprocess_value(self, value: dict[str, Any]) -> dict[str, Any]:
        unit = value['unit']
        if unit != self._old_unit:
            preprocessed = value.copy()
            preprocessed['low'] = self._to_unit(value['low'], self._old_unit, unit)
            preprocessed['high'] = self._to_unit(value['high'], self._old_unit, unit)
            return preprocessed
        return value

    def _to_unit(self, value: float, old_unit: str, new_unit: str) -> float:
        return sc.scalar(value, unit=old_unit).to(unit=new_unit).value


class ControllerFactory:
    """
    Factory for creating controllers linked to a config value.

    Controllers created by this factory will subscribe to changes of values of a
    specific configuration value. The controller can also update the value. Controllers
    isolate widgets from the underlying (pydantic) configuration values.
    """

    def __init__(
        self, *, config_service: ConfigService, schema_registry: SchemaRegistry
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
        self, config_key: ConfigKey, controller: type[Controller] | None = None
    ) -> Controller:
        """
        Create a controller for the given configuration key.

        Parameters
        ----------
        config_key:
            The configuration key to create a controller for.

        Returns
        -------
        A controller instance linked to the configuration value.

        Raises
        ------
        KeyError:
            If no schema is registered for the given key.
        """
        if config_key not in self._schema_registry:
            raise KeyError(f"No schema registered for config key: {config_key}")

        schema = self._schema_registry[config_key]
        if controller is None:
            controller = Controller
        return controller(config_key, self._config_service, schema)
