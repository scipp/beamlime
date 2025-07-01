# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pydantic

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

    @contextmanager
    def _disable_updates(self) -> Generator[None, None, None]:
        """Context manager to disable set_value calls during config updates."""
        old_updating = self._updating
        self._updating = True
        try:
            yield
        finally:
            self._updating = old_updating

    def set_value(self, value: dict[str, Any]) -> None:
        """Set the configuration value from a dictionary."""
        if self._updating:
            return
        model_instance = self._schema.model_validate(value)
        self._config_service.update_config(self._config_key, model_instance)

    def subscribe(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to configuration value changes."""

        def _wrapped_callback(model: pydantic.BaseModel) -> None:
            with self._disable_updates():
                callback(model.model_dump())

        self._config_service.subscribe(self._config_key, _wrapped_callback)


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

    def create(self, config_key: ConfigKey) -> Controller:
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
        return Controller(config_key, self._config_service, schema)
