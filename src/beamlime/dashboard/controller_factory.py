# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any, Callable

import pydantic

from .config_service import ConfigService

SchemaRegistry = dict[str, pydantic.BaseModel]


class Controller:
    def set_value(self, value: dict[str, Any]) -> None: ...
    def subscribe(self, callback: Callable[[dict[str, Any]], None]) -> None: ...


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
