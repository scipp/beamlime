# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable
from typing import Any

import param
import pydantic

from ..config import models
from .config_service import ConfigService


class ConfigBackedParam(param.Parameterized):
    """Base class for parameter models with config service integration."""

    def from_pydantic(self) -> Callable[..., None]:
        """Wrapper to make linters/mypy happy with the callback signature."""

        def update_callback(model: pydantic.BaseModel) -> None:
            self.param.update(**model.model_dump())

        return update_callback

    @property
    def service_name(self) -> str:
        """The service name for this parameter model."""
        raise NotImplementedError("Subclasses must implement service_name")

    @property
    def config_key_name(self) -> str:
        """The specific key name within the service."""
        raise NotImplementedError("Subclasses must implement config_key_name")

    @property
    def schema(self) -> type[models.BaseModel]:
        """The pydantic schema for this parameter model."""
        raise NotImplementedError("Subclasses must implement schema")

    def panel(self) -> Any:
        """Return widget for displaying this parameter in the dashboard."""
        raise NotImplementedError("Subclasses must implement panel")

    @property
    def config_key(self) -> models.ConfigKey:
        """Generate the config key for this parameter model."""
        return models.ConfigKey(
            service_name=self.service_name, key=self.config_key_name
        )

    def subscribe(self, config_service: ConfigService) -> None:
        """Subscribe to config service updates and register callbacks."""
        key = self.config_key
        config_service.register_schema(key, self.schema)
        config_service.subscribe(key=key, callback=self.from_pydantic())

        def set_as_pydantic(**kwargs) -> None:
            model = self.schema.model_validate(kwargs)
            config_service.update_config(key, model)

        # Get all param names that aren't methods/properties
        param_names = [
            name
            for name in self.param.values()
            if (not name.startswith('_') or name == 'name')
        ]
        param_kwargs = {name: getattr(self.param, name) for name in param_names}

        param.bind(set_as_pydantic, **param_kwargs, watch=True)


class MonitorDataParam(ConfigBackedParam):
    """Base class for monitor data parameters."""

    @property
    def service_name(self) -> str:
        return 'monitor_data'


class DetectorDataParam(ConfigBackedParam):
    """Base class for detector data parameters."""

    @property
    def service_name(self) -> str:
        return 'detector_data'


class DataReductionParam(ConfigBackedParam):
    """Base class for data reduction parameters."""

    @property
    def service_name(self) -> str:
        return 'data_reduction'
