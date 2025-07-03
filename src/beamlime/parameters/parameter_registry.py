# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, Field

T = TypeVar('T', bound=BaseModel)


@dataclass(frozen=True, slots=True)
class ModelId:
    """Identifier for a model (name and version)."""

    name: str
    version: int = 1


class ParameterRegistry(UserDict[str, dict[int, type[BaseModel]]]):
    def register(self, name: str, *, version: int) -> Callable[[type[T]], type[T]]:
        """
        Decorator to register a parameter model with a name and version.
        """

        def decorator(cls: type[T]) -> type[T]:
            if name not in self.data:
                self.data[name] = {}
            if version in self.data[name]:
                raise ValueError(
                    f"Parameter model '{name}' version '{version}' already registered."
                )
            self.data[name][version] = cls
            return cls

        return decorator

    def get_model(self, model_spec: ModelId) -> type[BaseModel]:
        """Get the model class for a given model specification."""
        if (models := self.get(model_spec.name)) is None:
            raise ValueError(f"Parameter '{model_spec.name}' not registered.")
        if (model := models.get(model_spec.version)) is None:
            raise ValueError(
                f"Version {model_spec.version} of parameter "
                f"'{model_spec.name}' not found. Available versions: "
                f"{list(models.keys())}."
            )
        return model

    def prepare_for_serialization(
        self, model_sec: ModelId, value: BaseModel
    ) -> ParamValue:
        model = self.get_model(model_sec)
        if not isinstance(value, model):
            raise ValueError(
                f"Value must be an instance of {model.__name__}, "
                f"but got {type(value).__name__}."
            )
        return ParamValue(model=model_sec, value=value.model_dump())


class ParamSpec(BaseModel):
    """
    Specification for a parameter.

    Used by the backend to communicate available parameters to the frontend.
    """

    name: str
    description: str
    model: ModelId = Field(description="Model identifier.")


class ParamValue(BaseModel):
    """
    Value of a parameter, including its model identifier and serialized value.

    Used by the frontend to send parameter values to the backend.
    """

    model: ModelId = Field(description="Model identifier.")
    value: dict[str, Any]

    @staticmethod
    def from_model(model: ModelId, value: BaseModel) -> ParamValue:
        return ParamValue(model=model, value=value.model_dump())

    def validate_value(self, registry: ParameterRegistry) -> BaseModel:
        model_cls = registry.get_model(self.model)
        return model_cls.model_validate(self.value)


_registry = ParameterRegistry()


def get_parameter_registry() -> ParameterRegistry:
    return _registry
