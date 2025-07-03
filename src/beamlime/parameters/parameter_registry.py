# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
from collections import UserDict
from collections.abc import Callable

from pydantic import BaseModel, Field


class ParamSpec(BaseModel):
    """Specification for a parameter."""

    name: str
    description: str
    model_name: str = Field(description="Name of the model in the registry.")
    model_version: int = Field(default=1, description="Version of the model.")


class ParameterRegistry(UserDict[str, dict[int, type[BaseModel]]]):
    def register(
        self, name: str, *, version: int
    ) -> Callable[[type[BaseModel]], type[BaseModel]]:
        """
        Decorator to register a parameter model with a name and version.
        """

        def decorator(cls: type[BaseModel]) -> type[BaseModel]:
            if name not in self.data:
                self.data[name] = {}
            if version in self.data[name]:
                raise ValueError(
                    f"Parameter model '{name}' version '{version}' already registered."
                )
            self.data[name][version] = cls
            return cls

        return decorator

    def get_model(self, spec: ParamSpec) -> type[BaseModel]:
        """Get the model class for a given parameter specification."""
        if (models := self.get(spec.model_name)) is None:
            raise ValueError(f"Parameter '{spec.model_name}' not registered.")
        if (model := models.get(spec.model_version)) is None:
            raise ValueError(
                f"Version {spec.model_version} of parameter "
                f"'{spec.model_name}' not found. Available versions: "
                f"{list(models.keys())}."
            )
        return model
