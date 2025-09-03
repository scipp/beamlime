# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Plotter definition and registration."""

import enum
import typing
from abc import ABC, abstractmethod
from collections import UserDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

import pydantic
import scipp as sc

from beamlime.config.workflow_spec import ResultKey


class PlotScale(enum.Enum):
    """Enumeration of plot scales."""

    linear = 'linear'
    log = 'log'


class PlotScaleParams(pydantic.BaseModel):
    x_scale: PlotScale = pydantic.Field(
        default=PlotScale.linear, description="Scale for x-axis", title="X Axis Scale"
    )
    y_scale: PlotScale = pydantic.Field(
        default=PlotScale.linear, description="Scale for y-axis", title="Y Axis Scale"
    )
    color_scale: PlotScale = pydantic.Field(
        default=PlotScale.log,
        description="Scale for color axis",
        title="Color Axis Scale",
    )


class PlotParams2d(pydantic.BaseModel):
    """Common parameters for 2d plots."""

    plot_scale: PlotScaleParams = pydantic.Field(
        default_factory=PlotScaleParams,
        description="Scaling options for the plot axes.",
    )


class DataRequirements(pydantic.BaseModel):
    """Specification for data requirements of a plotter."""

    min_dims: int = pydantic.Field(description="Minimum number of dimensions")
    max_dims: int = pydantic.Field(description="Maximum number of dimensions")
    required_coords: list[str] = pydantic.Field(
        default_factory=list, description="Required coordinate names"
    )
    data_types: list[str] = pydantic.Field(
        default_factory=lambda: ["float32", "float64", "int32", "int64"],
        description="Supported data types",
    )
    multiple_datasets: bool = pydantic.Field(
        default=False, description="Whether plotter supports multiple datasets"
    )

    def validate_data(self, data: dict[ResultKey, sc.DataArray]) -> bool:
        """Validate that the data meets these requirements."""
        if not data:
            return False

        if not self.multiple_datasets and len(data) > 1:
            return False

        for dataset in data.values():
            if not self._validate_dataset(dataset):
                return False

        return True

    def _validate_dataset(self, dataset: sc.DataArray) -> bool:
        """Validate a single dataset."""
        # Check dimensions
        if dataset.ndim < self.min_dims or dataset.ndim > self.max_dims:
            return False

        # Check required coordinates
        for coord in self.required_coords:
            if coord not in dataset.coords:
                return False

        # Check data type
        if str(dataset.dtype) not in self.data_types:
            return False

        return True


class PlotterSpec(pydantic.BaseModel):
    """
    Specification for a plotter.

    This model defines the metadata and a parameters specification. This allows for
    dynamic creation of user interfaces for configuring plots.
    """

    name: str = pydantic.Field(description="Name of the plot type. Used internally.")
    title: str = pydantic.Field(
        description="Title of the plot type. For display in the UI."
    )
    description: str = pydantic.Field(description="Description of the plot type.")
    params: type[pydantic.BaseModel] = pydantic.Field(
        description="Pydantic model defining the parameters for the plot."
    )
    data_requirements: DataRequirements = pydantic.Field(
        description="Data requirements for this plotter."
    )


# TODO Define plotter protocols for single-item plots and multi-item plots.
class Plotter(Protocol):
    """Protocol for a plotter function."""

    def __call__(self, data: dict[ResultKey, sc.DataArray]) -> Any: ...


@dataclass
class PlotterEntry:
    """Entry combining a plotter specification with its factory function."""

    spec: PlotterSpec
    factory: Callable[[Any], Plotter]  # Use Any since we store different param types


# Type variable for parameter types
P = TypeVar('P', bound=pydantic.BaseModel)


class PlotterRegistry(UserDict[str, PlotterEntry]):
    def register_plotter(
        self, name: str, title: str, description: str
    ) -> Callable[[Callable[[P], Plotter]], Callable[[P], Plotter]]:
        def decorator(factory: Callable[[P], Plotter]) -> Callable[[P], Plotter]:
            # Try to get the type hint of the 'params' argument if it exists
            # Use get_type_hints to resolve forward references, in case we used
            # `from __future__ import annotations`.
            type_hints = typing.get_type_hints(factory, globalns=factory.__globals__)
            # Get data requirements from the factory function attribute
            if not hasattr(factory, 'data_requirements'):
                raise ValueError(
                    f"Factory function {factory.__name__} must have a 'data_requirements' attribute"
                )

            spec = PlotterSpec(
                name=name,
                title=title,
                description=description,
                params=type_hints['params'],
                data_requirements=factory.data_requirements,
            )
            self[name] = PlotterEntry(spec=spec, factory=factory)
            return factory

        return decorator

    def get_compatible_plotters(
        self, data: dict[ResultKey, sc.DataArray]
    ) -> dict[str, PlotterSpec]:
        """Get plotters compatible with the given data."""
        return {
            name: entry.spec
            for name, entry in self.items()
            if entry.spec.data_requirements.validate_data(data)
        }

    def get_specs(self) -> dict[str, PlotterSpec]:
        """Get all plotter specifications for UI display."""
        return {name: entry.spec for name, entry in self.items()}

    def get_spec(self, name: str) -> PlotterSpec:
        """Get specification for a specific plotter."""
        return self[name].spec

    def create_plotter(self, name: str, params: pydantic.BaseModel) -> Plotter:
        """Create a plotter instance with the given parameters."""
        return self[name].factory(params)


plotter_registry = PlotterRegistry()


def plotter_factory(data_requirements: DataRequirements):
    """Decorator to mark a function as a plotter factory with data requirements."""

    def decorator(func):
        func.data_requirements = data_requirements
        return func

    return decorator


class PlotterFactory(ABC, Generic[P]):
    def __init__(self, data_requirements: DataRequirements):
        self.data_requirements = data_requirements

    @abstractmethod
    def __call__(self, params: P) -> Plotter: ...


@plotter_registry.register_plotter(
    name='sum_of_2d',
    title='Sum of 2D',
    description='Plot the sum over all frames as a 2D image.',
)
@plotter_factory(DataRequirements(min_dims=2, max_dims=2))
def _sum_of_2d(params: PlotParams2d) -> Plotter:
    from . import plots

    # TODO Use params
    return plots.AutoscalingPlot(value_margin_factor=0.1).plot_sum_of_2d


@plotter_registry.register_plotter(
    name='lines',
    title='Lines',
    description='Plot the data as line plots.',
)
@plotter_factory(DataRequirements(min_dims=1, max_dims=1))
def _lines(params: PlotScaleParams) -> Plotter:
    from . import plots

    # TODO Use params
    return plots.AutoscalingPlot(value_margin_factor=0.1).plot_lines
