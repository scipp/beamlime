# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Plotter definition and registration."""

import enum
import typing
from collections import UserDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

import pydantic
import scipp as sc

from beamlime.config.workflow_spec import ResultKey


class PlotScale(enum.Enum):
    """Enumeration of plot scales."""

    linear = 'linear'
    log = 'log'


class PlotParams2d(pydantic.BaseModel):
    """Common parameters for 2d plots."""

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


# Define plotter protocols for single-item plots and multi-item plots.
class Plotter(Protocol):
    """
    Protocol for a plotter function.
    """

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
            spec = PlotterSpec(
                name=name,
                title=title,
                description=description,
                params=type_hints['params'],
            )
            self[name] = PlotterEntry(spec=spec, factory=factory)
            return factory

        return decorator

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


@plotter_registry.register_plotter(
    name='sum_of_2d',
    title='Sum of 2D',
    description='Plot the sum over all frames as a 2D image.',
)
def make_plot(params: PlotParams2d) -> Plotter:
    from . import plots

    return plots.AutoscalingPlot(**params.model_dump()).plot_sum_of_2d
