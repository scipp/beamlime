# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Plotter definition and registration."""

import enum

import pydantic


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


def register_plotter(name: str, title: str, description: str):
    # Get parameters model from function signature
    params = None
    spec = PlotterSpec(
        name=name,
        title=title,
        description=description,
        params=params,  # type: ignore[assignment]
    )
    # TODO


# TODO subclass
# Define plotter protocols for single-item plots and multi-item plots.
def make_plot(params: PlotParams2d) -> Plotter:
    from . import plots

    return plots.AutoscalingPlot(**params.model_dump()).plot_sum_of_2d
