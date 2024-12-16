# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Benchmark result visualization helpers for scipp data structure.
import scipp as sc
from matplotlib.contour import ContourSet
from matplotlib.figure import Figure
from matplotlib.pyplot import Axes


def plot_contourf(
    da: sc.DataArray,
    *,
    x_coord: str,
    y_coord: str,
    fig: Figure | None = None,
    ax: Axes,
    under_color: str | None = None,
    over_color: str | None = None,
    **contourf_kwargs,
) -> ContourSet:
    """Plot contour plots using coordinates of a ``DataArray``.

    Parameters
    ----------
    da:
        Data array to plot.

    x_coord:
        Name of the coordinate for x axis.

    y_coord:
        Name of the coordinate for y axis.

    fig:
        Matplotlib figure.

    ax:
        Matplotlib axes.

    under_color:
        Color for values under the minimum value.

    over_color:
        Color for values over the maximum value.

    contourf_kwargs:
        Keyword arguments passed to `matplotlib.pyplot.contourf`.

    """
    x = da.coords[x_coord].values
    y = da.coords[y_coord].values
    data = sc.transpose(
        da, dims=[da.coords[y_coord].dim, da.coords[x_coord].dim]
    ).values
    ax.set_xlabel(da.dims[0])
    ax.set_ylabel(da.dims[1])
    ax.set_xlabel(f"{x_coord} [{da.coords[x_coord].unit}]")
    ax.set_ylabel(f"{y_coord} [{da.coords[y_coord].unit}]")

    ctr = ax.contourf(x, y, data, **contourf_kwargs)

    # Optional settings.
    if fig:
        fig.colorbar(ctr, ax=ax)
    if under_color:
        ctr.get_cmap().set_under(under_color)
    if over_color:
        ctr.get_cmap().set_over(over_color)

    return ctr
