# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""This file contains utilities for creating plots in the dashboard."""

from math import prod

import holoviews as hv
import numpy as np
import scipp as sc
from holoviews import opts

from .scipp_to_holoviews import to_holoviews
from .subscribers import RawData


def remove_bokeh_logo(plot, element):
    """Remove Bokeh logo from plots."""
    plot.state.toolbar.logo = None


def monitor_total_counts_bar_chart(**monitors: RawData | None) -> hv.Bars:
    """Create bar chart showing total counts from all monitors."""
    totals = [
        (name, np.nan if monitor is None else np.sum(monitor.current.values))
        for name, monitor in monitors.items()
    ]
    bars = hv.Bars(totals, kdims='Monitor', vdims='Total Counts')

    return bars.opts(  # pyright: ignore[reportReturnType]
        opts.Bars(
            title="",
            height=50 + 30 * len(totals),
            color='lightblue',
            ylabel="Total Counts",
            xlabel="",
            invert_axes=True,
            show_legend=False,
            toolbar=None,
            responsive=True,
            xformatter='%.1e',
            xrotation=25,
        )
    )


monitor_colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray']


def plot_monitor(
    data: RawData | None,
    *,
    title: str,
    color: str = 'blue',
    view_mode: str = 'Current',
    normalize: bool = False,
) -> hv.Curve:
    """Create a plot for a single monitor."""
    options = opts.Curve(
        title=title,
        color=color,
        responsive=True,
        height=400,
        ylim=(0, None),
        framewise=True,
        hooks=[remove_bokeh_logo],
    )
    if data is None:
        return hv.Curve([]).opts(options)

    da = data.cumulative if view_mode == 'Cumulative' else data.current
    if normalize:
        coord = da.coords['time_of_arrival'].to(unit='s')
        bin_width = coord[1:] - coord[:-1]
        total_counts = sc.sum(da.data)
        da = da / total_counts
        da = da / bin_width  # Convert to distribution
    da = da.assign_coords(time_of_arrival=sc.midpoints(da.coords['time_of_arrival']))
    return to_holoviews(da).opts(options)


def plot_monitor1(data, view_mode: str = 'Current') -> hv.Curve:
    """Create monitor 1 plot."""
    return plot_monitor(data, title="Monitor 1", color='blue', view_mode=view_mode)


def plot_monitor2(data, view_mode: str = 'Current') -> hv.Curve:
    """Create monitor 2 plot."""
    return plot_monitor(data, title="Monitor 2", color='red', view_mode=view_mode)


def plot_monitors_combined(
    *, view_mode: str = 'Current', **monitors: RawData | None
) -> hv.Overlay:
    """Combined plot of monitor1 and monitor2."""
    plots = [
        plot_monitor(
            monitor, title=name, color=color, view_mode=view_mode, normalize=True
        ).relabel(name)
        for (name, monitor), color in zip(
            monitors.items(), monitor_colors, strict=False
        )
    ]
    plot = prod(plots[1:], start=plots[0])
    return plot.opts(title="Monitors (normalized)")
