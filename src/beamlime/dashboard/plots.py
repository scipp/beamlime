# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""This file contains utilities for creating plots in the dashboard."""

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
    bars = hv.Bars(reversed(totals), kdims='Monitor', vdims='Total Counts')

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


def plot_monitor(
    data, monitor_name: str, color: str = 'blue', view_mode: str = 'Current'
) -> hv.Curve:
    """Create a plot for a single monitor."""
    if not data:
        return hv.Curve([])

    data = data.cumulative if view_mode == 'Cumulative' else data.current
    data = data.assign_coords(
        time_of_arrival=sc.midpoints(data.coords['time_of_arrival'])
    )
    curve = to_holoviews(data)

    return curve.opts(
        title=monitor_name,
        xlabel="TOA",
        ylabel="Counts",
        color=color,
        line_width=2,
        responsive=True,
        height=400,
        hooks=[remove_bokeh_logo],
    )


def plot_monitor1(data, view_mode: str = 'Current') -> hv.Curve:
    """Create monitor 1 plot."""
    return plot_monitor(data, "Monitor 1", color='blue', view_mode=view_mode)


def plot_monitor2(data, view_mode: str = 'Current') -> hv.Curve:
    """Create monitor 2 plot."""
    return plot_monitor(data, "Monitor 2", color='red', view_mode=view_mode)


def plot_monitors_combined(
    monitor1, monitor2, logscale: bool = False, view_mode: str = 'Current'
) -> hv.Overlay:
    """Combined plot of monitor1 and monitor2."""
    mon1 = plot_monitor1(monitor1, view_mode=view_mode)
    mon2 = plot_monitor2(monitor2, view_mode=view_mode)
    mons = mon1 * mon2
    return mons.opts(title="Monitors", logy=logscale)
