# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""This file contains utilities for creating plots in the dashboard."""

from collections import defaultdict
from math import prod
from typing import Any

import holoviews as hv
import numpy as np
import scipp as sc
from holoviews import opts

from beamlime.config.workflow_spec import ResultKey

from .scipp_to_holoviews import to_holoviews


def remove_bokeh_logo(plot, element):
    """Remove Bokeh logo from plots."""
    plot.state.toolbar.logo = None


class AutoscalingPlot:
    """
    A plot that automatically adjusts its bounds based on the data.

    Maybe I missed something in the Holoviews docs, but looking, e.g., at
    https://holoviews.org/FAQ.html we need framewise=True to autoscale for streaming
    data. However, this leads to losing the current pan/zoom state when new data
    arrives, making it unusable for interactive exploration.
    Instead, we use this class to track the bounds of the data and update the plot with
    framewise=True only when the bounds *increase*, i.e., new data extends the
    existing bounds. This way, we keep the current pan/zoom state most of the time while
    still allowing the plot to grow as new data comes in. This is especially important
    since there seems to be no way of initializing holoviews.streams.Pipe without
    initial dummy data (such as `None`), i.e., we need to return an empty plot with no
    good starting guess of bounds.
    """

    def __init__(
        self,
        value_margin_factor: float = 0.01,
        image_opts: dict[str, Any] | None = None,
    ):
        """
        Initialize the plot with empty bounds.

        Parameters
        ----------
        value_margin_factor:
            Factor by which to extend the value bounds when updating, by default 0.01.
            This prevents the plot from jumping around when new data arrives that only
            slightly extends the bounds. The value bounds are updated to be 99% of the
            new minimum and 101% of the new maximum when set to 0.01, for example.
        """
        self._value_margin_factor = value_margin_factor
        self.coord_bounds: dict[str, tuple[float | None, float | None]] = defaultdict(
            lambda: (None, None)
        )
        self.value_bounds = (None, None)
        self._image_opts = image_opts or {}

    def update_bounds(self, data: sc.DataArray) -> bool:
        """Update bounds based on the data, return True if bounds changed."""
        coords = [data.coords[dim] for dim in data.dims]
        changed = False
        for coord in coords:
            changed |= self._update_coord_bounds(coord)
        changed |= self._update_value_bounds(data.data)
        return changed

    def _update_coord_bounds(self, coord: sc.Variable) -> bool:
        """Update bounds for a single coordinate."""
        name = coord.dim
        low = coord[0].value
        high = coord[-1].value
        changed = False

        if self.coord_bounds[name][0] is None or low < self.coord_bounds[name][0]:
            self.coord_bounds[name] = (low, self.coord_bounds[name][1])
            changed = True
        if self.coord_bounds[name][1] is None or high > self.coord_bounds[name][1]:
            self.coord_bounds[name] = (self.coord_bounds[name][0], high)
            changed = True

        return changed

    def _update_value_bounds(self, data: sc.Variable) -> bool:
        """Update value bounds based on the data, return True if bounds changed."""
        low = data.nanmin().value
        high = data.nanmax().value
        changed = False

        if self.value_bounds[0] is None or low < self.value_bounds[0]:
            self.value_bounds = (
                low * (1 - self._value_margin_factor),
                self.value_bounds[1],
            )
            changed = True
        if self.value_bounds[1] is None or high > self.value_bounds[1]:
            self.value_bounds = (
                self.value_bounds[0],
                high * (1 + self._value_margin_factor),
            )
            changed = True

        return changed

    def plot_lines(self, data: dict[ResultKey, sc.DataArray]) -> hv.Overlay:
        """Create a line plot from a dictionary of scipp DataArrays."""
        options = opts.Curve(
            responsive=True,
            height=400,
            framewise=False,
            ylim=(0, None),
            hooks=[remove_bokeh_logo],
        )
        if data is None:
            return hv.Overlay([hv.Curve([])]).opts(options)
        curves = []
        bounds_changed = False
        for data_key, da in data.items():
            name = data_key.job_id.source_name
            da = da.assign_coords(dspacing=sc.midpoints(da.coords['dspacing']))
            bounds_changed |= self.update_bounds(da)
            curves.append(to_holoviews(da).relabel(name))
        return (
            hv.Overlay(curves).opts(options).opts(opts.Curve(framewise=bounds_changed))
        )

    def plot_2d(self, data: sc.DataArray | None) -> hv.Image:
        """Create a 2D plot from a scipp DataArray."""
        base_opts = {
            'responsive': True,
            'height': 400,
            'framewise': False,
            'logz': True,
            'colorbar': True,
            'cmap': 'viridis',
            'hooks': [remove_bokeh_logo],
        }
        base_opts.update(self._image_opts)
        options = opts.Image(**base_opts)
        if data is None:
            # Explicit clim required for initial empty plot with logz=True. Changing to
            # logz=True only when we have data is not supported by Holoviews.
            return hv.Image([]).opts(options).opts(clim=(0.1, None))
        # With logz=True we need to exclude zero values for two reasons:
        # 1. The value bounds calculation should properly adjust the color limits. Since
        #    zeros can never be included we want to adjust to the lowest positive value.
        # 2. Holoviews does not seem to all empty `clim` when `logz=True` for empty
        #    data, which we are forced to return above since Holoviews does not appear
        #    to support empty holoviews.streams.Pipe, i.e., we some "empty" image needs
        #    to be returned. Since at that time we cannot guess the true limits this
        #    will always be too low or too high. Once set, it seems it cannot be unset,
        #    i.e., we cannot rely on the autoscale enabled by `framewise=True` but have
        #    to set the limits manually. This is ok since they are computed anyway.
        data = data.to(dtype='float64')
        masked = data.assign(
            sc.where(
                data.data <= sc.scalar(0.0, unit=data.unit),
                sc.scalar(np.nan, unit=data.unit, dtype=data.dtype),
                data.data,
            )
        )
        bounds_changed = self.update_bounds(masked)
        # We are using the masked data here since Holoviews (at least with the Bokeh
        # backend) show values below the color limits with the same color as the lowest
        # value in the colormap, which is not what we want for, e.g., zeros on a log
        # scale plot. The nan values will be shown as transparent.
        histogram = to_holoviews(masked)
        return histogram.opts(options).opts(
            framewise=bounds_changed,
            clim=(self.value_bounds[0], self.value_bounds[1]),
        )

    def plot_sum_of_2d(self, data: dict[ResultKey, sc.DataArray]) -> hv.Image:
        """Create a 2D plot from a dictionary of scipp DataArrays."""
        if data is None:
            return self.plot_2d(data)
        reducer = sc.reduce(list(data.values()))
        # This is not a great check, probably the whole approach is questionable, but
        # this probably does the job for Dream focussed vs. vanadium normalized data.
        if next(iter(data.values())).unit == '':
            combined = reducer.nanmean()
        else:
            combined = reducer.nansum()
        return self.plot_2d(combined)


# TODO Monitor plots below are currently unused and will be replaced
RawData = Any


def monitor_total_counts_bar_chart(**monitors: RawData | None) -> hv.Bars:
    """Create bar chart showing total counts from all monitors."""
    totals = [
        (name, np.nan if monitor is None else np.sum(monitor.current.values))
        for name, monitor in reversed(monitors.items())
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
    dim = da.dim
    if normalize:
        coord = da.coords[dim].to(unit='s')
        bin_width = coord[1:] - coord[:-1]
        total_counts = sc.sum(da.data)
        da = da / total_counts
        da = da / bin_width  # Convert to distribution
    da = da.assign_coords({dim: sc.midpoints(da.coords[dim])})
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
