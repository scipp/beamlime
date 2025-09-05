# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""This file contains utilities for creating plots in the dashboard."""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any

import holoviews as hv
import numpy as np
import scipp as sc
from holoviews import opts

from beamlime.config.workflow_spec import ResultKey

from .plot_params import (
    PlotAspect,
    PlotAspectType,
    PlotParams1d,
    PlotParams2d,
    PlotScale,
    PlotScaleParams2d,
)
from .scipp_to_holoviews import to_holoviews


def remove_bokeh_logo(plot, element):
    """Remove Bokeh logo from plots."""
    plot.state.toolbar.logo = None


class Autoscaler:
    """
    A helper class that automatically adjusts bounds based on data.

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

    def __init__(self, value_margin_factor: float = 0.01):
        """
        Initialize the autoscaler with empty bounds.

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


class Plotter(ABC):
    """
    Base class for plots that support autoscaling.
    """

    def __init__(
        self,
        *,
        autoscaler: Autoscaler | None = None,
        plot_aspect: PlotAspect | None = None,
        combine_mode: str = 'layout',  # 'overlay' or 'layout'
        **kwargs,
    ):
        """
        Initialize the plotter.

        Parameters
        ----------
        autoscaler:
            Autoscaler instance to use for bound tracking. If None, creates a new one.
        combine_mode:
            How to combine multiple datasets: 'overlay' or 'layout'.
        **kwargs:
            Additional keyword arguments passed to the autoscaler if created.
        """
        self.autoscaler = autoscaler if autoscaler is not None else Autoscaler(**kwargs)
        self.combine_mode = combine_mode
        plot_aspect = plot_aspect or PlotAspect()

        # Note: The way Holoviews (or Bokeh?) determines the axes and data sizing seems
        # to be broken in weird ways. This happens in particular when we return a Layout
        # of multiple plots. Axis ranges that cover less than one unit in data space are
        # problematic in particular, but I have not been able to nail down the exact
        # conditions. Plots will then either have zero frame width or height, or be very
        # small, etc. It is therefore important to set either width or height when using
        # data_aspect or aspect='equal'.
        # However, even that does not solve all problem, for example we can end up with
        # whitespace between plots in a layout.
        self._sizing_opts: dict[str, Any]
        match plot_aspect.aspect_type:
            case PlotAspectType.free:
                self._sizing_opts = {}
            case PlotAspectType.equal:
                self._sizing_opts = {'aspect': 'equal'}
            case PlotAspectType.square:
                self._sizing_opts = {'aspect': 'square'}
            case PlotAspectType.aspect:
                self._sizing_opts = {'aspect': plot_aspect.ratio}
            case PlotAspectType.data_aspect:
                self._sizing_opts = {'data_aspect': plot_aspect.ratio}
        if plot_aspect.fix_width:
            self._sizing_opts['frame_width'] = plot_aspect.width
        if plot_aspect.fix_height:
            self._sizing_opts['frame_height'] = plot_aspect.height
        self._sizing_opts['responsive'] = True

    def __call__(
        self, data: dict[ResultKey, sc.DataArray]
    ) -> hv.Overlay | hv.Layout | hv.Element:
        """Create one or more plots from the given data."""
        plots: list[hv.Element] = []
        try:
            for data_key, da in data.items():
                plot_element = self.plot(da)
                # Add label from data_key if the plot supports it
                if hasattr(plot_element, 'relabel'):
                    plot_element = plot_element.relabel(data_key.job_id.source_name)
                plots.append(plot_element)
        except Exception as e:
            print(f"Error while plotting data: {e}")
            plots = [
                hv.Text(0.5, 0.5, f"Error: {e}").opts(
                    text_align='center', text_baseline='middle'
                )
            ]

        plots = [self._apply_generic_options(p) for p in plots]

        if len(plots) == 1:
            return plots[0]
        if self.combine_mode == 'overlay':
            return hv.Overlay(plots)
        return hv.Layout(plots)

    def _apply_generic_options(self, plot_element: hv.Element) -> hv.Element:
        """Apply generic options like height, responsive, hooks to a plot element."""
        base_opts = {
            'hooks': [remove_bokeh_logo],
            **self._sizing_opts,
        }
        return plot_element.opts(**base_opts)

    def _update_autoscaler_and_get_framewise(self, data: sc.DataArray) -> bool:
        """Update autoscaler with data and return whether bounds changed."""
        return self.autoscaler.update_bounds(data)

    @abstractmethod
    def plot(self, data: sc.DataArray) -> Any:
        """Create a plot from the given data. Must be implemented by subclasses."""


class LinePlotter(Plotter):
    """Plotter for line plots from scipp DataArrays."""

    @classmethod
    def from_params(cls, params: PlotParams1d):
        """Create LinePlotter from PlotParams1d."""
        # TODO: Use params to configure the plotter
        return cls(value_margin_factor=0.1, combine_mode='overlay')

    def plot(self, data: sc.DataArray) -> hv.Curve:
        """Create a line plot from a scipp DataArray."""
        if data.coords.is_edges(data.dim):
            da = data.assign_coords({data.dim: sc.midpoints(data.coords[data.dim])})
        else:
            da = data
        framewise = self._update_autoscaler_and_get_framewise(da)

        curve = to_holoviews(da)
        return curve.opts(framewise=framewise, ylim=(0, None))


class ImagePlotter(Plotter):
    """Plotter for 2D images from scipp DataArrays."""

    def __init__(
        self,
        scale_opts: PlotScaleParams2d,
        **kwargs,
    ):
        """
        Initialize the image plotter.

        Parameters
        ----------
        **kwargs:
            Additional keyword arguments passed to the base class.
        """
        super().__init__(**kwargs)
        self._base_opts = {
            'colorbar': True,
            'cmap': 'viridis',
            'logx': True if scale_opts.x_scale == PlotScale.log else False,
            'logy': True if scale_opts.y_scale == PlotScale.log else False,
            'logz': True,
        }

    @classmethod
    def from_params(cls, params: PlotParams2d):
        """Create SumImagePlotter from PlotParams2d."""
        return cls(
            value_margin_factor=0.1,
            plot_aspect=params.plot_aspect,
            scale_opts=params.plot_scale,
        )

    def plot(self, data: sc.DataArray) -> hv.Image:
        """Create a 2D plot from a scipp DataArray."""
        # With logz=True we need to exclude zero values for two reasons:
        # 1. The value bounds calculation should properly adjust the color limits. Since
        #    zeros can never be included we want to adjust to the lowest positive value.
        # 2. Holoviews does not seem to allow empty `clim` when `logz=True` for empty
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

        framewise = self._update_autoscaler_and_get_framewise(masked)
        # We are using the masked data here since Holoviews (at least with the Bokeh
        # backend) show values below the color limits with the same color as the lowest
        # value in the colormap, which is not what we want for, e.g., zeros on a log
        # scale plot. The nan values will be shown as transparent.
        histogram = to_holoviews(masked)
        return histogram.opts(
            framewise=framewise,
            clim=(self.autoscaler.value_bounds[0], self.autoscaler.value_bounds[1]),
            **self._base_opts,
        )


class SumImagePlotter(ImagePlotter):
    """Plotter for 2D images created by summing multiple scipp DataArrays."""

    @classmethod
    def from_params(cls, params):
        """Create SumImagePlotter from PlotParams2d."""
        # TODO: Use params to configure the plotter
        return cls(value_margin_factor=0.1)

    def plot(self, data: dict[ResultKey, sc.DataArray]) -> hv.Image:
        """Create a 2D plot from a dictionary of scipp DataArrays."""
        if data is None:
            return super().plot(data)
        reducer = sc.reduce(list(data.values()))
        # This is not a great check, probably the whole approach is questionable, but
        # this probably does the job for Dream focussed vs. vanadium normalized data.
        if next(iter(data.values())).unit == '':
            combined = reducer.nanmean()
        else:
            combined = reducer.nansum()
        return super().plot(combined)


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
