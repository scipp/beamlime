# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
"""
Crude helper for simple Matplotlib plotting of 1D and 2D data.

This is using Matplotlib directly instead of Plopp to avoid performance issues
from pcolormesh. It is not intended for production at this point.
"""

from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import scipp as sc
from matplotlib.gridspec import GridSpec

from beamlime.config.raw_detectors import (
    dream_detectors_config,
    loki_detectors_config,
    nmx_detectors_config,
)

detector_registry = {
    'DREAM': dream_detectors_config,
    'LoKI': loki_detectors_config,
    'NMX': nmx_detectors_config,
}


def _plot_1d(
    da: sc.DataArray,
    *,
    ax: plt.Axes,
    title: str,
    norm: str | None = None,
    vmin: float | None = None,
    vmax: float | None = None,
    aspect: str | None = None,
    **kwargs,
):
    x = sc.midpoints(da.coords[da.dims[0]]).values
    image = ax.errorbar(x, da.values, yerr=np.sqrt(da.variances), fmt='o', **kwargs)
    if norm is not None:
        ax.set_yscale(norm)
    y_min = vmin if vmin is not None else 0
    y_max = vmax if vmax is not None else 1.05 * da.values.max()
    ax.set_ylim(y_min, y_max)
    ax.set_xlabel(f'{da.dims[0]} [{da.coords[da.dims[0]].unit}]')
    ax.set_ylabel(f'[{da.unit}]')
    ax.set_title(title)
    return image


def plot_images_with_offsets(
    *,
    ax,
    image_data_list,
    x_coords_list,
    y_coords_list,
    cmap='viridis',
    num_ticks=5,
    **kwargs,
):
    import numpy as np

    # Calculate common axis ranges
    x_min = min(x.min() for x in x_coords_list)
    x_max = max(x.max() for x in x_coords_list)
    y_min = min(y.min() for y in y_coords_list)
    y_max = max(y.max() for y in y_coords_list)

    # Plot each image with correct extent
    for image_data, x_coords, y_coords in zip(
        image_data_list, x_coords_list, y_coords_list, strict=True
    ):
        extent = [x_coords.min(), x_coords.max(), y_coords.min(), y_coords.max()]
        ax.imshow(
            image_data,
            extent=extent,
            origin='lower',
            interpolation='none',
            cmap=cmap,
            **kwargs,
        )

    # Set axis limits and ticks based on common ranges
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)
    ax.set_xticks(np.linspace(x_min, x_max, num=num_ticks))
    ax.set_yticks(np.linspace(y_min, y_max, num=num_ticks))

    # Set labels and title
    ax.set_xlabel('X-axis')
    ax.set_ylabel('Y-axis')
    ax.set_title('Images Offset by Coordinate Arrays')


def _plot_2d(
    da: sc.DataArray | sc.DataGroup,
    *,
    ax: plt.Axes,
    title: str,
    **kwargs,
):
    if isinstance(da, sc.DataGroup):
        das = list(da.values())
        xname = das[0].dims[1]
        yname = das[0].dims[0]
        return plot_images_with_offsets(
            ax=ax,
            image_data_list=[da.values for da in das],
            x_coords_list=[da.coords[xname].values for da in das],
            y_coords_list=[da.coords[yname].values for da in das],
            **kwargs,
        )

    x = da.coords.get(da.dims[1], sc.arange(da.dims[1], da.shape[1], unit=None))
    y = da.coords.get(da.dims[0], sc.arange(da.dims[0], da.shape[0], unit=None))
    if x.unit == y.unit:
        aspect = float((y[-1] - y[0]) / (x[-1] - x[0]))
        aspect *= da.shape[1] / da.shape[0]
    else:
        aspect = 'equal'

    values = da.values
    image = ax.imshow(values, **kwargs, aspect=aspect)
    ax.invert_yaxis()
    # Note: Drawing the colorbar actually takes a long time!
    # cbar = plt.colorbar(ax.images[0], ax=ax)
    # cbar.set_label(f'[{da.unit}]')
    ax.set_title(title)
    ax.set_xlabel(f'{da.dims[1]} [{x.unit}]')
    ax.set_ylabel(f'{da.dims[0]} [{y.unit}]')
    x_coords = x.values[::20]
    ax.set_xticks(np.linspace(0, values.shape[1] - 1, len(x_coords)))
    ax.set_xticklabels([round(x, 2) for x in x_coords])
    y_coords = y.values[::20]
    ax.set_yticks(np.linspace(0, values.shape[0] - 1, len(y_coords)))
    ax.set_yticklabels([round(y, 2) for y in y_coords])
    return image


class MatplotlibPlotter:
    """Plot handler to save the updated histogram into an image file."""

    def __init__(self) -> None:
        self._fig = None
        self._images = {}
        self._kwargs = {}

    @property
    def fig(self) -> plt.Figure:
        return self._fig

    def _setup_figure(self, das: dict[str, sc.DataArray], **kwargs: Any) -> None:
        instrument = next(iter(das.keys()))[0]
        dashboard = detector_registry[instrument]['dashboard']
        ncol = dashboard['ncol']
        nrow = dashboard['nrow']
        figsize_scale = dashboard.get('figsize_scale', 6)
        self._fig = plt.figure(figsize=(figsize_scale * ncol, figsize_scale * nrow))
        gs = GridSpec(nrow, ncol, figure=self._fig)
        for key, da in das.items():
            instrument, detname, params = key
            name = f"{detname} {params}"
            grid_loc = detector_registry[instrument]['detectors'][detname]['gridspec']
            ax = self._fig.add_subplot(gs[grid_loc])
            extra = {'norm': 'log', 'vmax': 1e4}
            extra.update(kwargs)
            self._images[key] = (_plot_2d if da.ndim == 2 else _plot_1d)(
                da, ax=ax, title=name, **extra
            )
        self._fig.tight_layout()

    def update_data(self, das: dict[str, sc.DataArray], **kwargs: Any) -> None:
        if self._fig is None or self._kwargs != kwargs:
            self._setup_figure(das, **kwargs)
            self._kwargs = kwargs
        else:
            for key, da in das.items():
                self._images[key].set_data(da.values)
