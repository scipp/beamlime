# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import NewType, cast

import matplotlib.pyplot as plt
import numpy as np
import plopp as pp
import scipp as sc
import scippnexus as snx
from ess.reduce.live import raw
from matplotlib.gridspec import GridSpec
from streaming_data_types.eventdata_ev44 import EventData

from beamlime.config.raw_detectors import (
    dream_detectors_config,
    loki_detectors_config,
    nmx_detectors_config,
)

from ..workflow_protocols import WorkflowResult
from .daemons import DataPieceReceived, NexusFilePath

try:
    from appstract import logging as applogs
    from appstract import mixins as appmixins
except ImportError as e:
    raise ImportError(
        "The appstract package is required for the old version of beamlime."
        "Please install it using `pip install appstract`."
    ) from e

detector_registry = {
    'DREAM': dream_detectors_config,
    'LoKI': loki_detectors_config,
    'NMX': nmx_detectors_config,
}


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


class RawCountHandler(appmixins.LogMixin):
    """
    Continuously handle raw counts for every ev44 message.

    This ignores run-start and run-stop messages.
    """

    def __init__(
        self,
        *,
        logger: applogs.AppLogger,
        nexus_file: NexusFilePath,
        update_every: float,
        window_length: float,
    ) -> None:
        """
        Parameters
        ----------
        logger:
            Logger instance.
        nexus_file:
            Path to the nexus file that has static information such as detector numbers
            and pixel positions. This is not necessarily the same as the current run,
            provided that there is no difference in relevant detector information.
        update_every:
            Update the raw-detector view every UPDATE_EVERY seconds.
        window_length:
            Length of the window in seconds. Must be larger or equal to update-every.
        """
        self.logger = logger
        self._detectors: dict[str, list[str]] = {}
        self._views: dict[str, raw.RollingDetectorView] = {}
        self._update_every = sc.scalar(update_every, unit='s').to(
            unit='ns', dtype='int64'
        )
        self._window_length = round(window_length / update_every)
        self._next_update: int | None = None

        with snx.File(nexus_file) as f:
            entry = next(iter(f[snx.NXentry].values()))
            instrument = next(iter(entry[snx.NXinstrument].values()))
            self._instrument = str(instrument['name'][()])

        for name, detector in detector_registry[self._instrument]['detectors'].items():
            self._detectors.setdefault(detector['detector_name'], []).append(name)
            self._views[name] = raw.RollingDetectorView.from_nexus(
                nexus_file,
                detector_name=detector['detector_name'],
                window=self._window_length,
                projection=detector['projection'],
                resolution=detector.get('resolution'),
                pixel_noise=detector.get('pixel_noise'),
            )
        self._chunk = {name: 0 for name in self._views}
        self._buffer = {name: [] for name in self._views}
        self.info("Initialized with %s", list(self._views))

    def handle(self, message: DataPieceReceived) -> WorkflowResultUpdate | None:
        data_piece = cast(EventData, message.content.deserialized)
        detname = data_piece.source_name.split('/')[-1]
        event_id = data_piece.pixel_id
        if (det := self._detectors.get(detname)) is not None:
            for name in det:
                buffer = self._buffer[name]
                buffer.append(event_id)
        else:
            self.info("Ignoring data for %s", detname)
        reference_time = sc.datetime(int(data_piece.reference_time), unit='ns')
        if self._next_update is None:
            self._next_update = reference_time.copy()
        if reference_time >= self._next_update:
            self.info("Updating views at %s", reference_time)
            # If there were no pulses for a while we need to skip several updates.
            # Note that we do not simply set _next_update based on reference_time
            # to avoid drifts.
            self._next_update += (
                (reference_time - self._next_update) // self._update_every + 1
            ) * self._update_every
            results = {}
            for name, det in self._views.items():
                buffer = self._buffer[name]
                if len(buffer):
                    det.add_counts(np.concatenate(buffer))
                    buffer.clear()
                # The detector view supports multiple windows, but we do not have
                # configured any instrument that uses this currently, so this is a
                # length-1 tuple.
                for window in (self._window_length,):
                    length = (window * self._update_every).to(dtype='float64', unit='s')
                    key = (self._instrument, name, f'window={round(length.value, 1)} s')
                    results[key] = det.get(window=window)
            self.info("Publishing result for detectors %s", list(self._views))
            return WorkflowResultUpdate(results)

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group("Raw Counter Configuration")
        group.add_argument(
            "--static-file-path",
            help="Path to the nexus file that has static information.",
            type=str,
            required=True,
        )
        group.add_argument(
            "--update-every",
            help="Update the raw-detector view every UPDATE_EVERY seconds. "
            "Can be a float.",
            type=float,
            default=1.0,
        )
        group.add_argument(
            "--window-length",
            help="Length of the window in seconds. Must be larger or equal to "
            "update-every. Can be a float.",
            type=float,
            default=10.0,
        )

    @classmethod
    def from_args(
        cls, logger: applogs.AppLogger, args: argparse.Namespace
    ) -> "RawCountHandler":
        return cls(
            logger=logger,
            nexus_file=args.static_file_path,
            update_every=args.update_every,
            window_length=args.window_length,
        )


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}"))


MaxPlotColumn = NewType("MaxPlotColumn", int)
DefaultMaxPlotColumn = MaxPlotColumn(3)


class PlotStreamer(appmixins.LogMixin):
    def __init__(
        self,
        *,
        logger: applogs.AppLogger,
        max_column: MaxPlotColumn = DefaultMaxPlotColumn,
    ) -> None:
        self.logger = logger
        self.figures = {}
        self.artists = {}
        self.max_column = max_column
        super().__init__()

    def plot_item(self, name: str, data: sc.DataArray) -> None:
        figure = self.figures.get(name)
        if figure is None:
            plot = pp.plot(data, title=name)
            # TODO Either improve Plopp's update method, or handle multiple artists
            if len(plot.artists) > 1:
                raise NotImplementedError("Data with multiple items not supported.")
            self.artists[name] = next(iter(plot.artists))
            self.figures[name] = plot
        else:
            figure.update({self.artists[name]: data})

    def update_histogram(self, message: WorkflowResultUpdate) -> None:
        content = message.content
        for name, data in content.items():
            self.plot_item(name, data)

    def show(self):
        """Show the figures in a grid layout."""
        from plopp.widgets import Box

        figures = list(self.figures.values())
        n_rows = len(figures) // self.max_column + 1
        return Box(
            [
                figures[
                    i_row * self.max_column : i_row * self.max_column + self.max_column
                ]
                for i_row in range(n_rows)
            ],
        )


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


class PlotSaver(PlotStreamer):
    """Plot handler to save the updated histogram into an image file."""

    def __init__(
        self,
        *,
        logger: applogs.AppLogger,
        image_path_prefix: ImagePath,
        max_column: MaxPlotColumn = DefaultMaxPlotColumn,
    ) -> None:
        super().__init__(logger=logger, max_column=max_column)
        self.image_path_prefix = image_path_prefix
        self._fig = None
        self._images = {}

    def _setup_figure(self, message: WorkflowResultUpdate) -> None:
        instrument = next(iter(message.content.keys()))[0]
        dashboard = detector_registry[instrument]['dashboard']
        ncol = dashboard['ncol']
        nrow = dashboard['nrow']
        figsize_scale = dashboard.get('figsize_scale', 6)
        self._fig = plt.figure(figsize=(figsize_scale * ncol, figsize_scale * nrow))
        gs = GridSpec(nrow, ncol, figure=self._fig)
        for key, da in message.content.items():
            instrument, detname, params = key
            name = f"{detname} {params}"
            grid_loc = detector_registry[instrument]['detectors'][detname]['gridspec']
            ax = self._fig.add_subplot(gs[grid_loc])
            extra = {'norm': 'log', 'vmax': 1e4}
            self._images[key] = (_plot_2d if da.ndim == 2 else _plot_1d)(
                da, ax=ax, title=name, **extra
            )
        self._fig.tight_layout()

    def save_histogram(self, message: WorkflowResultUpdate) -> None:
        start = time.time()

        image_file_name = f"{self.image_path_prefix}.png"
        self.info("Received histogram(s), saving into %s...", image_file_name)

        if self._fig is None:
            self._setup_figure(message)
        else:
            for key, da in message.content.items():
                self._images[key].set_data(da.values)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            # Saving into a tempfile avoids flickering when the image is updated, if
            # the image is displayed in a GUI.
            self._fig.savefig(tmpfile.name, dpi=150)
            shutil.move(tmpfile.name, image_file_name)
        self.info("Plotting took %.2f s", time.time() - start)

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group("Plot Saver Configuration")
        group.add_argument(
            "--image-path-prefix",
            help="Path to save the histogram image.",
            type=str,
            default=None,
        )

    @classmethod
    def from_args(
        cls, logger: applogs.AppLogger, args: argparse.Namespace
    ) -> "PlotSaver":
        return cls(
            logger=logger,
            image_path_prefix=args.image_path_prefix or random_image_path(),
        )
