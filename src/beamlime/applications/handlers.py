# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import shutil
import tempfile
import time
from dataclasses import dataclass
from numbers import Number
from typing import NewType, cast

import matplotlib.pyplot as plt
import numpy as np
import plopp as pp
import scipp as sc
import scippnexus as snx
from ess.reduce.live import raw
from ess.reduce.nexus.json_nexus import JSONGroup
from matplotlib.gridspec import GridSpec
from streaming_data_types.eventdata_ev44 import EventData

from beamlime.logging import BeamlimeLogger

from ..workflow_protocols import LiveWorkflow, WorkflowResult
from ._nexus_helpers import (
    NexusStore,
    StreamModuleKey,
    StreamModuleValue,
    merge_message_into_nexus_store,
    nexus_path_as_string,
)
from .base import HandlerInterface
from .daemons import DataPieceReceived, NexusFilePath, RunStart

ResultRegistry = NewType("ResultRegistry", dict[str, sc.DataArray])
"""Workflow result container."""
Events = NewType("Events", list[sc.DataArray])
MergeMessageCountInterval = NewType("MergeMessageCountInterval", Number)
"""Every MergeMessageCountInterval-th message the data assembler receives
the data reduction is run"""
MergeMessageTimeInterval = NewType("MergeMessageTimeInterval", Number)
"""The data reduction is run when the DataAssembler receives a message and the time
since the last reduction exceeds the length of the interval (in seconds)"""


@dataclass
class WorkflowInput:
    nxevent_data: dict[str, JSONGroup]
    nxlog: dict[str, JSONGroup]


@dataclass
class DataReady:
    content: WorkflowInput


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


def maxcount_or_maxtime(maxcount: Number, maxtime: Number):
    if maxcount <= 0:
        raise ValueError("maxcount must be positive")
    if maxtime <= 0:
        raise ValueError("maxtime must be positive")

    count = 0
    last = time.time()

    def run():
        nonlocal count, last
        count += 1
        if count >= maxcount or time.time() - last >= maxtime:
            count = 0
            last = time.time()
            return True

    return run


DETECTOR_BANK_SIZES = {
    'larmor_detector': {'layer': 4, 'tube': 32, 'straw': 7, 'pixel': 512}
}

# NOTE: This config should obviously be moved/managed elsewhere, but this works for
# for testing and is convenient for now.
_res_scale = 8
# Order in 'resolution' matters so plots have X as horizontal axis and Y as vertical.
detector_registry = {}
# The other DREAM detectors have non-consecutive detector numbers. This is not
# supported currently
_dream = {
    'dashboard': {'nrow': 3, 'ncol': 2},
    'detectors': {
        'endcap_backward': {
            'detector_name': 'endcap_backward_detector',
            'resolution': {'y': 30 * _res_scale, 'x': 20 * _res_scale},
            'gridspec': (0, 0),
        },
        'endcap_forward': {
            'detector_name': 'endcap_forward_detector',
            'resolution': {'y': 20 * _res_scale, 'x': 20 * _res_scale},
            'gridspec': (0, 1),
        },
        # We use the arc length instead of phi as it makes it easier to get a correct
        # aspect ratio for the plot if both axes have the same unit.
        'mantle_projection': {
            'detector_name': 'mantle_detector',
            'resolution': {'arclength': 10 * _res_scale, 'z': 40 * _res_scale},
            'projection': 'cylinder_mantle_z',
            'gridspec': (1, slice(None, 2)),
        },
        # Different view of the same detector, showing just the front layer instead of
        # a projection.
        'mantle_front_layer': {
            'detector_name': 'mantle_detector',
            'projection': raw.LogicalView(
                fold={
                    'wire': 32,
                    'module': 5,
                    'segment': 6,
                    'strip': 256,
                    'counter': 2,
                },
                transpose=('wire', 'module', 'segment', 'counter', 'strip'),
                select={'wire': 0},
                flatten={'z_id': ('module', 'segment', 'counter')},
            ),
            'gridspec': (2, slice(None, 2)),
        },
    },
}
_loki = {
    'dashboard': {'nrow': 3, 'ncol': 9, 'figsize_scale': 3},
    'detectors': {
        'Rear-detector': {
            'detector_name': 'loki_detector_0',
            'resolution': {'y': 12 * _res_scale, 'x': 12 * _res_scale},
            'gridspec': (slice(0, 3), slice(0, 3)),
            'pixel_noise': 'cylindrical',
        },
        # First window frame
        'loki_detector_1': {
            'detector_name': 'loki_detector_1',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (2, 4),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_2': {
            'detector_name': 'loki_detector_2',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 3),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_3': {
            'detector_name': 'loki_detector_3',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (0, 4),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_4': {
            'detector_name': 'loki_detector_4',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 5),
            'pixel_noise': 'cylindrical',
        },
        # Second window frame
        'loki_detector_5': {
            'detector_name': 'loki_detector_5',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (2, 7),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_6': {
            'detector_name': 'loki_detector_6',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 6),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_7': {
            'detector_name': 'loki_detector_7',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (0, 7),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_8': {
            'detector_name': 'loki_detector_8',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 8),
            'pixel_noise': 'cylindrical',
        },
    },
}
detector_registry['DREAM'] = _dream
detector_registry['LoKI'] = _loki


class RawCountHandler(HandlerInterface):
    """
    Continuously handle raw counts for every ev44 message.

    This ignores run-start and run-stop messages.
    """

    def __init__(self, *, logger: BeamlimeLogger, nexus_file: NexusFilePath) -> None:
        self.logger = logger
        self._pulse = 0
        self._previous: sc.DataArray | None = None
        self._detectors: dict[str, list[str]] = {}
        self._views: dict[str, raw.RollingDetectorView] = {}

        with snx.File(nexus_file) as f:
            entry = next(iter(f[snx.NXentry].values()))
            instrument = next(iter(entry[snx.NXinstrument].values()))
            self._instrument = str(instrument['name'][()])

        for name, detector in detector_registry[self._instrument]['detectors'].items():
            self._detectors.setdefault(detector['detector_name'], []).append(name)
            self._views[name] = raw.RollingDetectorView.from_nexus(
                nexus_file,
                detector_name=detector['detector_name'],
                window=100,
                projection=detector.get('projection', 'xy_plane'),
                resolution=detector.get('resolution'),
                pixel_noise=detector.get('pixel_noise', sc.scalar(0.01, unit='m')),
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
                self._pulse += 1
        else:
            self.info("Ignoring data for %s", detname)
            return
        npulse = 10
        if self._pulse % npulse == 0:
            results = {}
            for name, det in self._views.items():
                buffer = self._buffer[name]
                if len(buffer):
                    det.add_counts(np.concatenate(buffer))
                    buffer.clear()
                for window in (50,):
                    key = (self._instrument, name, f'window={window*npulse}')
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

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "RawCountHandler":
        return cls(logger=logger, nexus_file=args.static_file_path)


class DataAssembler(HandlerInterface):
    """Receives data and assembles it into a single data structure."""

    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        merge_every_nth: MergeMessageCountInterval = 1,
        max_seconds_between_messages: MergeMessageTimeInterval = float("inf"),
    ):
        self.static_filename: pathlib.Path
        self.streaming_modules: dict[StreamModuleKey, StreamModuleValue]
        self.logger = logger
        self._nexus_store: NexusStore = {}
        self._should_send_message = maxcount_or_maxtime(
            merge_every_nth, max_seconds_between_messages
        )

    def set_run_start(self, message: RunStart) -> None:
        self.streaming_modules = message.content.streaming_modules
        self.debug("Expecting data for modules: %s", self.streaming_modules.values())
        self.static_filename = pathlib.Path(message.content.filename)

    def merge_data_piece(self, message: DataPieceReceived) -> DataReady | None:
        module_spec = self.streaming_modules[message.content.key]
        merge_message_into_nexus_store(
            module_key=message.content.key,
            module_spec=module_spec,
            nexus_store=self._nexus_store,
            data=message.content.deserialized,
        )
        self.debug("Data piece merged for %s", message.content.key)
        if self._should_send_message():
            nxevent_data = {
                nexus_path_as_string(self.streaming_modules[key].path): value
                for key, value in self._nexus_store.items()
                if key.module_type == "ev44"
            }
            nxlog = {
                nexus_path_as_string(module_spec.path): value
                for key, value in self._nexus_store.items()
                if key.module_type == "f144"
            }
            result = DataReady(
                content=WorkflowInput(nxevent_data=nxevent_data, nxlog=nxlog)
            )
            self._nexus_store = {}
            return result

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group("Data Assembler Configuration")
        group.add_argument(
            "--merge-every-nth",
            help="Merge data every nth message.",
            type=int,
            default=1,
        )
        group.add_argument(
            "--max-seconds-between-messages",
            help="Maximum time between messages in seconds.\
                  This should be (N: int) times (number of messages per frame).",
            type=float,
            default=float("inf"),
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "DataAssembler":
        return cls(
            logger=logger,
            merge_every_nth=args.merge_every_nth,
            max_seconds_between_messages=args.max_seconds_between_messages,
        )


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data."""

    def __init__(
        self,
        workflow: type[LiveWorkflow],
        reuslt_registry: ResultRegistry | None = None,
    ) -> None:
        self.workflow_constructor = workflow
        self.workflow: LiveWorkflow
        self.result_registry = {} if reuslt_registry is None else reuslt_registry
        super().__init__()

    def set_run_start(self, message: RunStart) -> None:
        file_path = pathlib.Path(message.content.filename)
        self.debug("Initializing workflow with %s", file_path)
        self.workflow = self.workflow_constructor(file_path)

    def reduce_data(self, message: DataReady) -> WorkflowResultUpdate:
        nxevent_data = {
            key: JSONGroup(value) for key, value in message.content.nxevent_data.items()
        }
        nxlog = {key: JSONGroup(value) for key, value in message.content.nxlog.items()}
        self.info("Running data reduction")
        results = self.workflow(nxevent_data=nxevent_data, nxlog=nxlog)
        self.result_registry.update(results)
        return WorkflowResultUpdate(content=results)


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}"))


MaxPlotColumn = NewType("MaxPlotColumn", int)
DefaultMaxPlotColumn = MaxPlotColumn(3)


class PlotStreamer(HandlerInterface):
    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
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
        logger: BeamlimeLogger,
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
    def from_args(cls, logger: BeamlimeLogger, args: argparse.Namespace) -> "PlotSaver":
        return cls(
            logger=logger,
            image_path_prefix=args.image_path_prefix or random_image_path(),
        )
