# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import shutil
import tempfile
from dataclasses import dataclass
from math import ceil
from numbers import Number
from time import time
from typing import NewType

import matplotlib.pyplot as plt
import numpy as np
import plopp as pp
import scipp as sc
import scippnexus as snx
from ess.reduce.live import raw
from ess.reduce.nexus.json_nexus import JSONGroup

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


class RawCountHandler(HandlerInterface):
    """
    Continuously handle raw counts for every ev44 message.

    This ignores run-start and run-stop messages.
    """

    def __init__(self, *, logger: BeamlimeLogger, nexus_file: NexusFilePath) -> None:
        self.logger = logger
        self._pulse = 0
        self._previous: sc.DataArray | None = None
        self._detectors: dict[str, raw.RollingDetectorView] = {}

        with snx.File(nexus_file) as f:
            entry = next(iter(f[snx.NXentry].values()))
            instrument = next(iter(entry[snx.NXinstrument].values()))
            path = instrument.name
            detectors = list(instrument[snx.NXdetector])
        detectors = {name: name for name in detectors}
        for name, nexus_name in detectors.items():
            if name.split('/')[-1].startswith('loki'):
                xres = 90 if name[-1] in ('0', '1', '3', '5', '7') else 30
                yres = 90 if name[-1] in ('0', '2', '4', '6', '8') else 30
            else:
                xres = 128
                yres = 128
            self._detectors[f'{path}/{name}'] = raw.RollingDetectorView.from_nexus(
                nexus_file, detector_name=nexus_name, window=100, xres=xres, yres=yres
            )
        self._chunk = {name: 0 for name in self._detectors}
        self._buffer = {name: [] for name in self._detectors}
        self.info("Initialized with %s", list(self._detectors))

    def handle(self, message: DataPieceReceived) -> WorkflowResultUpdate | None:
        name = message.content.deserialized['source_name']
        event_id = message.content.deserialized['pixel_id']
        if (det := self._detectors.get(name)) is not None:
            buffer = self._buffer[name]
            buffer.append(event_id)
            self._pulse += 1
        else:
            self.info("Ignoring data for %s", name)
            return
        npulse = 100
        if self._pulse % npulse == 0:
            results = {}
            for name, det in self._detectors.items():
                buffer = self._buffer[name]
                if len(buffer):
                    det.add_counts(np.concatenate(buffer))
                    buffer.clear()
                for window in (1,):
                    key = f'{name.split("/")[-1]} window={window*npulse}'
                    results[key] = det.get(window=window)
            self.info("Publishing result for detectors %s", list(self._detectors))
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
NormalizeHistogramFlag = NewType("NormalizeHistogramFlag", bool)
DefaultNormalizeHistogramFlag = NormalizeHistogramFlag(False)


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
    ax.errorbar(x, da.values, yerr=np.sqrt(da.variances), fmt='o', **kwargs)
    if norm is not None:
        ax.set_yscale(norm)
    y_min = vmin if vmin is not None else 0
    y_max = vmax if vmax is not None else 1.05 * da.values.max()
    ax.set_ylim(y_min, y_max)
    ax.set_xlabel(f'{da.dims[0]} [{da.coords[da.dims[0]].unit}]')
    ax.set_ylabel(f'[{da.unit}]')
    ax.set_title(title)


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

    x = da.coords[da.dims[1]].values
    y = da.coords[da.dims[0]].values
    aspect = (x[-1] - x[0]) / (y[-1] - y[0])

    values = da.values
    ax.imshow(values, **kwargs, aspect=aspect)
    ax.invert_yaxis()
    # Note: Drawing the colorbar actually takes a long time!
    # cbar = plt.colorbar(ax.images[0], ax=ax)
    # cbar.set_label(f'[{da.unit}]')
    ax.set_title(title)
    ax.set_xlabel(f'{da.dims[1]} [{da.coords[da.dims[1]].unit}]')
    ax.set_ylabel(f'{da.dims[0]} [{da.coords[da.dims[0]].unit}]')
    x_coords = da.coords[da.dims[1]].values[::20]
    ax.set_xticks(np.linspace(0, values.shape[1] - 1, len(x_coords)))
    ax.set_xticklabels([round(x, 2) for x in x_coords])
    y_coords = da.coords[da.dims[0]].values[::20]
    ax.set_yticks(np.linspace(0, values.shape[0] - 1, len(y_coords)))
    ax.set_yticklabels([round(y, 2) for y in y_coords])


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

    def save_histogram(self, message: WorkflowResultUpdate) -> None:
        start = time()

        image_file_name = f"{self.image_path_prefix}.png"
        self.info("Received histogram(s), saving into %s...", image_file_name)

        nplot = len(message.content)
        nrow = ceil(nplot / self.max_column)
        ncol = min(nplot, self.max_column)
        fig, axes = plt.subplots(nrow, ncol, figsize=(6 * ncol, 6 * nrow))
        for (name, da), ax in zip(message.content.items(), axes.flat, strict=False):
            # TODO We need a way of configuring plot options for each item. The below
            # works for the SANS 60387-2022-02-28_2215.nxs AgBeh file and is useful for
            # testing.
            extra = {'norm': 'log'}
            if da.ndim == 2:
                _plot_2d(da, ax=ax, title=name, **extra)
            elif isinstance(da, sc.DataArray):
                _plot_1d(da, ax=ax, title=name, **extra)
            else:
                da.plot(ax=ax, title=name, **extra)
        fig.tight_layout()
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            # Saving into a tempfile avoids flickering when the image is updated, if
            # the image is displayed in a GUI.
            fig.savefig(tmpfile.name, dpi=100)
            shutil.move(tmpfile.name, image_file_name)
        self.logger.info("Plotting took %.2f s", time() - start)
        plt.close(fig)

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
            normalize=args.normalize,
        )
