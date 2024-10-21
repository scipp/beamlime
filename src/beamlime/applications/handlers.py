# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import time
from dataclasses import dataclass
from math import ceil
from numbers import Number
from typing import NewType
import numpy as np

import matplotlib.pyplot as plt
import plopp as pp
import scipp as sc
import scippnexus as snx
from ess.reduce.nexus.json_nexus import JSONGroup
from ess.reduce.live import raw

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
from .daemons import DataPieceReceived, RunStart, NexusFilePath

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
        self._pulse = -1
        self._previous: sc.DataArray | None = None
        self._detectors: dict[str, raw.RollingDetectorView] = {}

        with snx.File(nexus_file) as f:
            entry = next(iter(f[snx.NXentry].values()))
            instrument = next(iter(entry[snx.NXinstrument].values()))
            detectors = instrument[snx.NXdetector]
            for name, detector in detectors.items():
                detector_number = detector['detector_number'][()]
                x_pixel_offset = detector['x_pixel_offset'][()]
                y_pixel_offset = detector['y_pixel_offset'][()]
                z_pixel_offset = detector['z_pixel_offset'][()]
                if (sizes := DETECTOR_BANK_SIZES.get(name)) is not None:
                    detector_number = detector_number.fold(
                        dim='detector_number', sizes=sizes
                    )
                    x_pixel_offset = x_pixel_offset.fold(
                        dim='detector_number', sizes=sizes
                    )
                    y_pixel_offset = y_pixel_offset.fold(
                        dim='detector_number', sizes=sizes
                    )
                    z_pixel_offset = z_pixel_offset.fold(
                        dim='detector_number', sizes=sizes
                    )
                params = raw.DetectorParams(
                    detector_number=detector_number,
                    x_pixel_offset=x_pixel_offset,
                    y_pixel_offset=y_pixel_offset,
                    z_pixel_offset=z_pixel_offset,
                )
                self._detectors[detector.name] = raw.RollingDetectorView(
                    params, window=1000
                )
            self.info("Initialized with %s", list(self._detectors))

    def handle(self, message: DataPieceReceived) -> WorkflowResultUpdate | None:
        name = message.content.deserialized['source_name']
        event_id = message.content.deserialized['pixel_id']
        self.info("Received %s events for %s", len(event_id), name)
        if (det := self._detectors.get(name)) is not None:
            det.add_counts(event_id)
            self.info("Total counts for %s: %s", name, det.data.sum().value)
            self._pulse += 1
            # data = det.data.sum(('layer')).flatten(
            #    dims=('tube', 'straw'), to='straw'
            # )
            if self._pulse % 5 == 0:
                results = {}
                for window in (20, None):
                    data = det.get(window)
                    # data.sum(('layer', 'straw'))
                    # data = data.fold('pixel', sizes={'pixel': -1, '_': 8}).sum('_')
                    results[f'{name.split("/")[-1]} window={window}'] = data
                return WorkflowResultUpdate(results)
            else:
                return None
        else:
            self.info("Ignoring data for %s", name)

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "RawCountHandler":
        return cls(logger=logger, nexus_file=args.nexus_file_path)


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
DefaultMaxPlotColumn = MaxPlotColumn(2)


def project_xy(
    x: sc.Variable, y: sc.Variable, z: sc.Variable
) -> tuple[sc.Variable, sc.Variable, sc.Variable]:
    # Initialize projected coordinate arrays
    x_proj = sc.zeros_like(x)
    y_proj = sc.zeros_like(y)
    z_proj = sc.ones_like(z)

    # Compute scaling factor t = 1 / z where z != 0
    t = sc.zeros_like(z)
    t = 1 / z

    # Compute projected coordinates
    x_proj = x * t
    y_proj = y * t

    return x_proj, y_proj, z_proj


class PlotStreamer(HandlerInterface):
    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        max_column: MaxPlotColumn = DefaultMaxPlotColumn,
    ) -> None:
        self.logger = logger
        self.figures = {}
        self.data_nodes = {}
        self.artists = {}
        self.max_column = max_column
        super().__init__()

    def plot_item(self, name: str, da: sc.DataArray) -> None:
        # data = data['layer', 0]
        if False:
            data = da.fold('pixel', sizes={'pixel': -1, '_': 8})
            data.coords['x'] = data.coords['x_pixel_offset']
            data.coords['y'] = data.coords['y_pixel_offset']
            data.coords['z'] = data.coords['z_pixel_offset']
            for combine in ('straw', '_'):
                tmp = data.sum(combine)
                tmp.coords['x'] = data.coords['x'].mean(combine)
                tmp.coords['y'] = data.coords['y'].mean(combine)
                tmp.coords['z'] = data.coords['z'].mean(combine)
                data = tmp
            data = data.flatten(to='pix').copy()
        else:
            data = da.flatten(to='pixel').copy()
            position = sc.vector([-0.49902349, 0.43555999, 4.09899989], unit='m')
            beam_center = sc.vector([-0.02864121, -0.01850989, 0.0], unit='m')
            offset = sc.spatial.as_vectors(
                x=data.coords['x_pixel_offset'],
                y=data.coords['y_pixel_offset'],
                z=data.coords['z_pixel_offset'],
            )
            pos = position + offset - beam_center
            x, y, z = project_xy(pos.fields.x, pos.fields.y, pos.fields.z)
            tmp = sc.DataArray(data.data, coords={'x': x, 'y': y})
            data = tmp.hist(x=100, y=100)

        print(data)

        try:
            figure = self.figures.get(name)
            if figure is None:
                data_node = pp.Node(data)
                self.data_nodes[name] = data_node
                # self.artists[name] = next(iter(plot.artists))
                if False and 'x' in data.coords:
                    self.figures[name] = pp.scatter3dfigure(
                        data_node,
                        # title=name,
                        norm='log',
                        pixel_size=0.02,
                        cbar=True,
                        vmin=sc.scalar(1, unit='counts'),
                        vmax=sc.scalar(100, unit='counts'),
                    )
                else:
                    self.figures[name] = pp.plot(
                        data_node,
                        title=name,
                        norm='log',
                        vmin=sc.scalar(1, unit='counts'),
                        vmax=sc.scalar(100, unit='counts'),
                        aspect='equal',
                    )
                # self.figures[name] = pp.imagefigure(data_node, title=name)
            else:
                self.data_nodes[name].input_value.data = data.data
                self.data_nodes[name].notify_children(message=None)
                # figure.update({self.artists[name]: data})
        except Exception as e:
            import traceback

            traceback.print_exc()
            raise e

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
        image_file_name = f"{self.image_path_prefix}.png"
        self.info("Received histogram(s), saving into %s...", image_file_name)

        fig, axes = plt.subplots(
            ceil(len(message.content) / self.max_column),
            self.max_column,
            figsize=(16, 8),
        )
        plt.subplots_adjust(wspace=0.3, hspace=0.3)
        for (name, da), ax in zip(message.content.items(), axes.flat, strict=False):
            # TODO We need a way of configuring plot options for each item. The below
            # works for the SANS 60387-2022-02-28_2215.nxs AgBeh file and is useful for
            # testing.
            if da.unit == '':
                extra = {'norm': 'log', 'vmin': 1e-1, 'vmax': 1e1, 'aspect': 'equal'}
            else:
                extra = {}
            da.plot(ax=ax, title=name, **extra, norm='log')
        fig.savefig(image_file_name, dpi=100)
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
        )
