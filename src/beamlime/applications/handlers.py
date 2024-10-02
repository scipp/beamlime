# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import time
from dataclasses import dataclass
from numbers import Number
from typing import NewType

import plopp as pp
import scipp as sc
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
from .daemons import DataPieceReceived, RunStart

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
                nexus_path_as_string(module_spec.path): JSONGroup(value)
                for key, value in self._nexus_store.items()
                if key.module_type == "ev44"
            }
            nxlog = {
                nexus_path_as_string(module_spec.path): JSONGroup(value)
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
            plot = pp.plot(data, title='\n'.join(name.split("-")))
            # line break for long names
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
            ]
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
        from plopp import tiled

        super().update_histogram(message)
        image_file_name = f"{self.image_path_prefix}.png"
        self.info("Received histogram(s), saving into %s...", image_file_name)
        plot_tile = tiled((len(self.figures) // self.max_column) + 1, self.max_column)
        for i_fig, figure in enumerate(self.figures.values()):
            plot_tile[i_fig // self.max_column, i_fig % self.max_column] = figure

        plot_tile.save(image_file_name)

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
