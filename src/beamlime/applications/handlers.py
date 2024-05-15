# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import time
from dataclasses import dataclass
from numbers import Number
from typing import NewType

import scipp as sc
from scippneutron.io.nexus._json_nexus import JSONGroup

from beamlime.logging import BeamlimeLogger

from ..stateless_workflow import StatelessWorkflow, WorkflowResult
from ._nexus_helpers import combine_store_and_structure, merge_message_into_store
from .base import HandlerInterface
from .daemons import (
    ChopperDataReceived,
    DetectorDataReceived,
    LogDataReceived,
    RunStart,
)

Events = NewType("Events", list[sc.DataArray])
MergeMessageCountInterval = NewType("MergeMessageCountInterval", Number)
'''Every MergeMessageCountInterval-th message the data assembler receives
the data reduction is run'''
MergeMessageTimeInterval = NewType("MergeMessageTimeInterval", Number)
'''The data reduction is run when the DataAssembler receives a message and the time
since the last reduction exceeds the length of the interval (in seconds)'''


@dataclass
class DataReady:
    content: JSONGroup


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


def maxcount_or_maxtime(maxcount: Number, maxtime: Number):
    if maxcount <= 0:
        raise ValueError('maxcount must be positive')
    if maxtime <= 0:
        raise ValueError('maxtime must be positive')

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
        merge_every_nth: MergeMessageCountInterval = 1,
        max_seconds_between_messages: MergeMessageTimeInterval = float("inf"),
    ):
        self._store = {}
        self._should_send_message = maxcount_or_maxtime(
            merge_every_nth, max_seconds_between_messages
        )

    def set_run_start(self, message: RunStart) -> None:
        self.structure = message.content

    def _merge_message_and_return_response_if_ready(self, kind, message):
        merge_message_into_store(self._store, self.structure, (kind, message))
        if self._should_send_message():
            message = DataReady(
                combine_store_and_structure(self._store, self.structure),
            )
            self._store = {}
            return message

    def assemble_detector_data(self, message: DetectorDataReceived) -> DataReady:
        return self._merge_message_and_return_response_if_ready('ev44', message.content)

    def assemble_log_data(self, message: LogDataReceived) -> DataReady:
        return self._merge_message_and_return_response_if_ready('f144', message.content)

    def assemble_chopper_data(self, message: ChopperDataReceived) -> DataReady:
        return self._merge_message_and_return_response_if_ready('tdct', message.content)


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data."""

    def __init__(self, workflow: StatelessWorkflow) -> None:
        self.workflow = workflow
        super().__init__()

    def reduce_data(self, message: DataReady) -> WorkflowResultUpdate:
        content = JSONGroup(message.content)
        self.info("Running data reduction")
        return WorkflowResultUpdate(content=self.workflow(content))


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
            plot = data.plot(title=name)
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
