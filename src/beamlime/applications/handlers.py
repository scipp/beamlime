# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
from dataclasses import dataclass
from typing import NewType

import scipp as sc
from scippneutron.io.nexus._json_nexus import JSONGroup

from beamlime.logging import BeamlimeLogger

from ..stateless_workflow import StatelessWorkflow, WorkflowResult
from .base import HandlerInterface
from .daemons import DetectorDataReceived, RunStart

Events = NewType("Events", list[sc.DataArray])


@dataclass
class DataReady:
    content: JSONGroup


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


class DataAssembler(HandlerInterface):
    """Data assembler receives data and assembles it into a single data structure."""

    def __init__(self) -> None:
        from ._nexus_helpers import NexusContainer

        self.nexus_container: NexusContainer

    def set_run_start(self, message: RunStart) -> None:
        self.nexus_container = message.content

    def assemble_detector_data(self, message: DetectorDataReceived) -> DataReady:
        self.info("Received data from detector %s", message.content['source_name'])
        self.nexus_container.insert_ev44(message.content)
        return DataReady(
            content=self.nexus_container.nexus_dict  # should be a JSONGroup later.
        )


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data."""

    def __init__(self, workflow: StatelessWorkflow) -> None:
        self.workflow = workflow
        super().__init__()

    def reduce_data(self, message: DataReady) -> WorkflowResultUpdate:
        content = message.content
        self.info("Running data reduction")
        return WorkflowResultUpdate(content=self.workflow(content))


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}"))


class PlotStreamer(HandlerInterface):
    def __init__(self, logger: BeamlimeLogger) -> None:
        self.logger = logger
        self.figures = {}
        self.artists = {}
        super().__init__()

    def plot_item(self, name: str, data: sc.DataArray) -> None:
        figure = self.figures.get(name)
        if figure is None:
            plot = data.plot()
            # TODO Either improve Plopp's update method, or handle multiple artists
            if len(plot.artists) > 1:
                raise NotImplementedError("Data with multiple items not supported.")
            self.artists[name] = next(iter(plot.artists))
            self.figures[name] = plot
        else:
            figure.update(data, key=self.artists[name])

    def update_histogram(self, message: WorkflowResultUpdate) -> None:
        content = message.content
        for name, data in content.items():
            self.plot_item(name, data)


class PlotSaver(PlotStreamer):
    """Plot handler to save the updated histogram into an image file."""

    def __init__(self, logger: BeamlimeLogger, image_path_prefix: ImagePath) -> None:
        super().__init__(logger)
        self.image_path_prefix = image_path_prefix

    def save_histogram(self, message: WorkflowResultUpdate) -> None:
        super().update_histogram(message)
        self.info("Received histogram, saving into %s...", self.image_path_prefix)
        for name, figure in self.figures.items():
            figure.save(f'{self.image_path_prefix}-{name}.png')

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group('Plot Saver Configuration')
        group.add_argument(
            "--image-path-prefix",
            help="Path to save the histogram image.",
            type=str,
            default=None,
        )

    @classmethod
    def from_args(cls, logger: BeamlimeLogger, args: argparse.Namespace) -> "PlotSaver":
        return cls(logger, args.image_path_prefix or random_image_path())
