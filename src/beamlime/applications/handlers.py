# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import NewType

import scipp as sc

from beamlime.logging import BeamlimeLogger

from ..stateless_workflow import StatelessWorkflow, WorkflowResult
from .base import HandlerInterface
from .daemons import DetectorDataReceived, RunStart


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data."""

    def __init__(self, workflow: StatelessWorkflow) -> None:
        from ._nexus_helpers import NexusContainer

        self.nexus_container: NexusContainer
        self.workflow = workflow
        super().__init__()

    def update_nexus_template(self, message: RunStart) -> None:
        self.nexus_container = message.content
        self.info("Received nexus template, ready to process the data.")

    def process_message(self, message: DetectorDataReceived) -> WorkflowResultUpdate:
        self.info("Received data from %s.", message.content["source_name"])
        self.nexus_container.insert_ev44(message.content)
        return WorkflowResultUpdate(
            content=self.workflow(self.nexus_container.nexus_dict)
        )


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

    def __init__(self, logger: BeamlimeLogger, image_path: ImagePath) -> None:
        super().__init__(logger)
        self.image_path = image_path

    def save_histogram(self, message: WorkflowResultUpdate) -> None:
        super().update_histogram(message)
        self.info("Received histogram, saving into %s...", self.image_path)
        for name, figure in self.figures.items():
            figure.save(f'{self.image_path}-{name}.png')
