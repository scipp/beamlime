# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import Any, NewType

import plopp as pp
import scipp as sc

from beamlime.logging import BeamlimeLogger

from ._workflow import Events, FirstPulseTime, Histogrammed, WorkflowPipeline
from .base import BeamlimeMessage, HandlerInterface


@dataclass
class HistogramUpdated(BeamlimeMessage):
    content: pp.graphics.basefig.BaseFig


@dataclass
class RawDataSent(BeamlimeMessage):
    content: Events


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data and update the histogram.

    It receives a list of events, and reduces them into a histogram.
    It also triggers the update of a plot stream node.
    """

    pipeline: WorkflowPipeline

    def __init__(self) -> None:
        self.output_da: Histogrammed
        self.stream_node: pp.Node
        super().__init__()

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {Events.__name__}"

    def process_first_input(self, da: Events) -> None:
        first_pulse_time = da[0].coords['event_time_zero'][0]
        self.pipeline[FirstPulseTime] = first_pulse_time

    def process_first_output(self, data: Histogrammed) -> None:
        self.output_da = Histogrammed(sc.zeros_like(data))
        self.info("First data as a seed of histogram: %s", self.output_da)
        self.stream_node = pp.Node(self.output_da)
        self.figure = pp.figure1d(
            self.stream_node,
            title="Wavelength Histogram",
            grid=True,
        )

    async def process_data(self, data: Events) -> Histogrammed:
        self.info("Received, %s", self.format_received(data))
        self.pipeline[Events] = data
        return self.pipeline.compute(Histogrammed)

    async def process_message(
        self, message: RawDataSent | BeamlimeMessage
    ) -> BeamlimeMessage:
        if not isinstance(message, RawDataSent):
            raise TypeError(f"Message type should be {RawDataSent.__name__}.")

        if not hasattr(self, "output_da"):
            self.process_first_input(message.content)
            output = await self.process_data(message.content)
            self.process_first_output(output)
        else:
            output = await self.process_data(message.content)

        self.output_da.values += output.values
        self.stream_node.notify_children("update")
        return HistogramUpdated(
            sender=DataReductionHandler,
            receiver=Any,
            content=self.figure,
        )


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}.png"))


class PlotSaver(HandlerInterface):
    """Plot handler to save the updated histogram into an image file."""

    def __init__(self, logger: BeamlimeLogger, image_path: ImagePath) -> None:
        self.logger = logger
        self.image_path = image_path.absolute()
        self.create_dummy_image()
        super().__init__()

    def create_dummy_image(self) -> None:
        import matplotlib.pyplot as plt

        plt.plot([])
        plt.savefig(self.image_path)
        self.info(f"PlotHandler will save updated image into: {self.image_path}")

    async def save_histogram(self, message: HistogramUpdated | BeamlimeMessage) -> None:
        if not isinstance(message, HistogramUpdated):
            raise TypeError(f"Message type should be {HistogramUpdated.__name__}.")

        self.info("Received histogram, saving into %s...", self.image_path)
        message.content.save(self.image_path)
