# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import Any, Dict, NewType

import plopp as pp
import scipp as sc

from beamlime.logging import BeamlimeLogger

from ._workflow import Events, FirstPulseTime, Histogrammed, WorkflowPipeline
from .base import HandlerInterface, MessageProtocol


@dataclass
class UpdateHistogram(MessageProtocol):
    content: Histogrammed
    sender: type
    receiver: type


@dataclass
class RawDataSent(MessageProtocol):
    content: Events
    sender: type
    receiver: type


class DataReductionHandler(HandlerInterface):
    """Data reduction handler to process the raw data and update the histogram.

    It receives a list of events, and reduces them into a histogram.
    It also triggers the update of a plot stream node.
    """

    def __init__(self, pipeline: WorkflowPipeline) -> None:
        self.pipeline = pipeline
        self.first_pulse_time: sc.Variable
        super().__init__()

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {Events.__name__}"

    def process_first_input(self, da: Events) -> None:
        self.first_pulse_time = da[0].coords['event_time_zero'][0]
        self.pipeline[FirstPulseTime] = self.first_pulse_time

    def process_data(self, data: Events) -> Histogrammed:
        self.info("Received, %s", self.format_received(data))
        self.pipeline[Events] = data
        return self.pipeline.compute(Histogrammed)

    def process_message(self, message: MessageProtocol) -> MessageProtocol:
        try:
            self.first_pulse_time
        except AttributeError:
            self.process_first_input(message.content)

        return UpdateHistogram(
            sender=DataReductionHandler,
            receiver=Any,
            content=self.process_data(message.content),
        )


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}.png"))


class PlotStreamer(HandlerInterface):
    def __init__(self, logger: BeamlimeLogger) -> None:
        self.logger = logger
        self.figure = pp.figure1d(
            # If you know the layout, you can just use ``pp.plot`` directly.
            title="Wavelength Histogram",
            grid=True,
        )
        self.binning_coords: Dict[str, sc.Variable]
        self.output_da: Histogrammed
        super().__init__()

    def process_first_histogram(self, data: Histogrammed) -> None:
        self.output_da = Histogrammed(sc.zeros_like(data))
        self.binning_coords = {"wavelength": data.coords["wavelength"]}
        self.info("First data as a seed of histogram: %s", self.output_da)
        self.figure.update(self.output_da, key='a')

    def update_histogram(self, message: MessageProtocol) -> None:
        try:
            self.binning_coords
        except AttributeError:
            self.process_first_histogram(message.content)

        self.output_da += sc.rebin(message.content, self.binning_coords)
        self.figure.update(self.output_da, key='a')


class PlotSaver(PlotStreamer):
    """Plot handler to save the updated histogram into an image file."""

    def __init__(self, logger: BeamlimeLogger, image_path: ImagePath) -> None:
        super().__init__(logger)
        self.image_path = image_path.absolute()
        self.create_dummy_image()

    def create_dummy_image(self) -> None:
        import matplotlib.pyplot as plt

        plt.plot([])
        plt.savefig(self.image_path)
        self.info(f"PlotHandler will save updated image into: {self.image_path}")

    def save_histogram(self, message: MessageProtocol) -> None:
        super().update_histogram(message)
        self.info("Received histogram, saving into %s...", self.image_path)
        self.figure.save(self.image_path)
