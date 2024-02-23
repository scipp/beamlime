# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import Any, List, NewType

import plopp as pp
import scipp as sc

from beamlime.logging import BeamlimeLogger

from ._workflow import Events, FirstPulseTime, Histogrammed, WorkflowPipeline
from .base import BaseHandler, BeamlimeMessage, MessageRouter


@dataclass
class FirstMessageSent(BeamlimeMessage):
    ...


@dataclass
class HistogramUpdated(BeamlimeMessage):
    content: pp.graphics.basefig.BaseFig


@dataclass
class RawDataSent(BeamlimeMessage):
    content: List[Events]


class StopWatch(BaseHandler):
    class Start(BeamlimeMessage):
        ...

    class Stop(BeamlimeMessage):
        ...

    def __init__(self, messenger: MessageRouter) -> None:
        super().__init__(messenger)
        self._start: float
        self._stop: float

    @property
    def duration(self) -> float:
        return self._stop - self._start

    def register_handlers(self) -> None:
        self.messenger.register_handler(StopWatch.Stop, self.stop)
        self.messenger.register_handler(StopWatch.Start, self.start)

    def start(self, _: BeamlimeMessage) -> None:
        import time

        self._start = time.time()
        self.debug("StopWatch logging started.")

    def stop(self, _: BeamlimeMessage) -> None:
        import time

        self._stop = time.time()
        self.debug("StopWatch logging finished. Total duration: %s", self.duration)


class DataReductionHandler(BaseHandler):
    input_pipe: List[Events]
    pipeline: WorkflowPipeline

    def __init__(self, messenger: MessageRouter) -> None:
        self.output_da: Histogrammed
        self.stream_node: pp.Node
        super().__init__(messenger)

    def register_handlers(self) -> None:
        self.messenger.register_awaitable_handler(RawDataSent, self.process_message)

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {Events.__name__}"

    def process_first_input(self, da: Events) -> None:
        first_pulse_time = da[0].coords['event_time_zero'][0]
        self.pipeline[FirstPulseTime] = first_pulse_time

    def process_first_output(self, data: Histogrammed) -> None:
        self.output_da = Histogrammed(sc.zeros_like(data))
        self.info("First data as a seed of histogram: %s", self.output_da)
        self.stream_node = pp.Node(self.output_da)
        self.figure = pp.figure1d(self.stream_node)

    def process_data(self, data: Events) -> Histogrammed:
        self.info("Received, %s", self.format_received(data))
        self.pipeline[Events] = data
        return self.pipeline.compute(Histogrammed)

    async def process_message(self, message: RawDataSent | BeamlimeMessage) -> None:
        if not isinstance(message, RawDataSent):
            raise TypeError(f"Message type should be {RawDataSent.__name__}.")

        data = message.content.pop(0)
        if not hasattr(self, "output_da"):
            self.process_first_input(data)
            output = self.process_data(data)
            self.process_first_output(output)
        else:
            output = self.process_data(data)

        self.output_da.values += output.values
        self.stream_node.notify_children("update")
        await self.messenger.send_message_async(
            HistogramUpdated(
                sender=DataReductionHandler,
                receiver=Any,
                content=self.figure,
            )
        )

    def __del__(self) -> None:
        from matplotlib import pyplot as plt

        plt.close()


ImagePath = NewType("ImagePath", pathlib.Path)


def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}.png"))


class PlotHandler(BaseHandler):
    def __init__(
        self, logger: BeamlimeLogger, messenger: MessageRouter, image_path: ImagePath
    ) -> None:
        self.logger = logger
        self.image_path = image_path.absolute()
        super().__init__(messenger)

    def register_handlers(self) -> None:
        self.create_dummy_image()
        self.messenger.register_awaitable_handler(HistogramUpdated, self.save_histogram)
        self.info(f"PlotHandler will save updated image into: {self.image_path}")
        self.messenger.register_awaitable_handler(
            self.messenger.StopRouting, self.inform_last_plot
        )

    def create_dummy_image(self) -> None:
        import matplotlib.pyplot as plt

        plt.plot([])
        plt.savefig(self.image_path)

    async def inform_last_plot(self, message: BeamlimeMessage) -> None:
        if not isinstance(message, BeamlimeMessage):
            raise TypeError(f"Message type should be {self.messenger.StopRouting}.")

        self.info(f"Last plot saved into {self.image_path}.")

    async def save_histogram(self, message: HistogramUpdated | BeamlimeMessage) -> None:
        if not isinstance(message, HistogramUpdated):
            raise TypeError(f"Message type should be {HistogramUpdated.__name__}.")

        self.info("Received histogram saved to %s.", self.image_path)
        message.content.save(self.image_path)
