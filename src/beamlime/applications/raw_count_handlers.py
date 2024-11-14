# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import NewType, cast

import msgpack
import numpy as np
import plopp as pp
import requests
import scipp as sc
import scippnexus as snx
import zmq
from ess.reduce.live import raw
from streaming_data_types.eventdata_ev44 import EventData

from beamlime.config.raw_detectors import (
    dream_detectors_config,
    loki_detectors_config,
    nmx_detectors_config,
)
from beamlime.core.serialization import serialize_data_array
from beamlime.logging import BeamlimeLogger
from beamlime.plotting.plot_matplotlib import MatplotlibPlotter

from ..workflow_protocols import WorkflowResult
from .base import HandlerInterface
from .daemons import DataPieceReceived, NexusFilePath

detector_registry = {
    'DREAM': dream_detectors_config,
    'LoKI': loki_detectors_config,
    'NMX': nmx_detectors_config,
}


@dataclass
class WorkflowResultUpdate:
    content: WorkflowResult


class RawCountHandler(HandlerInterface):
    """
    Continuously handle raw counts for every ev44 message.

    This ignores run-start and run-stop messages.
    """

    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        nexus_file: NexusFilePath,
        update_every: float,
        window_length: float,
    ) -> None:
        """
        Parameters
        ----------
        logger:
            Logger instance.
        nexus_file:
            Path to the nexus file that has static information such as detector numbers
            and pixel positions. This is not necessarily the same as the current run,
            provided that there is no difference in relevant detector information.
        """
        self.logger = logger
        self._detectors: dict[str, list[str]] = {}
        self._views: dict[str, raw.RollingDetectorView] = {}
        self._update_every = sc.scalar(update_every, unit='s').to(
            unit='ns', dtype='int64'
        )
        self._window_length = round(window_length / update_every)
        self._next_update: int | None = None

        with snx.File(nexus_file) as f:
            entry = next(iter(f[snx.NXentry].values()))
            instrument = next(iter(entry[snx.NXinstrument].values()))
            self._instrument = str(instrument['name'][()])

        for name, detector in detector_registry[self._instrument]['detectors'].items():
            self._detectors.setdefault(detector['detector_name'], []).append(name)
            self._views[name] = raw.RollingDetectorView.from_nexus(
                nexus_file,
                detector_name=detector['detector_name'],
                window=self._window_length,
                projection=detector['projection'],
                resolution=detector.get('resolution'),
                pixel_noise=detector.get('pixel_noise'),
            )
        self._chunk = {name: 0 for name in self._views}
        self._buffer = {name: [] for name in self._views}
        self.info("Initialized with %s", list(self._views))

    def set_time_range(self, start: float, end: float) -> None:
        pass

    def handle(self, message: DataPieceReceived) -> WorkflowResultUpdate | None:
        data_piece = cast(EventData, message.content.deserialized)
        detname = data_piece.source_name.split('/')[-1]
        event_id = data_piece.pixel_id
        if (det := self._detectors.get(detname)) is not None:
            for name in det:
                buffer = self._buffer[name]
                buffer.append(event_id)
        else:
            self.info("Ignoring data for %s", detname)
        reference_time = sc.datetime(int(data_piece.reference_time), unit='ns')
        if self._next_update is None:
            self._next_update = reference_time.copy()
        if reference_time >= self._next_update:
            self.info("Updating views at %s", reference_time)
            while reference_time >= self._next_update:
                # If there were no pulses for a while we need to skip several updates.
                # Note that we do not simply set _next_update based on reference_time
                # to avoid drifts.
                self._next_update += self._update_every
            results = {}
            for name, det in self._views.items():
                buffer = self._buffer[name]
                if len(buffer):
                    det.add_counts(np.concatenate(buffer))
                    buffer.clear()
                # The detector view supports multiple windows, but we do not have
                # configured any instrument that uses this currently, so this is a
                # length-1 tuple.
                for window in (self._window_length,):
                    length = (window * self._update_every).to(dtype='float64', unit='s')
                    key = (self._instrument, name, f'window={round(length.value, 1)} s')
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
        group.add_argument(
            "--update-every",
            help="Update the raw-detector view every UPDATE_EVERY seconds. "
            "Can be a float.",
            type=float,
            default=1.0,
        )
        group.add_argument(
            "--window-length",
            help="Length of the window in seconds. Must be larger or equal to "
            "update-every. Can be a float.",
            type=float,
            default=10.0,
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "RawCountHandler":
        return cls(
            logger=logger,
            nexus_file=args.static_file_path,
            update_every=args.update_every,
            window_length=args.window_length,
        )


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
        self._plotter = MatplotlibPlotter()

    def update_data(self, message: WorkflowResultUpdate) -> None:
        start = time.time()

        image_file_name = f"{self.image_path_prefix}.png"
        self.info("Received histogram(s), saving into %s...", image_file_name)

        self._plotter.update_data(message.content)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            # Saving into a tempfile avoids flickering when the image is updated, if
            # the image is displayed in a GUI.
            self._plotter.fig.savefig(tmpfile.name, dpi=150)
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
            image_path_prefix=args.image_path_prefix
            or ImagePath(pathlib.Path(f"beamlime_plot_{time.strftime('%Y-%m-%d')}")),
        )


class PlotPosterRequest(HandlerInterface):
    def __init__(self, *, logger: BeamlimeLogger, port: int = 5556) -> None:
        super().__init__()
        self.logger = logger
        self.port = port
        # self._figure = None
        # self._source = ColumnDataSource(data=dict(image=[np.zeros((10, 10))]))
        # color_mapper = LinearColorMapper(palette="Viridis256", low=0.0, high=100.0)
        # self.widgets = []
        # plot = figure(title="Dynamic 2D Heatmap", x_range=(0, 10), y_range=(0, 10))
        # plot.image(
        #    image='image',
        #    x=0,
        #    y=0,
        #    dw=10,
        #    dh=10,
        #    color_mapper=color_mapper,
        #    source=self._source,
        # )
        # scale_slider = Slider(title="Scale", value=2.0, start=0.1, end=5.0, step=0.1)
        # self.widgets.append(scale_slider)
        # self.widgets.append(plot)

    def refresh_data(self, message: WorkflowResultUpdate) -> None:
        scale = requests.get(f"http://localhost:{self.port}/get_scale")
        scale = float(scale.text)
        # dummy data for testing, handle full message once this works
        res = int(scale * 100)
        dummy = np.random.rand(res, res) * 10
        url = f"http://localhost:{self.port}/update_data"
        # response = requests.post(url, json=dummy.tolist())
        headers = {'Content-Type': 'application/x-msgpack'}
        response = requests.post(
            url, data=msgpack.packb({'Z': dummy.tolist()}), headers=headers
        )
        if response.status_code != 200:
            self.logger.error("Failed to post data to server: %s", response.text)

    def update(self):
        pass
        # self._source.data = dict(image=np.random.rand(10, 10) * 100)

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "PlotPoster":
        return cls(logger=logger)


class PlotPoster(HandlerInterface):
    def __init__(self, *, logger: BeamlimeLogger, port: int = 5555) -> None:
        super().__init__()
        self.logger = logger
        self.port = port
        self.context: zmq.Context | None = None
        self.socket: zmq.Socket | None = None

    async def start(self) -> None:
        """Initialize ZMQ socket"""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{self.port}")
        self.logger.info("ZMQ publisher started on port %d", self.port)

    def update_data(self, message: WorkflowResultUpdate) -> None:
        """Send array data over ZMQ"""
        if not self.socket:
            self.logger.error("No socket available for sending data")
            return

        data = {
            '||'.join(k): serialize_data_array(v) for k, v in message.content.items()
        }
        try:
            packed = msgpack.packb(data)
            self.socket.send(packed)
            self.logger.warning("Sending update")
        except Exception as e:
            self.logger.error("Failed to send data: %s", e)

    async def stop(self) -> None:
        """Clean shutdown of ZMQ socket"""
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.context:
            self.context.term()
            self.context = None

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "PlotPoster":
        return cls(logger=logger)
