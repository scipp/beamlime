# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio

import numpy as np
from PIL import Image
from PIL.ImageOps import flip

from ..core.application import AsyncApplicationInterce
from ..resources.images import load_icon_img
from ..resources.images.generators import fake_2d_detector_img_generator


class Fake2dDetectorImageFeeder(AsyncApplicationInterce):
    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        super().__init__(config, logger, **kwargs)
        self._paused = True  # TODO: change other applications too...!
        self._paused_interval = 1
        self._paused_time = 0

    def start(self) -> None:
        self._paused = False
        self._paused_time = 0

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False
        self._paused_time = 0

    def __del__(self) -> None:
        ...

    def parse_config(self, config: dict) -> None:
        self.detector_size = tuple(config.get("detector-size", (64, 64)))
        self.num_frame = int(config.get("num-frame", 128))
        self.min_intensity = float(config.get("min-intensity", 0.5))
        self.signal_mu = float(config.get("signal-mu", 0.1))
        self.signal_err = float(config.get("signal-err", 0.1))
        self.noise_mu = float(config.get("noise-mu", 1))
        self.noise_err = float(config.get("noise-err", 0.3))
        self.timeout = float(config.get("timeout", 5)) or 5

    @staticmethod
    async def _run(self: AsyncApplicationInterce) -> None:
        self.info("Start data feeding...")
        original_img = Image.fromarray(np.uint8(load_icon_img()))
        resized_img = original_img.resize(self.detector_size)
        seed_img = np.asarray(flip(resized_img), dtype=np.float64)
        await asyncio.sleep(0.1)
        for iframe, frame in enumerate(
            fake_2d_detector_img_generator(
                seed_img,
                num_frame=self.num_frame,
                min_intensity=self.min_intensity,
                signal_mu=self.signal_mu,
                signal_err=self.signal_err,
                noise_mu=self.noise_mu,
                noise_err=self.noise_err,
            )
        ):
            await asyncio.sleep(0.1)
            while self._paused:
                if self.timeout < self._paused_time:
                    self.info("Timeout. Stopping the application ... ")
                    return
                self.info(
                    "Application paused, "
                    "waiting for start/resume call for %s seconds. ",
                    self._paused_time + 1,
                )
                await asyncio.sleep(self._paused_interval)
                self._paused_time += self._paused_interval

            fake_data = {
                "sample_id": "typical-lime-intaglio-0",
                "detector-data": {
                    "detector-id": "unknown-2d-detector",
                    "image": frame.tolist(),
                    "frame-number": iframe,
                },
            }
            self.info("Sending %dth frame", iframe)
            data_sent = await self.send_data(data=fake_data, timeout=10)
            if not data_sent:
                self.info(
                    "Output channel broken. %dth frame was not sent. "
                    "Stopping the application ... ",
                    iframe,
                )
                return
        self.info("Finishing data feeding...")
