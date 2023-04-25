# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import numpy as np
from PIL import Image
from PIL.ImageOps import flip

from ..resources.images import load_icon_img
from ..resources.images.generators import fake_2d_detector_img_generator
from .interfaces import BeamlimeApplicationInterface


class Fake2dDetectorImageFeeder(BeamlimeApplicationInterface):
    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        super().__init__(config, logger, **kwargs)
        self._update_rate = 0.1

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

    async def _run(self) -> None:
        self.info("Start data feeding...")
        original_img = Image.fromarray(np.uint8(load_icon_img()))
        resized_img = original_img.resize(self.detector_size)
        seed_img = np.asarray(flip(resized_img), dtype=np.float64)
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
            continue_flag = await self.should_proceed()
            if not continue_flag:
                return

            fake_data = {
                "sample_id": "typical-lime-intaglio-0",
                "detector-data": {
                    "detector-id": "unknown-2d-detector",
                    "image": frame.tolist(),
                    "frame-number": iframe,
                },
            }
            self.info("Sending %dth frame", iframe)

            if not await self.send_data(data=fake_data):
                self.info(
                    "Output channel broken. %dth frame was not sent. "
                    "Stopping the application ... ",
                    iframe,
                )
                return
        self.info("Finishing data feeding...")
