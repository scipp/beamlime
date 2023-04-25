# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import numpy as np

from ..applications.interfaces import BeamlimeApplicationInterface


class Fake2dDetectorImageFeeder(BeamlimeApplicationInterface):
    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        super().__init__(config, logger, **kwargs)

    def parse_config(self, config: dict) -> None:
        from functools import partial

        from ..resources.images.generators import fake_2d_detector_img_generator

        self._timeout = float(config.get("timeout", 5)) or 5
        self._wait_int = float(config.get("update-rate", 0.1)) or 0.1
        self.num_frame = int(config.get("num-frame", 128))
        detector_size = tuple(config.get("detector-size", (64, 64)))
        seed_img = self._prepare_seed_image(detector_size)
        min_intensity = float(config.get("min-intensity", 0.5))
        signal_mu = float(config.get("signal-mu", 0.1))
        signal_err = float(config.get("signal-err", 0.1))
        noise_mu = float(config.get("noise-mu", 1))
        noise_err = float(config.get("noise-err", 0.3))
        self.data_generator = partial(
            fake_2d_detector_img_generator,
            seed_img=seed_img,
            num_frame=self.num_frame,
            min_intensity=min_intensity,
            signal_mu=signal_mu,
            signal_err=signal_err,
            noise_mu=noise_mu,
            noise_err=noise_err,
        )

    def _prepare_seed_image(self, detector_size: tuple) -> np.ndarray:
        from PIL import Image
        from PIL.ImageOps import flip

        from ..resources.images import load_icon_img

        original_img = Image.fromarray(np.uint8(load_icon_img()))
        resized_img = original_img.resize(detector_size)
        return np.asarray(flip(resized_img), dtype=np.float64)

    async def _run(self) -> None:
        self.info("Start data feeding...")

        for iframe, frame in enumerate(self.data_generator()):
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
