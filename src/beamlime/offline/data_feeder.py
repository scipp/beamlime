import asyncio

import numpy as np
from colorama import Fore, Style
from PIL import Image
from PIL.ImageOps import flip

from ..core.application import BeamlimeApplicationInterface
from ..resources.images import load_icon_img
from ..resources.images.generators import fake_2d_detector_img_generator


class Fake2dDetectorImageFeeder(BeamlimeApplicationInterface):
    def __init__(
        self, config: dict, verbose: bool = False, verbose_option: str = Fore.BLUE
    ) -> None:
        super().__init__(config, verbose, verbose_option)

    def parse_config(self, config: dict) -> None:
        self.detector_size = tuple(config.get("detector-size", (64, 64)))
        self.num_frame = int(config.get("num-frame", 128))
        self.min_intensity = float(config.get("min-intensity", 0.5))
        self.signal_mu = float(config.get("signal-mu", 0.1))
        self.signal_err = float(config.get("signal-err", 0.1))
        self.noise_mu = float(config.get("noise-mu", 1))
        self.noise_err = float(config.get("noise-err", 0.3))

    @staticmethod
    async def _run(self) -> None:
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
            fake_data = {
                "sample_id": "typical-lime-intaglio-0",
                "detector-data": {
                    "detector-id": "unknown-2d-detector",
                    "image": frame.tolist(),
                    "frame-number": iframe,
                },
            }
            send_result = await self.send_data(data=fake_data)
            if not send_result:
                break
            if self.verbose:
                print(self.verbose_option, f"Sending {iframe}th frame", Style.RESET_ALL)
