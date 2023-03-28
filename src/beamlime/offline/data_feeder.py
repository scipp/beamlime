import asyncio

import numpy as np
from colorama import Fore, Style
from PIL import Image
from PIL.ImageOps import flip

from ..core.application import BeamlimeApplicationInterface
from ..resources.images import load_icon_img
from ..resources.images.generators import fake_2d_detector_img_generator


class Fake2dDetectorImageFeeder(BeamlimeApplicationInterface):
    detector_size = None
    num_frame = None

    def __init__(
        self, config: dict, verbose: bool = False, verbose_option: str = Fore.BLUE
    ) -> None:
        super().__init__(config, verbose, verbose_option)

    def parse_config(self, config: dict) -> None:
        self.detector_size = tuple(config.get("detector-size", (64, 64)))
        self.num_frame = int(config.get("num-frame", 128))
        self.noise_range = float(config.get("noise-range", 0.5))

    @staticmethod
    async def _run(self) -> None:
        original_img = Image.fromarray(np.uint8(load_icon_img()))
        resized_img = original_img.resize(self.detector_size)
        seed_img = np.asarray(flip(resized_img), dtype=np.float64)

        await asyncio.sleep(0.1)
        for iframe, frame in enumerate(
            fake_2d_detector_img_generator(
                seed_img, num_frame=self.num_frame, noise_range=self.noise_range
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
            await self.send_data(fake_data)
            if self.verbose:
                print(self.verbose_option, f"Sending {iframe}th frame", Style.RESET_ALL)
