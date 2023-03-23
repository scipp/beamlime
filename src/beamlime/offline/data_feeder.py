import asyncio
from queue import Queue

import numpy as np
from PIL import Image
from PIL.ImageOps import flip

from beamlime.resources import load_icon_img
from beamlime.resources.images.generators import fake_2d_detector_img_generator


class Fake2dDetectorImageFeeder:
    queue = None

    def __init__(self, detector_size: tuple = (64, 64), num_frame: int = 128) -> None:
        self.detector_size = detector_size
        self.num_frame = num_frame

    def set_queue(self, queue: Queue):
        self.queue = queue

    def get_queue(self) -> Queue:
        return self.queue

    @staticmethod
    async def send_data(queue: Queue, data: dict) -> None:
        queue.put(data)

    @staticmethod
    async def _run(queue: Queue, detector_size: tuple, num_frame: int) -> None:
        original_img = Image.fromarray(np.uint8(load_icon_img()))
        resized_img = original_img.resize(detector_size)
        seed_img = np.asarray(flip(resized_img), dtype=np.float64)

        for iframe, frame in enumerate(
            fake_2d_detector_img_generator(seed_img, num_frame=num_frame)
        ):
            await asyncio.sleep(0.3)
            fake_data = {
                "sample_id": "typical-lime-intaglio-0",
                "timestamp": iframe,
                "detector-data": {
                    "detector-id": "unknown-2d-detector",
                    "image": frame.tolist(),
                },
            }
            await Fake2dDetectorImageFeeder.send_data(queue, fake_data)
            print(
                f"\033[0;31m {iframe}th sent fake data"
                f" with sample id: {fake_data['sample_id']}"
            )

    def create_task(self):
        return asyncio.create_task(
            self._run(
                queue=self.queue,
                detector_size=self.detector_size,
                num_frame=self.num_frame,
            )
        )
