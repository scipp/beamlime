# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from logging import Logger

import numpy as np

from ..applications.interfaces import BeamlimeApplicationInterface
from ..communication.broker import CommunicationBroker


class FakeEventDataFeeder(BeamlimeApplicationInterface):
    def __init__(
        self,
        /,
        name: str,
        broker: CommunicationBroker = None,
        logger: Logger = None,
        timeout: float = 1,
        wait_interval: float = 0.1,
        chunk_size: int = 16,
    ) -> None:
        super().__init__(
            name=name,
            broker=broker,
            logger=logger,
            timeout=timeout,
            wait_interval=wait_interval,
        )
        self.chunk_size = chunk_size

    def __del__(self):
        ...

    async def check_run_start(self, timeout: int = 0) -> bool:
        from streaming_data_types.run_start_pl72 import deserialise_pl72

        self.info("Pull a run-start message...")
        run_start_msg_buf_list = await self.consume(
            channel="kafka-log-consumer", timeout=timeout
        )
        if not run_start_msg_buf_list:
            self.info(run_start_msg_buf_list)
            return False
        else:
            run_start_msg_buf = run_start_msg_buf_list[-1]
            self.debug("Received message: %s", run_start_msg_buf)
            run_start_msg = deserialise_pl72(run_start_msg_buf)
            self.info(
                "Run start message received: "
                "\n\t\t\t Run name: %s"
                "\n\t\t\t Instrument name: %s",
                run_start_msg.run_name,
                run_start_msg.instrument_name,
            )
            return True

    async def _run(self) -> None:
        self.info("Start the fake event streaming...")
        run_start_msg = await self.check_run_start(timeout=10)
        # wait for a longer time for the first run-start message.

        while run_start_msg and await self.should_proceed(wait_on_true=True):
            new_chunks = await self.consume(
                channel="kafka-data-consumer", chunk_size=self.chunk_size
            )
            self.info(new_chunks)

            # if await self.check_run_start(timeout=0):
            #     self.info("New run start message received.")

        self.info("Finishing data feeding...")


class Fake2dDetectorImageFeeder(BeamlimeApplicationInterface):
    def __init__(
        self,
        /,
        name: str,
        broker: CommunicationBroker = None,
        logger: Logger = None,
        timeout: float = 1,
        wait_interval: float = 0.1,
        num_frame: int = 128,
        detector_size: tuple = (64, 64),
        min_intensity: float = 0.5,
        signal_mu: float = 0.1,
        signal_err: float = 0.1,
        noise_mu: float = 1,
        noise_err: float = 0.3,
    ) -> None:
        super().__init__(
            name=name,
            broker=broker,
            logger=logger,
            timeout=timeout,
            wait_interval=wait_interval,
        )
        from functools import partial

        from ..resources.images.generators import fake_2d_detector_img_generator

        self.num_frame = num_frame
        seed_img = self._prepare_seed_image(detector_size)
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

    def __del__(self):
        ...

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

            if not await self.put(data=fake_data):
                self.info(
                    "Output channel broken. %dth frame was not sent. "
                    "Stopping the application ... ",
                    iframe,
                )
                return
        self.info("Finishing data feeding...")
