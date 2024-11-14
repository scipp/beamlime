# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# zmq_server.py
import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass

import msgpack
import numpy as np
import zmq
import zmq.asyncio


@dataclass
class ZMQServerConfig:
    bind_address: str = "tcp://*:5555"
    array_size: tuple[int, int] = (640, 480)  # Example image size
    send_interval_ms: int = 100  # 10Hz


class ZMQServer:
    def __init__(self, config: ZMQServerConfig | None = None) -> None:
        self.config = config or ZMQServerConfig()
        self.context: zmq.asyncio.Context | None = None
        self.socket: zmq.Socket | None = None
        self.running = False
        self.send_task: asyncio.Task | None = None
        self.logger = logging.getLogger(__name__)

    async def start(self) -> None:
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(self.config.bind_address)
        self.running = True
        self.send_task = asyncio.create_task(self._send_loop())
        self.logger.info("Server started on %s", self.config.bind_address)

    async def _send_loop(self) -> None:
        while self.running:
            # Generate random array
            array = np.random.rand(*self.config.array_size).astype(np.float32)
            self.config.array_size = (
                self.config.array_size[0],
                self.config.array_size[1] + 1,
            )

            # Pack data with metadata
            data = {
                "timestamp": float(asyncio.get_event_loop().time()),
                "shape": array.shape,
                "dtype": str(array.dtype),
                "data": array.tobytes(),
            }
            packed = msgpack.packb(data)

            # Send
            if self.socket:
                await self.socket.send(packed)
                self.logger.debug("Sent array of shape %s", array.shape)

            await asyncio.sleep(self.config.send_interval_ms / 1000)

    async def stop(self) -> None:
        self.running = False
        if self.send_task:
            self.send_task.cancel()
            try:
                await self.send_task
            except asyncio.CancelledError:
                pass

        if self.socket:
            self.socket.close()
        if self.context:
            await self.context.term()
        self.logger.info("Server stopped")

    @asynccontextmanager
    async def session(self):
        try:
            await self.start()
            yield self
        finally:
            await self.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main():
        async with ZMQServer().session() as _:
            try:
                # Run until interrupted
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                pass

    asyncio.run(main())
