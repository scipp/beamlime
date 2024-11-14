# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import zmq
import zmq.asyncio
import asyncio
from dataclasses import dataclass
from typing import Final
import logging
from contextlib import asynccontextmanager


@dataclass
class ZMQConfig:
    server_address: str = "tcp://localhost:5555"
    timeout_ms: int = 1000
    reconnect_delay_ms: int = 500
    max_retries: int = 3


class ZMQConnectionError(Exception):
    """Raised when connection issues occur"""

    pass


class ZMQClient:
    DEFAULT_BUFFER_SIZE: Final[int] = 1024 * 1024  # 1MB

    def __init__(self, config: ZMQConfig = ZMQConfig()) -> None:
        self.config = config
        self.context: zmq.asyncio.Context | None = None
        self.socket: zmq.Socket | None = None
        self.logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Establish async connection to ZMQ server"""
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.setsockopt(zmq.RCVTIMEO, self.config.timeout_ms)
        self.socket.connect(self.config.server_address)
        self.logger.info(f"Connected to {self.config.server_address}")

    async def receive_data(self) -> bytes | None:
        """Receive binary data with timeout and retry handling"""
        retries = 0
        while retries < self.config.max_retries:
            try:
                if not self.socket:
                    raise ZMQConnectionError("Socket not connected")
                return await self.socket.recv()
            except zmq.Again:
                self.logger.debug("Receive timeout")
                return None
            except zmq.ZMQError as e:
                retries += 1
                self.logger.error(
                    f"ZMQ error: {e}, attempt {retries}/{self.config.max_retries}"
                )
                await self._reconnect()
        raise ZMQConnectionError(f"Failed after {self.config.max_retries} attempts")

    async def _reconnect(self) -> None:
        """Handle reconnection after failure"""
        await self.close()
        await asyncio.sleep(self.config.reconnect_delay_ms / 1000)
        await self.connect()

    async def close(self) -> None:
        """Clean up resources"""
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.context:
            await self.context.term()
            self.context = None

    @asynccontextmanager
    async def session(self):
        """Async context manager"""
        try:
            await self.connect()
            yield self
        finally:
            await self.close()


# Usage example:
if __name__ == "__main__":

    async def main():
        logging.basicConfig(level=logging.INFO)
        config = ZMQConfig(reconnect_delay_ms=1000)

        async with ZMQClient(config).session() as client:
            while True:
                data = await client.receive_data()
                if data:
                    print(f"Received {len(data)} bytes")

    asyncio.run(main())
