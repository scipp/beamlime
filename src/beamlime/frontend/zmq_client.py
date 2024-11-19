# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, auto

import zmq
import zmq.asyncio


class ConnectionState(Enum):
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()


@dataclass
class ZMQConfig:
    server_address: str = "tcp://localhost:5555"
    timeout_ms: int = 1000
    reconnect_interval_ms: int = 1000
    heartbeat_interval_ms: int = 2000  # New setting


class ZMQClient:
    def __init__(self, config: ZMQConfig | None = None) -> None:
        self.config = config or ZMQConfig()
        self.context: zmq.asyncio.Context | None = None
        self.socket: zmq.Socket | None = None
        self.state = ConnectionState.DISCONNECTED
        self.reconnect_task: asyncio.Task | None = None
        self.heartbeat_task: asyncio.Task | None = None
        self.logger = logging.getLogger(__name__)
        self.last_received = 0.0

    async def start(self) -> None:
        """Start client with automatic reconnection"""
        self.context = zmq.asyncio.Context()
        self.reconnect_task = asyncio.create_task(self._reconnection_loop())
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _connect(self) -> None:
        """Attempt to establish connection"""
        if self.state == ConnectionState.CONNECTING:
            return

        self.state = ConnectionState.CONNECTING
        try:
            if self.socket:
                self.socket.close()

            self.socket = self.context.socket(zmq.SUB)
            self.socket.setsockopt(zmq.CONFLATE, 1)
            self.socket.setsockopt(zmq.RCVHWM, 1)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
            self.socket.setsockopt(zmq.RCVTIMEO, self.config.timeout_ms)
            self.socket.connect(self.config.server_address)

            # Try receive to verify connection
            try:
                await asyncio.wait_for(
                    self.socket.recv(), timeout=self.config.timeout_ms / 1000
                )
                self.state = ConnectionState.CONNECTED
                self.last_received = asyncio.get_event_loop().time()
                self.logger.info("Connected and receiving data")
            except (asyncio.TimeoutError, zmq.Again):
                self.state = ConnectionState.DISCONNECTED
                self.logger.warning("Connected but no data received")

        except zmq.ZMQError as e:
            self.state = ConnectionState.DISCONNECTED
            self.logger.error("Connection failed: %s", e)

    async def _reconnection_loop(self) -> None:
        """Background task handling reconnection"""
        while True:
            if self.state != ConnectionState.CONNECTED:
                await self._connect()
            await asyncio.sleep(self.config.reconnect_interval_ms / 1000)

    async def _heartbeat_loop(self) -> None:
        """Monitor connection health"""
        while True:
            now = asyncio.get_event_loop().time()
            if (
                self.state == ConnectionState.CONNECTED
                and now - self.last_received > self.config.heartbeat_interval_ms / 1000
            ):
                self.logger.warning("Connection timeout - no recent data")
                self.state = ConnectionState.DISCONNECTED
            await asyncio.sleep(self.config.heartbeat_interval_ms / 1000)

    async def receive_latest(self) -> bytes | None:
        """Receive latest message if connected"""
        if self.state != ConnectionState.CONNECTED:
            return None

        try:
            if not self.socket:
                return None
            data = await self.socket.recv()
            self.last_received = asyncio.get_event_loop().time()
            return data
        except (zmq.Again, asyncio.CancelledError):
            return None
        except zmq.ZMQError:
            self.logger.warning("ZMQ error during receive")
            self.state = ConnectionState.DISCONNECTED
            return None

    async def stop(self) -> None:
        """Stop client and cleanup safely"""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

        if self.reconnect_task:
            self.reconnect_task.cancel()
            try:
                await self.reconnect_task
            except asyncio.CancelledError:
                pass

        if self.socket:
            self.socket.close()
            self.socket = None

        if self.context:
            try:
                # Store context reference before nulling
                context = self.context
                self.context = None
                await context.term()
            except AttributeError:
                pass

        self.state = ConnectionState.DISCONNECTED

    @asynccontextmanager
    async def session(self):
        try:
            await self.start()
            yield self
        finally:
            await self.stop()


# Usage example:
if __name__ == "__main__":

    async def main():
        logging.basicConfig(level=logging.INFO)

        async with ZMQClient().session() as client:
            while True:
                data = await client.receive_latest()
                if data:
                    print(f"Received {len(data)} bytes")  # noqa: T201
                await asyncio.sleep(0.1)

    asyncio.run(main())
