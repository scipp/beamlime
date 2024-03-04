# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import Any

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from .base import DaemonInterface, MessageRouter
from .handlers import RawDataSent, Events


@dataclass
class BeamlimeMessage:
    """A message object that can be exchanged between daemons or handlers."""

    content: Any
    sender: type = Any
    receiver: type = Any


class DataStreamSimulator(DaemonInterface):
    """Data that simulates the data streaming from the random generator."""

    random_events: RandomEvents
    chunk_size: ChunkSize
    messenger: MessageRouter
    data_feeding_speed: DataFeedingSpeed

    def slice_chunk(self) -> Events:
        chunk, self.random_events = (
            Events(self.random_events[: self.chunk_size]),
            RandomEvents(self.random_events[self.chunk_size :]),
        )
        return chunk

    async def run(self) -> None:
        import asyncio

        self.info("Data streaming started...")

        num_chunks = len(self.random_events) // self.chunk_size

        for i_chunk in range(num_chunks):
            chunk = self.slice_chunk()
            self.info("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            await self.messenger.send_message_async(
                RawDataSent(
                    sender=DataStreamSimulator,
                    receiver=Any,
                    content=chunk,
                )
            )
            await asyncio.sleep(self.data_feeding_speed)

        await self.messenger.send_message_async(
            self.messenger.StopRouting(
                content=None,
                sender=DataStreamSimulator,
                receiver=self.messenger.__class__,
            )
        )

        self.info("Data streaming finished...")
