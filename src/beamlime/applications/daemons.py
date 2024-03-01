# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Generator

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from ._workflow import Events
from .base import DaemonInterface, MessageProtocol


@dataclass
class RawDataSent(MessageProtocol):
    content: Events
    sender: type
    receiver: type


class DataStreamSimulator(DaemonInterface):
    """Data that simulates the data streaming from the random generator."""

    random_events: RandomEvents
    chunk_size: ChunkSize
    data_feeding_speed: DataFeedingSpeed

    def slice_chunk(self) -> Generator[Events, None, None]:
        while self.random_events:
            chunk, self.random_events = (
                Events(self.random_events[: self.chunk_size]),
                RandomEvents(self.random_events[self.chunk_size :]),
            )
            yield chunk

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        import asyncio

        from .base import Application

        self.info("Data streaming started...")
        for i_chunk, chunk in enumerate(self.slice_chunk()):
            self.info("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            yield RawDataSent(
                sender=DataStreamSimulator,
                receiver=Any,
                content=chunk,
            )
            await asyncio.sleep(self.data_feeding_speed)

        self.info("Data streaming finished...")
        yield Application.Stop(
            content=None,
            sender=DataStreamSimulator,
            receiver=Application,
        )
