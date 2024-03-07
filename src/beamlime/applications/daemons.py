# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import os
from dataclasses import dataclass
<<<<<<< HEAD
from typing import Any, Union, AsyncGenerator, Generator

from scippneutron.io.nexus.load_nexus import JSONGroup, json_nexus_group
=======
from typing import AsyncGenerator, Generator
>>>>>>> b56b951 (Remove unused fields in the protocol.)

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from .base import DaemonInterface, MessageProtocol, MessageRouter, Application
from .handlers import Events

Path = Union[str, bytes, os.PathLike]


@dataclass
class RunStart:
    content: JSONGroup


@dataclass
class RawDataSent:
    content: Events


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

        self.info("Data streaming started...")
        for i_chunk, chunk in enumerate(self.slice_chunk()):
            self.info("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            yield RawDataSent(content=chunk)
            await asyncio.sleep(self.data_feeding_speed)

        self.info("Data streaming finished...")


class FakeListener(DaemonInterface):
    messenger: MessageRouter

    def __init__(self, nexus_structure: dict):
        self._group = json_nexus_group(nexus_structure)

    @classmethod
    def from_file(cls, path: Path):
        '''Read nexus structure from json file'''
        with open(path) as f:
            nexus_structure = json.load(f)
        return cls(nexus_structure)

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")

        yield RunStart(content=self._group)
        yield Application.Stop(content=None)
        self.info("Fake data streaming finished...")
