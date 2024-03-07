# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import os
from dataclasses import dataclass
from typing import Any, Union

from scippneutron.io.nexus.load_nexus import JSONGroup, json_nexus_group

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from .base import DaemonInterface, MessageProtocol, MessageRouter
from .handlers import Events, RawDataSent

Path = Union[str, bytes, os.PathLike]


@dataclass
class BeamlimeMessage:
    """A message object that can be exchanged between daemons or handlers."""

    content: Any
    sender: type = Any
    receiver: type = Any


@dataclass
class RunStart(MessageProtocol):
    content: JSONGroup
    sender: type
    receiver: type


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

    async def run(self) -> None:
        self.info("Fake data streaming started...")

        await self.messenger.send_message_async(
            RunStart(
                content=self._group,
                sender=self.__class__,
                receiver=self.messenger.__class__,
            )
        )
        await self.messenger.send_message_async(
            self.messenger.StopRouting(
                content=None,
                sender=self.__class__,
                receiver=self.messenger.__class__,
            )
        )
        self.info("Fake data streaming finished...")
