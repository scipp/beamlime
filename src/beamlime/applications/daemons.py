# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from types import FunctionType, GeneratorType
from typing import NewType

from ..communication.pipes import AsyncReadableBuffer, Pipe
from ..empty_factory import empty_app_factory
from .interfaces import BeamlimeApplicationInterface

RawData = NewType("RawData", object)
empty_app_factory.cache_product(Pipe[RawData], Pipe)

DataGenerator = NewType("DataGenerator", GeneratorType)


class DataStreamSimulator(BeamlimeApplicationInterface):
    raw_data_pipe: Pipe[RawData]
    data_generator: DataGenerator

    async def run(self) -> None:
        if await self.can_start(wait_on_true=True):
            for i_data, raw_data in enumerate(self.data_generator()):
                self.raw_data_pipe.write(raw_data)
                self.debug("Sending %sth data.", i_data)
                if not await self.running(wait_on_true=True):
                    break


empty_app_factory.register(DataStreamSimulator, DataStreamSimulator)

PostReductionData = NewType("PostReductionData", object)
empty_app_factory.cache_product(Pipe[PostReductionData], Pipe)

Workflow = NewType("Workflow", FunctionType)


@empty_app_factory.provider
class DataReduction(BeamlimeApplicationInterface):
    raw_data_pipe: Pipe[RawData]
    data_reduction_pipe: Pipe[PostReductionData]
    workflow: Workflow
    chunk_size: int = 4
    chunk_count: int = 0

    async def run(self) -> None:
        self.raw_data_pipe.chunk_size = self.chunk_size
        await self.can_start(wait_on_true=True)
        while await self.running(wait_on_true=True):
            raw_data: AsyncReadableBuffer
            async with self.raw_data_pipe.open_async_readable(timeout=3) as raw_data:
                chunk = []
                async for data in raw_data.readall():
                    chunk.append(data)
                processed = self.workflow(chunk)
                self.data_reduction_pipe.write(processed)
                self.chunk_count += 1
                self.debug("Processed %sth chunk. %s", self.chunk_count, processed)


@empty_app_factory.provider
class DataPlotter(BeamlimeApplicationInterface):
    post_dr_pipe: Pipe[PostReductionData]

    async def job(self) -> None:
        import scipp as sc

        raw_data: AsyncReadableBuffer
        async with self.post_dr_pipe.open_async_readable(timeout=3) as raw_data:
            _data: sc.Variable
            async for _data in raw_data.readall():
                self.first_data.values += _data.hist().values
            self.stream_node.notify_children("update")
            self.debug("Updated plot.")

    async def run(self) -> None:
        import plopp as pp

        await self.can_start(wait_on_true=True)
        if await self.running(wait_on_true=True):
            self.post_dr_pipe.chunk_size = 1
            async with self.post_dr_pipe.open_async_readable(timeout=5) as raw_data:
                self.first_data = (await raw_data.read()).hist()
                self.debug("First data as a seed of histogram: %s", self.first_data)
                self.stream_node = pp.Node(lambda: self.first_data)
                self.fig = pp.figure2d(self.stream_node)

        while await self.running(wait_on_true=True):
            async with self.post_dr_pipe.open_async_readable(timeout=3) as raw_data:
                async for _data in raw_data.readall():
                    self.first_data.values += _data.hist().values
                self.stream_node.notify_children("update")
                self.debug("Updated plot.")
