# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from queue import Empty
from typing import NewType

from ..communication.pipes import AsyncReadableBuffer, Pipe
from ..empty_factory import empty_app_factory as app_factory
from .interfaces import BeamlimeApplicationInterface, ControlInterface
from .providers import DataGenerator, Workflow

RawData = NewType("RawData", object)
app_factory.cache_product(Pipe[RawData], Pipe)
PostReductionData = NewType("PostReductionData", object)
app_factory.cache_product(Pipe[PostReductionData], Pipe)

DataStreamListener = NewType("DataStreamListener", BeamlimeApplicationInterface)


@app_factory.provider
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


@app_factory.provider
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
            try:
                async with self.raw_data_pipe.open_async_readable(
                    timeout=3
                ) as raw_data:
                    chunk = []
                    async for data in raw_data.readall():
                        chunk.append(data)
                    processed = self.workflow(chunk)
                    self.data_reduction_pipe.write(processed)
                    self.chunk_count += 1
                    self.debug("Processed %sth chunk. %s", self.chunk_count, processed)
            except Empty:
                self.info("No more data coming in ... Finishing ...")
                return


class DataPlotter(BeamlimeApplicationInterface):
    post_dr_pipe: Pipe[PostReductionData]

    def show(self):
        if not hasattr(self, "fig"):
            raise AttributeError("Please wait until the first figure is created.")
        return self.fig

    async def run(self) -> None:
        import plopp as pp

        if hasattr(self, "fig"):
            del self.fig

        await self.can_start(wait_on_true=True)
        if await self.running(wait_on_true=True):
            self.post_dr_pipe.chunk_size = 1
            async with self.post_dr_pipe.open_async_readable(timeout=5) as raw_data:
                self.first_data = (await raw_data.read()).hist()
                self.debug("First data as a seed of histogram: %s", self.first_data)
                self.stream_node = pp.Node(lambda: self.first_data)
                self.fig = pp.figure2d(self.stream_node)

        while await self.running(wait_on_true=True):
            try:
                async with self.post_dr_pipe.open_async_readable(timeout=3) as raw_data:
                    async for _data in raw_data.readall():
                        self.first_data.values += _data.hist().values
                    self.stream_node.notify_children("update")
                    self.debug("Updated plot.")
            except Empty:
                self.info("No more data coming in. Finishing ...")
                return


app_factory.register(DataStreamListener, DataStreamSimulator)
app_factory.cache_product(DataPlotter, DataPlotter)


@app_factory.provider
class DataProcessDaemon(BeamlimeApplicationInterface):
    remote_ctrl: ControlInterface
    data_stream_listener: DataStreamListener
    data_reduction: DataReduction
    data_plotter: DataPlotter

    def run(self):
        from ..core.schedulers import run_coroutines

        daemons: list[BeamlimeApplicationInterface]
        daemons = [self.data_stream_listener, self.data_reduction, self.data_plotter]
        daemon_coroutines = [daemon.run() for daemon in daemons]
        self.remote_ctrl.start()
        run_coroutines(*daemon_coroutines)
