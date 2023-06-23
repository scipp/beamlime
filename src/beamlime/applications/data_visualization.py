# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from ..communication.pipes import AsyncReadableBuffer, Pipe
from ..empty_factory import empty_app_factory as app_factory
from .data_reduction import PostReductionData
from .interfaces import BeamlimeApplicationInterface


class DataPlotter(BeamlimeApplicationInterface):
    post_dr_pipe: Pipe[PostReductionData]

    async def job(self) -> None:
        import scipp as sc

        raw_data: AsyncReadableBuffer
        async with self.post_dr_pipe.open_async_readable(timeout=3) as raw_data:
            _data: sc.Variable
            async for _data in raw_data.readall():
                self.first_data.values += _data.values
            self.stream_node.notify_children("update")
            self.debug("Updated plot.")

    async def run(self) -> None:
        import plopp as pp

        await self.can_start(wait_on_true=True)
        if await self.running(wait_on_true=True):
            async with self.post_dr_pipe.open_async_readable(timeout=3) as raw_data:
                self.first_data = await raw_data.read()
                self.stream_node = pp.Node(lambda: self.first_data)
                self.fig = pp.figure2d(self.stream_node)

        while await self.running(wait_on_true=True):
            await self.job()


app_factory.cache_product(DataPlotter, DataPlotter)
