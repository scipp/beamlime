# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import NewType

from ..communication.pipes import AsyncReadableBuffer, Pipe
from ..empty_factory import empty_app_factory as app_factory
from .data_generator import RawData, SimulationSetup
from .interfaces import BeamlimeApplicationInterface

PostReductionData = NewType("PreReductionData", object)

app_factory.cache_product(Pipe[PostReductionData], Pipe)


class DataFeeder(BeamlimeApplicationInterface):
    raw_data_pipe: Pipe[RawData]
    data_reduction_pipe: Pipe[PostReductionData]
    chunk_size: int = 16
    data_count: int = 0
    simulation_setup: SimulationSetup

    async def job(self) -> None:
        import scipp as sc

        st = self.simulation_setup
        raw_data: AsyncReadableBuffer
        async with self.raw_data_pipe.open_async_readable(timeout=3) as raw_data:
            _data: sc.Variable
            async for _data in raw_data.readall():
                self.data_count += 1
                x_coord = sc.linspace("x", start=0, stop=1, num=st.max_pixels, unit="m")
                y_coord = sc.linspace("y", start=0, stop=1, num=st.max_pixels, unit="m")
                processed = sc.DataArray(
                    data=_data, coords={"x": x_coord, "y": y_coord}
                )
                self.data_reduction_pipe.write(processed)
                self.debug("Processed %sth data. %s", self.data_count, processed)

    async def run(self) -> None:
        await self.can_start(wait_on_true=True)
        while await self.running(wait_on_true=True):
            await self.job()


app_factory.register(DataFeeder, DataFeeder)
