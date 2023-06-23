# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import NewType

from ..communication.pipes import Pipe
from ..empty_factory import empty_app_factory as app_factory
from .interfaces import BeamlimeApplicationInterface

RawData = NewType("RawData", object)

app_factory.cache_product(Pipe[RawData], Pipe)


@dataclass
class SimulationSetup:
    num_pulses: int = 10
    max_pixels: int = 100
    random_seed: int = 123


app_factory.cache_product(SimulationSetup, SimulationSetup)


@app_factory.provider
class DataGenerator(BeamlimeApplicationInterface):
    raw_data_pipe: Pipe[RawData]
    chunk_size: int = 16
    simulation_settup: SimulationSetup

    async def run(self) -> None:
        import asyncio

        import scipp as sc
        from numpy.random import default_rng

        if await self.can_start(wait_on_true=True):
            st = self.simulation_settup
            rng = default_rng(self.simulation_settup.random_seed)

            for i_pulse in range(st.num_pulses):
                if await self.running(wait_on_true=True):
                    data_size = [int(st.max_pixels)] * 2
                    data = sc.array(
                        dims=["x", "y"], values=rng.random(data_size), unit="count"
                    )
                    self.raw_data_pipe.write(data)
                    self.info("Sending %sth data.", i_pulse)

        self.info("Finish data generating, will wait for 2 seconds more ...")
        await asyncio.sleep(2)
        self._command.stop()
