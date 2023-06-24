# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from types import FunctionType, GeneratorType
from typing import NewType

import scipp as sc

from ..empty_factory import empty_app_factory as app_factory


@dataclass
class SimulationSetup:
    num_data: int = 12
    max_pixels: int = 64
    random_seed: int = 123


app_factory.cache_product(SimulationSetup, SimulationSetup)

DataGenerator = NewType("DataGenerator", GeneratorType)


@app_factory.provider
def data_generator_provider(simulation_setup: SimulationSetup) -> DataGenerator:
    from numpy.random import default_rng

    st = simulation_setup
    rng = default_rng(st.random_seed)
    data_size = int(st.max_pixels)
    x_coord = sc.linspace("counts", start=0, stop=1, num=st.max_pixels, unit="m")
    y_coord = sc.linspace("counts", start=0, stop=1, num=st.max_pixels, unit="m")

    def data_generator():
        for _ in range(st.num_data):
            values = rng.random(data_size)
            data = sc.array(dims=["counts"], values=values, unit="count")
            yield sc.DataArray(data=data, coords={"x": x_coord, "y": y_coord})

    return data_generator


Workflow = NewType("Workflow", FunctionType)


@app_factory.provider
def workflow_provider(simulation_setup: SimulationSetup) -> Workflow:
    from functools import reduce

    binsize = int(simulation_setup.max_pixels / 16)

    def workflow(data_chunk: list[sc.DataArray]):
        summed = reduce(lambda x, y: x + y, data_chunk)
        return summed.bin(x=binsize, y=binsize)

    return workflow
