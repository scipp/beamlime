# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from beamlime.communication import Pipe
from beamlime.constructors import Factory


def test_pipe_single_instance_per_type():
    factory = Factory()
    factory.cache_product(Pipe[int], Pipe)
    factory.cache_product(Pipe[float], Pipe)
    assert factory[Pipe[int]] is factory[Pipe[int]]
    assert factory[Pipe[float]] is factory[Pipe[float]]
    assert factory[Pipe[float]] is not factory[Pipe[int]]
