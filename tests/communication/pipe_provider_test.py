# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from beamlime.communication.providers import Pipe, pipe_builder


def test_pipe_single_instance_per_type():
    type1 = Pipe[int]
    type2 = Pipe[float]
    assert pipe_builder(type1) is pipe_builder(type1)
    assert pipe_builder(type2) is pipe_builder(type2)
    assert pipe_builder(type1) is not pipe_builder(type2)
