# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from typing import Dict, Type

from .pipes import BufferData, Pipe


class PipeLine:
    """
    PipeLine class that contains all data pipes.

    There should be only 1 pipe for each type.

    If the requested type of pipe doesn't exist,
    it create one and returns the pipe.
    """

    _pipes: Dict[Type, Pipe] = dict()

    @classmethod
    def connect_pipe(cls, pipe_type: Type[Pipe[BufferData]]) -> Pipe[BufferData]:
        if pipe_type in cls._pipes:
            return cls._pipes[pipe_type]
        else:
            cls._pipes[pipe_type] = Pipe()
            return cls._pipes[pipe_type]


def pipe_builder(pipe_type: Type[Pipe[BufferData]]) -> Pipe:
    """
    Pipe provider helper function.
    It can be used with partial to create a specific type of pipe.
    """
    return PipeLine.connect_pipe(pipe_type)
