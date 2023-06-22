# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from typing import Dict, Type

from .pipes import BufferData, Pipe


class PipeLine:
    _pipes: Dict[Type, Pipe] = dict()

    @classmethod
    def build_pipe(cls, pipe_type: Type[Pipe[BufferData]]):
        if pipe_type in cls._pipes:
            return cls._pipes[pipe_type]
        else:
            cls._pipes[pipe_type] = Pipe()
            return cls._pipes[pipe_type]


def pipe_builder(pipe_type: Type[Pipe[BufferData]]) -> Pipe:
    return PipeLine.build_pipe(pipe_type)
