# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from typing import Dict, Generic, Type

from beamlime.communication.pipes import (
    DEFAULT_CHUNK_SIZE,
    MAX_BUFFER_SIZE,
    BufferData,
    PipeObject,
    pipe,
)


class Pipe(pipe, Generic[BufferData]):
    """
    Pipe generic type.
    Calling the Pipe generic type of ``BufferData`` type
    will create a provider of the ``Pipe[BufferData]`` automatically
    by creating a new ``PipeObject`` via ``pipe``.

    ``pipe`` class will contain all ``PipeObject`` istances created
    via its constructor.

    If registering provider is not intended, use ``pipe`` for type hints
    instead of ``Pipe``.

    """

    _pipes: Dict[Type, PipeObject] = dict()

    def __new__(
        cls,
        *,
        data_type: type,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_size: int = MAX_BUFFER_SIZE,
    ):
        return pipe(data_type=data_type, chunk_size=chunk_size, max_size=max_size)

    def __class_getitem__(cls, tp: Type[BufferData]):
        from functools import partial

        from beamlime.constructors import ProviderNotFoundError, Providers

        _buffer_type = pipe[tp]  # type: ignore[valid-type]
        try:
            Providers[_buffer_type]
        except ProviderNotFoundError:
            Providers[_buffer_type] = partial(pipe, data_type=tp)
        return _buffer_type
