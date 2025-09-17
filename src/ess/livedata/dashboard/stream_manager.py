# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Utilities for connecting subscribers to :py:class:`DataService`
"""

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from ess.livedata.config.workflow_spec import ResultKey

from .data_service import DataService
from .data_subscriber import DataSubscriber, Pipe, StreamAssembler

P = TypeVar('P', bound=Pipe)


class MergingStreamAssembler(StreamAssembler):
    """Assembler for merging data from multiple sources into a dict."""

    def assemble(self, data: dict[ResultKey, Any]) -> dict[ResultKey, Any]:
        return {key: data[key] for key in self.keys if key in data}


class StreamManager(Generic[P]):
    """Base class for managing data streams."""

    def __init__(
        self,
        *,
        data_service: DataService,
        pipe_factory: Callable[[dict[ResultKey, Any]], P],
    ):
        self.data_service = data_service
        self._pipe_factory = pipe_factory

    def make_merging_stream(self, items: dict[ResultKey, Any]) -> P:
        """Create a merging stream for the given set of data keys."""
        assembler = MergingStreamAssembler(set(items))
        pipe = self._pipe_factory(items)
        subscriber = DataSubscriber(assembler, pipe)
        self.data_service.register_subscriber(subscriber)
        return pipe
