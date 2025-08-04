# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Utilities for connecting subscribers to :py:class:`DataService`
"""

from collections.abc import Callable, Hashable
from typing import Any, Generic, TypeVar

from .assemblers import ComponentStreamAssembler, MergingStreamAssembler
from .data_key import DataKey, DetectorDataKey, MonitorDataKey
from .data_service import DataService
from .data_subscriber import DataSubscriber, Pipe, StreamAssembler

P = TypeVar('P', bound=Pipe)


class StreamManager(Generic[P]):
    """Base class for managing data streams."""

    def __init__(self, *, data_service: DataService, pipe_factory: Callable[[], P]):
        self.data_service = data_service
        self._pipes: dict[Any, P] = {}
        self._pipe_factory = pipe_factory

    def _get_or_create_stream(self, key: Hashable, assembler: StreamAssembler) -> P:
        """Get or create a stream for the given key and assembler."""
        if key not in self._pipes:
            pipe = self._pipe_factory()
            subscriber = DataSubscriber(assembler, pipe)
            self.data_service.register_subscriber(subscriber)
            self._pipes[key] = pipe
        return self._pipes[key]


class DetectorStreamManager(StreamManager[P]):
    """A manager for detector data streams."""

    def get_stream(self, component_name: str, view_name: str) -> P:
        """Get or create a data stream for the given component key and view name."""
        data_key = DetectorDataKey(component_name=component_name, view_name=view_name)
        assembler = ComponentStreamAssembler(data_key)
        return self._get_or_create_stream(data_key, assembler)


class MonitorStreamManager(StreamManager[P]):
    """A manager for monitor data streams."""

    def get_stream(self, component_name: str) -> P:
        """Get or create a data stream for the given component key."""
        data_key = MonitorDataKey(component_name=component_name, view_name='')
        assembler = ComponentStreamAssembler(data_key)
        return self._get_or_create_stream(data_key, assembler)


class ReductionStreamManager(StreamManager[P]):
    """A manager for reduction data streams."""

    def get_stream(self, source_names: set[str], view_name: str) -> P:
        """Get or create a data stream for the given component key and view."""
        data_keys = {
            DataKey(
                service_name='data_reduction',
                source_name=source_name,
                key=f'reduced/{source_name}/{view_name}',
            )
            for source_name in source_names
        }
        key = (tuple(sorted(source_names)), view_name)
        assembler = MergingStreamAssembler(data_keys)
        return self._get_or_create_stream(key, assembler)
