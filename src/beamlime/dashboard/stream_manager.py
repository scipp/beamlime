# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Utilities for connecting subscribers to :py:class:`DataService`
"""

from collections.abc import Hashable
from typing import Any

from holoviews import streams

from .assemblers import ComponentStreamAssembler, MergingStreamAssembler
from .data_key import DataKey, MonitorDataKey
from .data_service import DataService
from .data_subscriber import DataSubscriber, StreamAssembler


class StreamManager:
    """Base class for managing data streams."""

    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self.pipes: dict[Any, streams.Pipe] = {}

    def _get_or_create_stream(
        self, key: Hashable, assembler: StreamAssembler
    ) -> streams.Pipe:
        """Get or create a stream for the given key and assembler."""
        if key not in self.pipes:
            pipe = streams.Pipe(data=None)
            subscriber = DataSubscriber(assembler, pipe)
            self.data_service.register_subscriber(subscriber)
            self.pipes[key] = pipe
        return self.pipes[key]


class MonitorStreamManager(StreamManager):
    """A manager for monitor data streams."""

    def get_stream(self, component_name: str) -> streams.Pipe:
        """Get or create a data stream for the given component key."""
        data_key = MonitorDataKey(component_name=component_name, view_name='')
        assembler = ComponentStreamAssembler(data_key)
        return self._get_or_create_stream(data_key, assembler)


class ReductionStreamManager(StreamManager):
    """A manager for reduction data streams."""

    def get_stream(self, source_names: set[str], view_name: str) -> streams.Pipe:
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
