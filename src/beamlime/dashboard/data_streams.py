# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Utilities for connecting subscribers to :py:class:`DataService`
"""

from holoviews import streams

from .data_key import ComponentDataKey, DataKey, MonitorDataKey
from .data_service import DataService
from .data_subscriber import DataSubscriber
from .subscribers import ComponentStreamAssembler, MergingStreamAssembler


class MonitorStreamManager:
    """A manager for monitor data streams."""

    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self.pipes: dict[ComponentDataKey, streams.Pipe] = {}

    def get_stream(self, component_name: str) -> streams.Pipe:
        """Get or create a data stream for the given component key."""
        # Currently there are now views for monitor data, so we use an empty view name.
        data_key = MonitorDataKey(component_name=component_name, view_name='')
        if data_key not in self.pipes:
            pipe = streams.Pipe(data=None)
            assembler = ComponentStreamAssembler(data_key)
            subscriber = DataSubscriber(assembler, pipe)
            self.data_service.register_subscriber(subscriber)
            self.pipes[data_key] = pipe
        return self.pipes[data_key]


class ReductionStreamManager:
    """A manager for reduction data streams."""

    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self.pipes: dict[tuple[tuple[str, ...], str], streams.Pipe] = {}

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
        key = (tuple(source_names), view_name)
        if key not in self.pipes:
            pipe = streams.Pipe(data=None)
            assembler = MergingStreamAssembler(data_keys)
            subscriber = DataSubscriber(assembler, pipe)
            self.data_service.register_subscriber(subscriber)
            self.pipes[key] = pipe
        return self.pipes[key]
