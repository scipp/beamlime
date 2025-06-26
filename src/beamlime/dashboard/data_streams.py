# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Utilities for connecting subscribers to :py:class:`DataService`
"""

from holoviews import streams

from .data_key import ComponentDataKey, DataKey, MonitorDataKey
from .data_service import DataService
from .subscribers import ComponentDataSubscriber, MergingDataSubscriber


class ComponentDataStream:
    """A data stream that holds a pipe and its associated subscriber."""

    def __init__(self, data_key: ComponentDataKey):
        self.pipe = streams.Pipe(data=None)
        self.data_key = data_key
        self.subscriber = ComponentDataSubscriber(
            component_key=self.data_key, pipe=self.pipe
        )


class MergingDataStream:
    """A data stream that merges multiple data sources into a single pipe."""

    def __init__(self, keys: set[DataKey]):
        self.pipe = streams.Pipe(data=None)
        self.keys = keys
        self.subscriber = MergingDataSubscriber(keys=keys, pipe=self.pipe)


class MonitorStreamManager:
    """A manager for monitor data streams."""

    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self.streams: dict[ComponentDataKey, ComponentDataStream] = {}

    def get_stream(self, component_name: str) -> streams.Pipe:
        """Get or create a data stream for the given component key."""
        # Currently there are now views for monitor data, so we use an empty view name.
        data_key = MonitorDataKey(component_name=component_name, view_name='')
        if data_key not in self.streams:
            stream = ComponentDataStream(data_key)
            self.data_service.register_subscriber(stream.subscriber)
            self.streams[data_key] = stream
        return self.streams[data_key].pipe


class ReductionStreamManager:
    """A manager for reduction data streams."""

    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self.streams: dict[tuple[tuple[str, ...], str], MergingDataStream] = {}

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
        if key not in self.streams:
            stream = MergingDataStream(data_keys)
            self.data_service.register_subscriber(stream.subscriber)
            self.streams[key] = stream
        return self.streams[key].pipe
