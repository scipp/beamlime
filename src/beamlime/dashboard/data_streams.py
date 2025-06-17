# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from holoviews import streams

from .data_key import ComponentDataKey, DataKey, MonitorDataKey
from .data_service import DataService
from .subscribers import ComponentDataSubscriber


class DataStream:
    def __init__(self, data_key: DataKey):
        self.pipe = streams.Pipe(data=None)
        self.data_key = data_key


class ComponentDataStream:
    """A data stream that holds a pipe and its associated subscriber."""

    def __init__(self, data_key: ComponentDataKey):
        self.pipe = streams.Pipe(data=None)
        self.data_key = data_key
        self.subscriber = ComponentDataSubscriber(
            component_key=self.data_key, pipe=self.pipe
        )


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
