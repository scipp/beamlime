# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Self

from ..core.message import StreamKind
from .message_adapter import (
    ChainedAdapter,
    Da00ToScippAdapter,
    Ev44ToDetectorEventsAdapter,
    F144ToLogDataAdapter,
    KafkaToDa00Adapter,
    KafkaToEv44Adapter,
    KafkaToF144Adapter,
    KafkaToMonitorEventsAdapter,
    LivedataConfigMessageAdapter,
    MessageAdapter,
    RouteBySchemaAdapter,
    RouteByTopicAdapter,
    X5f2ToJobStatusAdapter,
)
from .stream_mapping import StreamMapping


class RoutingAdapterBuilder:
    def __init__(self, *, stream_mapping: StreamMapping):
        self._stream_mapping = stream_mapping
        self._routes: dict[str, MessageAdapter] = {}

    def build(self) -> RouteByTopicAdapter:
        """Builds the routing adapter."""
        return RouteByTopicAdapter(self._routes)

    def with_beam_monitor_route(self) -> Self:
        """Adds the beam monitor route."""
        adapter = RouteBySchemaAdapter(
            routes={
                'ev44': KafkaToMonitorEventsAdapter(
                    stream_lut=self._stream_mapping.monitors
                ),
                'da00': ChainedAdapter(
                    first=KafkaToDa00Adapter(
                        stream_lut=self._stream_mapping.monitors,
                        stream_kind=StreamKind.MONITOR_COUNTS,
                    ),
                    second=Da00ToScippAdapter(),
                ),
            }
        )
        for topic in self._stream_mapping.monitor_topics:
            self._routes[topic] = adapter
        return self

    def with_detector_route(self) -> Self:
        """Adds the detector route."""
        adapter = ChainedAdapter(
            first=KafkaToEv44Adapter(
                stream_lut=self._stream_mapping.detectors,
                stream_kind=StreamKind.DETECTOR_EVENTS,
            ),
            second=Ev44ToDetectorEventsAdapter(
                merge_detectors=self._stream_mapping.instrument == 'bifrost'
            ),
        )
        for topic in self._stream_mapping.detector_topics:
            self._routes[topic] = adapter
        return self

    def with_logdata_route(self) -> Self:
        """Adds the logdata route."""
        adapter = ChainedAdapter(
            first=KafkaToF144Adapter(stream_lut=self._stream_mapping.logs),
            second=F144ToLogDataAdapter(),
        )
        for topic in self._stream_mapping.log_topics:
            self._routes[topic] = adapter
        return self

    def with_livedata_data_route(self) -> Self:
        """Adds the livedata data route."""
        self._routes[self._stream_mapping.livedata_data_topic] = ChainedAdapter(
            first=KafkaToDa00Adapter(stream_kind=StreamKind.LIVEDATA_DATA),
            second=Da00ToScippAdapter(),
        )
        return self

    def with_livedata_config_route(self) -> Self:
        """Adds the livedata config route."""
        self._routes[self._stream_mapping.livedata_config_topic] = (
            LivedataConfigMessageAdapter()
        )
        return self

    def with_livedata_status_route(self) -> Self:
        """Adds the livedata status route."""
        self._routes[self._stream_mapping.livedata_status_topic] = (
            X5f2ToJobStatusAdapter()
        )
        return self
