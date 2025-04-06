# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing_extensions import Self

from ..config.streams import stream_kind_to_topic
from ..core.message import StreamKind
from .message_adapter import (
    BeamlimeConfigMessageAdapter,
    ChainedAdapter,
    Da00ToScippAdapter,
    Ev44ToDetectorEventsAdapter,
    F144ToLogDataAdapter,
    KafkaToDa00Adapter,
    KafkaToEv44Adapter,
    KafkaToF144Adapter,
    KafkaToMonitorEventsAdapter,
    MessageAdapter,
    RouteBySchemaAdapter,
    RouteByTopicAdapter,
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
        self._routes.update(_beam_monitor_route(self._stream_mapping))
        return self

    def with_detector_route(self) -> Self:
        """Adds the detector route."""
        self._routes.update(_detector_route(self._stream_mapping))
        return self

    def with_logdata_route(self) -> Self:
        """Adds the logdata route."""
        self._routes.update(_logdata_route(self._stream_mapping.instrument))
        return self

    def with_beamlime_config_route(self) -> Self:
        """Adds the beamlime config route."""
        self._routes.update(_beamlime_config_route(self._stream_mapping.instrument))
        return self


def _beamlime_config_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for beamlime configuration."""
    topic = stream_kind_to_topic(instrument, StreamKind.BEAMLIME_CONFIG)
    return {topic: BeamlimeConfigMessageAdapter()}


def _beam_monitor_route(stream_mapping: StreamMapping) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for monitor data."""
    monitors = RouteBySchemaAdapter(
        routes={
            'ev44': KafkaToMonitorEventsAdapter(stream_lut=stream_mapping.monitors),
            'da00': ChainedAdapter(
                first=KafkaToDa00Adapter(
                    stream_lut=stream_mapping.monitors,
                    stream_kind=StreamKind.MONITOR_COUNTS,
                ),
                second=Da00ToScippAdapter(),
            ),
        }
    )
    return {topic: monitors for topic in stream_mapping.monitor_topics}


def _detector_route(stream_mapping: StreamMapping) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for detector data."""
    detectors = ChainedAdapter(
        first=KafkaToEv44Adapter(
            stream_lut=stream_mapping.detectors, stream_kind=StreamKind.DETECTOR_EVENTS
        ),
        second=Ev44ToDetectorEventsAdapter(
            merge_detectors=stream_mapping.instrument == 'bifrost'
        ),
    )
    return {topic: detectors for topic in stream_mapping.detector_topics}


def _logdata_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for log data."""
    topic = stream_kind_to_topic(instrument, StreamKind.LOG)
    return {
        topic: ChainedAdapter(first=KafkaToF144Adapter(), second=F144ToLogDataAdapter())
    }
