# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from .helpers import (
    beam_monitor_topic,
    beamlime_config_topic,
    detector_topic,
    motion_topic,
)
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
)


def beamlime_config_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for beamlime configuration."""
    return {beamlime_config_topic(instrument): BeamlimeConfigMessageAdapter()}


def beam_monitor_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for monitor data."""
    monitors = RouteBySchemaAdapter(
        routes={
            'ev44': KafkaToMonitorEventsAdapter(),
            'da00': ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        }
    )
    return {beam_monitor_topic(instrument): monitors}


def detector_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for detector data."""
    detectors = ChainedAdapter(
        first=KafkaToEv44Adapter(),
        second=Ev44ToDetectorEventsAdapter(merge_detectors=instrument == 'bifrost'),
    )
    return {detector_topic(instrument): detectors}


def logdata_route(instrument: str) -> dict[str, MessageAdapter]:
    """Returns a dictionary of routes for log data."""
    return {
        motion_topic(instrument): ChainedAdapter(
            first=KafkaToF144Adapter(), second=F144ToLogDataAdapter()
        )
    }
