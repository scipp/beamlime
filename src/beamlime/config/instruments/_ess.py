# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Mappings from (topic, source_name) to internal stream identifier.

Raw data is received from Kafka on a variety of topics. Each message has a source name.
The latter is not unique (not event within an instrument), only the combination of topic
and source name is. To isolate Beamlime code from the details of the Kafka topics, we
use this mapping to assign a unique stream name to each (topic, source name) pair.
"""

from __future__ import annotations

from typing import Any

from beamlime import StreamKind
from beamlime.config.streams import stream_kind_to_topic
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from . import get_config


def _make_cbm_monitors(instrument: str, monitor_count: int = 10) -> StreamLUT:
    # Might also be MONITOR_COUNTS, but topic is supposedly the same.
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.MONITOR_EVENTS)
    return {
        InputStreamKey(topic=topic, source_name=f'cbm{monitor}'): f'monitor{monitor}'
        for monitor in range(monitor_count)
    }


def _make_dev_detectors(instrument: str) -> StreamLUT:
    config = get_config(instrument=instrument)
    dev_detectors = config.detectors_config['fakes']

    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.DETECTOR_EVENTS)
    return {
        InputStreamKey(topic=topic, source_name=name): name for name in dev_detectors
    }


def _make_dev_beam_monitors(instrument: str) -> StreamLUT:
    # Might also be MONITOR_COUNTS, but topic is supposedly the same.
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.MONITOR_EVENTS)
    return {
        InputStreamKey(
            topic=topic, source_name=f'monitor{monitor}'
        ): f'monitor{monitor}'
        for monitor in range(10)
    }


def make_dev_stream_mapping(instrument: str) -> StreamMapping:
    motion_topic = f'{instrument}_motion'
    log_topics = {motion_topic}
    return StreamMapping(
        instrument=instrument,
        detectors=_make_dev_detectors(instrument),
        monitors=_make_dev_beam_monitors(instrument),
        log_topics=log_topics,
        beamline_config_topic=stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG
        ),
    )


def make_common_stream_mapping_inputs(instrument: str) -> dict[str, Any]:
    return {
        'instrument': instrument,
        'monitors': _make_cbm_monitors(instrument),
        'log_topics': None,
        'beamline_config_topic': stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG
        ),
    }
