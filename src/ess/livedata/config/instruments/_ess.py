# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Helpers for setting up the stream mapping for the ESS instruments.
"""

from __future__ import annotations

from typing import Any

from ess.livedata import StreamKind
from ess.livedata.config.streams import stream_kind_to_topic
from ess.livedata.kafka import InputStreamKey, StreamLUT, StreamMapping


def _make_cbm_monitors(instrument: str, monitor_count: int = 10) -> StreamLUT:
    # Might also be MONITOR_COUNTS, but topic is supposedly the same.
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.MONITOR_EVENTS)
    return {
        InputStreamKey(topic=topic, source_name=f'cbm{monitor}'): f'monitor{monitor}'
        for monitor in range(monitor_count)
    }


def _make_dev_detectors(*, instrument: str, detectors: list[str]) -> StreamLUT:
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.DETECTOR_EVENTS)
    return {InputStreamKey(topic=topic, source_name=name): name for name in detectors}


def _make_dev_beam_monitors(instrument: str) -> StreamLUT:
    # Might also be MONITOR_COUNTS, but topic is supposedly the same.
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.MONITOR_EVENTS)
    return {
        InputStreamKey(
            topic=topic, source_name=f'monitor{monitor}'
        ): f'monitor{monitor}'
        for monitor in range(10)
    }


def make_dev_stream_mapping(instrument: str, detectors: list[str]) -> StreamMapping:
    motion_topic = f'{instrument}_motion'
    log_topics = {motion_topic}
    return StreamMapping(
        instrument=instrument,
        detectors=_make_dev_detectors(instrument=instrument, detectors=detectors),
        monitors=_make_dev_beam_monitors(instrument),
        log_topics=log_topics,
        beamlime_config_topic=stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG
        ),
        beamlime_data_topic=stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_DATA
        ),
        beamlime_status_topic=stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_STATUS
        ),
    )


def make_common_stream_mapping_inputs(instrument: str) -> dict[str, Any]:
    return {
        'instrument': instrument,
        'monitors': _make_cbm_monitors(instrument),
        'log_topics': None,
        'beamlime_config_topic': stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG
        ),
        'beamlime_data_topic': stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_DATA
        ),
        'beamlime_status_topic': stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_STATUS
        ),
    }
