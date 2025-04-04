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

from beamlime.kafka import InputStreamKey, KafkaTopic

from .topics import beam_monitor_topic, detector_topic


class StreamMapping:
    def __init__(
        self,
        instrument: str,
        detectors: dict[InputStreamKey, str],
        monitors: dict[InputStreamKey, str],
    ) -> None:
        self.instrument = instrument
        self._detectors = detectors
        self._monitors = monitors

    @property
    def detector_topics(self) -> set[KafkaTopic]:
        """Returns the list of detector topics."""
        return {stream.topic for stream in self.detectors.keys()}

    @property
    def monitor_topics(self) -> set[KafkaTopic]:
        """Returns the list of monitor topics."""
        return {stream.topic for stream in self.monitors.keys()}

    @property
    def detectors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for detector data."""
        return self._detectors

    @property
    def monitors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for monitor data."""
        return self._monitors


def _make_cbm_monitors(
    instrument: str, monitor_count: int = 10
) -> dict[InputStreamKey, str]:
    return {
        InputStreamKey(
            topic=beam_monitor_topic(instrument), source_name=f'cbm{monitor}'
        ): f'monitor{monitor}'
        for monitor in range(monitor_count)
    }


def _make_loki_detectors() -> dict[InputStreamKey, str]:
    return {
        InputStreamKey(
            topic=f'loki_detector_bank{bank}', source_name='caen'
        ): f'loki_detector_{bank}'
        for bank in range(9)
    }


def _make_dream_detectors() -> dict[InputStreamKey, str]:
    mapping = {
        'bwec': 'endcap_backward',
        'fwec': 'endcap_forward',
        'hr': 'high_resolution',
        'mantle': 'mantle',
        'sans': 'sans',
    }
    return {
        InputStreamKey(
            topic=f'dream_detector_{key}', source_name='dream'
        ): f'{value}_detector'
        for key, value in mapping.items()
    }


def _make_dev_detectors(instrument: str) -> dict[InputStreamKey, str]:
    from beamlime.services.fake_detectors import detector_config

    return {
        InputStreamKey(topic=detector_topic(instrument), source_name=name): name
        for name in detector_config[instrument]
    }


def _make_dev_beam_monitors(instrument: str) -> dict[InputStreamKey, str]:
    return {
        InputStreamKey(
            topic=beam_monitor_topic(instrument), source_name=f'monitor{monitor}'
        ): f'monitor{monitor}'
        for monitor in range(10)
    }


def _make_dev_stream_mapping(instrument: str) -> StreamMapping:
    return StreamMapping(
        instrument=instrument,
        detectors=_make_dev_detectors(instrument),
        monitors=_make_dev_beam_monitors(instrument),
    )


def _dev_instruments():
    return {
        'dummy': _make_dev_stream_mapping('dummy'),
        'dream': _make_dev_stream_mapping('dream'),
        'loki': _make_dev_stream_mapping('loki'),
        'nmx': _make_dev_stream_mapping('nmx'),
        'bifrost': _make_dev_stream_mapping('bifrost'),
    }


_production_instruments = {
    'dream': StreamMapping(
        instrument='dream',
        detectors=_make_dream_detectors(),
        monitors=_make_cbm_monitors('dream'),
    ),
    'loki': StreamMapping(
        instrument='loki',
        detectors=_make_loki_detectors(),
        monitors=_make_cbm_monitors('loki'),
    ),
}


def get_stream_mapping(*, instrument: str, dev: bool) -> StreamMapping:
    """
    Returns the stream mapping for the given instrument.
    """
    if dev:
        return _dev_instruments()[instrument]
    return _production_instruments[instrument]
