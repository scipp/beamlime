# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Mappings from (topic, source_name) to internal stream name.

Raw data is received from Kafka on a variety of topics. Each message has a source name.
The latter is not unique (not event within an instrument), only the combination of topic
and source name is. To isolate Beamlime code from the details of the Kafka topics, we
use this mapping to assign a unique stream name to each (topic, source name) pair.
"""

from __future__ import annotations

from beamlime.kafka.message_adapter import InputStreamKey

from .topics import beam_monitor_topic, detector_topic

# We have two options, either setup topic-based routes dynamically for each instrument,
# or convert to internal stream names before sending to adapters.
# To know the source name, we need to desrialize the message, which requires specific
# adapter => topic-based routes.


class StreamMapping:
    def __init__(
        self,
        detectors: dict[InputStreamKey, str],
        monitors: dict[InputStreamKey, str],
    ) -> None:
        self._detectors = detectors
        self._monitors = monitors

    @property
    def detector_topics(self) -> list[str]:
        """Returns the list of detector topics."""
        return [stream.topic for stream in self.detectors.keys()]

    @property
    def monitor_topics(self) -> list[str]:
        """Returns the list of monitor topics."""
        return [stream.topic for stream in self.monitors.keys()]

    @property
    def detectors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for detector data."""
        return self._detectors

    @property
    def monitors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for monitor data."""
        return self._monitors


def _make_loki_detectors(production: bool = False) -> dict[InputStreamKey, str]:
    if production:
        return {
            InputStreamKey(
                topic=f'loki_detector_bank{bank}', source_name='caen'
            ): f'loki_detector_{bank}'
            for bank in range(9)
        }
    return {
        InputStreamKey(
            topic=detector_topic('loki'), source_name=f'loki_detector_{bank}'
        ): f'loki_detector_{bank}'
        for bank in range(9)
    }


def _make_cbm_monitors(
    instrument: str, monitor_count: int = 10
) -> dict[InputStreamKey, str]:
    return {
        InputStreamKey(
            topic=beam_monitor_topic(instrument), source_name=f'cbm{monitor}'
        ): f'monitor{monitor}'
        for monitor in range(monitor_count)
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


def make_dev_stream_mapping(instrument: str) -> StreamMapping:
    return StreamMapping(
        detectors=_make_dev_detectors(instrument),
        monitors=_make_dev_beam_monitors(instrument),
    )


_dev_instruments = {
    'dummy': make_dev_stream_mapping('dummy'),
    'dream': make_dev_stream_mapping('dream'),
    'loki': make_dev_stream_mapping('loki'),
    'nmx': make_dev_stream_mapping('nmx'),
    'bifrost': make_dev_stream_mapping('bifrost'),
}


instruments = {
    'loki': StreamMapping(
        detectors=_make_loki_detectors(), monitors=_make_cbm_monitors('loki')
    ),
}


def get_stream_mapping(instrument: str, production: bool = False) -> StreamMapping:
    """
    Returns the stream mapping for the given instrument.
    """
    if not production:
        return _dev_instruments[instrument]
    if instrument == 'loki':
        return instruments['loki']
    else:
        raise ValueError(f'Unknown instrument: {instrument}')
