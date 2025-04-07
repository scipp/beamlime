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
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping


def stream_kind_to_topic(instrument: str, kind: StreamKind) -> str:
    """
    Convert a StreamKind to a topic name.

    Used for constructing the topic name from the StreamKind when publishing to Kafka.
    The non-beamlime topics are thus only used when using our fake data generators.
    """
    match kind:
        case StreamKind.MONITOR_COUNTS:
            return f'{instrument}_beam_monitor'
        case StreamKind.MONITOR_EVENTS:
            return f'{instrument}_beam_monitor'
        case StreamKind.DETECTOR_EVENTS:
            return f'{instrument}_detector'
        case StreamKind.LOG:
            return f'{instrument}_motion'
        case StreamKind.BEAMLIME_DATA:
            return f'{instrument}_beamlime_data'
        case StreamKind.BEAMLIME_CONFIG:
            return f'{instrument}_beamlime_commands'
        case _:
            raise ValueError(f'Unknown stream kind: {kind}')


def _make_cbm_monitors(instrument: str, monitor_count: int = 10) -> StreamLUT:
    # Might also be MONITOR_COUNTS, but topic is supposedly the same.
    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.MONITOR_EVENTS)
    return {
        InputStreamKey(topic=topic, source_name=f'cbm{monitor}'): f'monitor{monitor}'
        for monitor in range(monitor_count)
    }


def _make_bifrost_detectors() -> StreamLUT:
    """
    Bifrost detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    # Source names have the format `arc=[0-4];triplet=[0-8]`.
    return {
        InputStreamKey(
            topic='bifrost_detector', source_name=f'arc={arc};triplet={triplet}'
        ): f'arc{arc}_triplet{triplet}'
        for arc in range(5)
        for triplet in range(9)
    }


def _make_dream_detectors() -> StreamLUT:
    """
    Dream detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
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


def _make_loki_detectors() -> StreamLUT:
    """
    Loki detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(
            topic=f'loki_detector_bank{bank}', source_name='caen'
        ): f'loki_detector_{bank}'
        for bank in range(9)
    }


def _make_nmx_detectors() -> StreamLUT:
    """
    NMX detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(
            topic=f'nmx_detector_p{panel}', source_name='nmx'
        ): 'nmx_detector'
        for panel in range(3)
    }


def _make_odin_detectors() -> StreamLUT:
    """
    Odin detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(topic='odin_detector', source_name='timepix3'): 'odin_detector'
    }


def _make_tbl_detectors() -> StreamLUT:
    """
    TBL detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(
            topic='tbl_detector_tpx3', source_name='timepix3'
        ): 'tbl_detector_tp3',
        InputStreamKey(
            topic='tbl_detector_mb', source_name='multiblade'
        ): 'tbl_detector_mb',
        InputStreamKey(
            topic='tbl_detector_3he', source_name='bank0'
        ): 'tbl_detector_3he_bank0',
        InputStreamKey(
            topic='tbl_detector_3he', source_name='bank1'
        ): 'tbl_detector_3he_bank1',
        InputStreamKey(
            topic='tbl_detector_ngem', source_name='tbl-ngem'
        ): 'tbl_detector_ngem',
    }


def _make_dev_detectors(instrument: str) -> StreamLUT:
    from beamlime.services.fake_detectors import detector_config

    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.DETECTOR_EVENTS)
    return {
        InputStreamKey(topic=topic, source_name=name): name
        for name in detector_config[instrument]
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


def _make_dev_stream_mapping(instrument: str) -> StreamMapping:
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


def _make_dev_instruments() -> dict[str, StreamMapping]:
    return {
        'dummy': _make_dev_stream_mapping('dummy'),
        'dream': _make_dev_stream_mapping('dream'),
        'loki': _make_dev_stream_mapping('loki'),
        'nmx': _make_dev_stream_mapping('nmx'),
        'bifrost': _make_dev_stream_mapping('bifrost'),
    }


def _make_common(instrument: str) -> dict[str, Any]:
    return {
        'instrument': instrument,
        'monitors': _make_cbm_monitors(instrument),
        'log_topics': None,
        'beamline_config_topic': stream_kind_to_topic(
            instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG
        ),
    }


def _make_production_instruments() -> dict[str, StreamMapping]:
    return {
        'bifrost': StreamMapping(
            **_make_common(instrument='bifrost'), detectors=_make_bifrost_detectors()
        ),
        'dream': StreamMapping(
            **_make_common(instrument='dream'), detectors=_make_dream_detectors()
        ),
        'loki': StreamMapping(
            **_make_common(instrument='loki'), detectors=_make_loki_detectors()
        ),
        'nmx': StreamMapping(
            **_make_common(instrument='nmx'), detectors=_make_nmx_detectors()
        ),
        'odin': StreamMapping(
            **_make_common(instrument='odin'), detectors=_make_odin_detectors()
        ),
        'tbl': StreamMapping(
            **_make_common(instrument='tbl'), detectors=_make_tbl_detectors()
        ),
    }


def get_stream_mapping(*, instrument: str, dev: bool) -> StreamMapping:
    """
    Returns the stream mapping for the given instrument.
    """
    if dev:
        return _make_dev_instruments()[instrument]
    return _make_production_instruments()[instrument]
