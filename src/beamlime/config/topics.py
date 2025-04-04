# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from collections.abc import Sequence

from ..core.message import StreamKind


def topic_for_instrument(*, topic: str | Sequence[str], instrument: str) -> str:
    """
    Return the topic name(s) for a given instrument.

    This implements the ECDC topic naming convention, prefixing the topic name with the
    instrument name.

    Parameters
    ----------
    topic:
        The topic name(s) to prefix with the instrument name.
    """
    if isinstance(topic, str):
        return f'{instrument}_{topic}'
    return [topic_for_instrument(topic=t, instrument=instrument) for t in topic]


def beam_monitor_topic(instrument: str) -> str:
    """
    Return the topic name for the beam monitor data of an instrument.
    """
    return topic_for_instrument(topic='beam_monitor', instrument=instrument)


def detector_topic(instrument: str) -> str:
    """
    Return the topic name for the detector data of an instrument.
    """
    return topic_for_instrument(topic='detector', instrument=instrument)


def motion_topic(instrument: str) -> str:
    """
    Return the topic name for the motion data of an instrument.
    """
    return topic_for_instrument(topic='motion', instrument=instrument)


def beamlime_config_topic(instrument: str) -> str:
    """
    Return the topic name for the beamlime configuration of an instrument.
    """
    return topic_for_instrument(topic='beamlime_commands', instrument=instrument)


def source_name(device: str, signal: str) -> str:
    """
    Return the source name for a given device and signal.

    This is used to construct the source name from the device name and signal name
    The source_name is used in various Kafka messages.

    ':' is used as the separator in the ECDC naming convention at ESS.
    """
    return f'{device}:{signal}'


def stream_kind_to_topic(instrument: str, kind: StreamKind) -> str:
    """
    Convert a StreamKind to a topic name.

    Used for constructing the topic name from the StreamKind when publishing to Kafka.
    The non-beamlime topics are thus only used when using our fake data generators.
    """
    match kind:
        case StreamKind.MONITOR_COUNTS:
            return beam_monitor_topic(instrument)
        case StreamKind.MONITOR_EVENTS:
            return beam_monitor_topic(instrument)
        case StreamKind.DETECTOR_EVENTS:
            return detector_topic(instrument)
        case StreamKind.LOG:
            return motion_topic(instrument)
        case StreamKind.BEAMLIME_DATA:
            return topic_for_instrument(topic='beamlime_data', instrument=instrument)
        case StreamKind.BEAMLIME_CONFIG:
            return beamlime_config_topic(instrument)
        case _:
            raise ValueError(f'Unknown stream kind: {kind}')
