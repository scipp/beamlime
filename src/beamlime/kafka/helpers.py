# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from collections.abc import Sequence


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
