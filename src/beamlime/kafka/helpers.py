# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
def topic_for_instrument(*, topic: str, instrument: str) -> str:
    """
    Return the topic name for a given instrument.

    This implements the ECDC topic naming convention, prefixing the topic name with the
    instrument name.
    """
    return f'{instrument}_{topic}'


def beam_monitor_topic(instrument: str) -> str:
    """
    Return the topic name for the beam monitor data of an instrument.
    """
    return topic_for_instrument(topic='beam_monitor', instrument=instrument)
