# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping


def _make_odin_detectors() -> StreamLUT:
    """
    Odin detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(topic='odin_detector', source_name='timepix3'): 'odin_detector'
    }


stream_mapping_dev = make_dev_stream_mapping('odin')
stream_mapping = StreamMapping(
    **make_common_stream_mapping_inputs(instrument='odin'),
    detectors=_make_odin_detectors(),
)
