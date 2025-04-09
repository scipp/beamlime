# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Detector configuration for a dummy instrument used for development and testing.
"""

import scipp as sc

from beamlime.config.env import StreamingEnv
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

detectors_config = {
    'detectors': {
        'Panel 0': {
            'detector_name': 'panel_0',
            'detector_number': sc.arange('yx', 0, 128**2, unit=None).fold(
                dim='yx', sizes={'y': -1, 'x': 128}
            ),
        },
    },
    'fakes': {
        'panel_0': (1, 128**2),
    },
}


def _make_dummy_detectors() -> StreamLUT:
    """Dummy detector mapping."""
    return {InputStreamKey(topic='dummy_detector', source_name='panel_0'): 'panel_0'}


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'dummy', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='dummy'),
        detectors=_make_dummy_detectors(),
    ),
}
