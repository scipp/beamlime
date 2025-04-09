# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import scipp as sc

from beamlime.config.env import StreamingEnv
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

# Note: Panel size is fake and does not correspond to production setting
detectors_config = {
    'detectors': {
        'Detector': {
            'detector_name': 'tbl_detector_tpx3',
            'detector_number': sc.arange('yx', 0, 128**2, unit=None).fold(
                dim='yx', sizes={'y': -1, 'x': 128}
            ),
        }
    },
    'fakes': {
        'tbl_detector_tpx3': (1, 128**2),
    },
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
        ): 'tbl_detector_tpx3',
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


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'tbl', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='tbl'),
        detectors=_make_tbl_detectors(),
    ),
}
