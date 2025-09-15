# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import scipp as sc

from ess.livedata.config import Instrument, instrument_registry
from ess.livedata.config.env import StreamingEnv
from ess.livedata.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

instrument = Instrument(name='odin')
instrument_registry.register(instrument)

# Note: Panel size is fake and does not correspond to production setting
detectors_config = {
    'detectors': {
        'Detector': {
            'detector_name': 'odin_detector',
            'detector_number': sc.arange('yx', 0, 128**2, unit=None).fold(
                dim='yx', sizes={'y': -1, 'x': 128}
            ),
        }
    },
    'fakes': {
        'odin_detector': (1, 128**2),
    },
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


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'odin', detector_names=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='odin'),
        detectors=_make_odin_detectors(),
    ),
}
