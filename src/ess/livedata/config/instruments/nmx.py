# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc

from beamlime.config import Instrument, instrument_registry
from beamlime.config.env import StreamingEnv
from beamlime.handlers.detector_data_handler import (
    DetectorLogicalView,
    LogicalViewConfig,
)
from beamlime.handlers.monitor_data_handler import register_monitor_workflows
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

instrument = Instrument(name='nmx')
instrument_registry.register(instrument)
register_monitor_workflows(instrument=instrument, source_names=['monitor1', 'monitor2'])

# TODO Unclear if this is transposed or not. Wait for updated files.
dim = 'detector_number'
sizes = {'x': 1280, 'y': 1280}
for panel in range(3):
    instrument.add_detector(
        f'detector_panel_{panel}',
        detector_number=sc.arange(
            'detector_number', panel * 1280**2 + 1, (panel + 1) * 1280**2 + 1, unit=None
        ).fold(dim=dim, sizes=sizes),
    )
_nmx_panels_config = LogicalViewConfig(
    name='panel_xy',
    title='Detector counts',
    description='Detector counts per pixel.',
    source_names=instrument.detector_names,
)
_nmx_panels_view = DetectorLogicalView(instrument=instrument, config=_nmx_panels_config)

detectors_config = {
    'fakes': {
        f'detector_panel_{i}': (i * 1280**2 + 1, (i + 1) * 1280**2) for i in range(3)
    },
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


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'nmx', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='nmx'),
        detectors=_make_nmx_detectors(),
    ),
}
