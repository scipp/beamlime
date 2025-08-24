# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc

from beamlime.config import Instrument, instrument_registry
from beamlime.config.env import StreamingEnv
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

instrument = Instrument(name='nmx')
instrument_registry.register(instrument)

# TODO Unclear if this is transposed or not. Wait for updated files.
dim = 'detector_number'
sizes = {'y': 1280, 'x': 1280}

_whole_panel = sc.arange(dim, 0 * 1280**2 + 1, 1 * 1280**2 + 1, unit=None).fold(
    dim=dim, sizes=sizes
)

detectors_config = {
    'detectors': {
        'ProtoType Panel (0, 0)': {
            'detector_name': 'nmx_detector_p1',
            'detector_number': _whole_panel,  # ['x', :640]['y', :640],
        }
        # 'Panel 0': {
        #     'detector_name': 'detector_panel_0',
        #     'detector_number': sc.arange(
        #         dim, 0 * 1280**2 + 1, 1 * 1280**2 + 1, unit=None
        #     ).fold(dim=dim, sizes=sizes),
        # },
        # 'Panel 1': {
        #     'detector_name': 'detector_panel_1',
        #     'detector_number': sc.arange(
        #         dim, 1 * 1280**2 + 1, 2 * 1280**2 + 1, unit=None
        #     ).fold(dim=dim, sizes=sizes),
        # },
        # 'Panel 2': {
        #     'detector_name': 'detector_panel_2',
        #     'detector_number': sc.arange(
        #         dim, 2 * 1280**2 + 1, 3 * 1280**2 + 1, unit=None
        #     ).fold(dim=dim, sizes=sizes),
        # },
    },
    'fakes': {
        'nmx_detector_p1': (0 * 1280**2 + 1, 1 * 1280**2 + 1),
        # 'nmx_detector_p2': (0 * 1280**2 + 1, 1 * 1280**2 + 1),
        # 'nmx_detector_p3': (0 * 1280**2 + 1, 1 * 1280**2 + 1),
        # 'nmx_detector_p4': (0 * 1280**2 + 1, 1 * 1280**2 + 1),
    },
    # {
    #     f'detector_panel_{i}': (i * 1280**2 + 1, (i + 1) * 1280**2) for i in range(3)
    # },
}


def _make_nmx_detectors() -> StreamLUT:
    """
    NMX detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(
            topic='nmx_detector_p1',  # Kafka topic for NMX detector
            source_name='nmx',  # Kafka message source_name for NMX
        ): 'nmx_detector'  # Mapped key
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
