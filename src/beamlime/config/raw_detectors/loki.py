# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import sciline
from ess.reduce.nexus.types import NeXusData, SampleRun
from ess.reduce.streaming import StreamProcessor
from scippnexus import NXdetector

from beamlime.handlers.workflow_manager import processor_factory

_res_scale = 12

detectors_config = {
    'detectors': {
        'Rear-detector': {
            'detector_name': 'loki_detector_0',
            'resolution': {'y': 12 * _res_scale, 'x': 12 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        # First window frame
        'loki_detector_1': {
            'detector_name': 'loki_detector_1',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_2': {
            'detector_name': 'loki_detector_2',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_3': {
            'detector_name': 'loki_detector_3',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_4': {
            'detector_name': 'loki_detector_4',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        # Second window frame
        'loki_detector_5': {
            'detector_name': 'loki_detector_5',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_6': {
            'detector_name': 'loki_detector_6',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_7': {
            'detector_name': 'loki_detector_7',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_8': {
            'detector_name': 'loki_detector_8',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': 'cylindrical',
        },
    },
}


@processor_factory.register(name='I(Q)')
def _i_of_q() -> StreamProcessor:
    return StreamProcessor(
        sciline.Pipeline,
        dynamic_keys=(NeXusData[NXdetector, SampleRun],),
        target_keys=(),
        accumulators=(),
    )


source_names = (
    'loki_detector_0',
    'loki_detector_1',
    'loki_detector_2',
    'loki_detector_3',
    'loki_detector_4',
    'loki_detector_5',
    'loki_detector_6',
    'loki_detector_7',
    'loki_detector_8',
)
source_to_key = {
    'loki_detector_0': NeXusData[NXdetector, SampleRun],
    'loki_detector_1': NeXusData[NXdetector, SampleRun],
    'loki_detector_2': NeXusData[NXdetector, SampleRun],
    'loki_detector_3': NeXusData[NXdetector, SampleRun],
    'loki_detector_4': NeXusData[NXdetector, SampleRun],
    'loki_detector_5': NeXusData[NXdetector, SampleRun],
    'loki_detector_6': NeXusData[NXdetector, SampleRun],
    'loki_detector_7': NeXusData[NXdetector, SampleRun],
    'loki_detector_8': NeXusData[NXdetector, SampleRun],
}
f144_attribute_registry = {}
