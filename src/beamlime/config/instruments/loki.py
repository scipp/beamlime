# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from ess.loki.live import _configured_Larmor_AgBeh_workflow
from ess.reduce.nexus.types import NeXusData, NeXusDetectorName, SampleRun
from ess.reduce.streaming import StreamProcessor
from ess.sans.types import (
    Denominator,
    Filename,
    Incident,
    IofQ,
    Numerator,
    ReducedQ,
    Transmission,
)
from scippnexus import NXdetector

from beamlime.config.env import StreamingEnv
from beamlime.handlers.detector_data_handler import get_nexus_geometry_filename
from beamlime.handlers.workflow_manager import processor_factory
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

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
    'fakes': {
        'loki_detector_0': (1, 802816),
        'loki_detector_1': (802817, 1032192),
        'loki_detector_2': (1032193, 1204224),
        'loki_detector_3': (1204225, 1433600),
        'loki_detector_4': (1433601, 1605632),
        'loki_detector_5': (1605633, 2007040),
        'loki_detector_6': (2007041, 2465792),
        'loki_detector_7': (2465793, 2752512),
        'loki_detector_8': (2752513, 3211264),
    },
}

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


@processor_factory.register(name='I(Q)', source_names=source_names)
def _i_of_q(source_name: str) -> StreamProcessor:
    wf = _configured_Larmor_AgBeh_workflow()
    wf[Filename[SampleRun]] = get_nexus_geometry_filename('loki')
    wf[NeXusDetectorName] = source_name
    return StreamProcessor(
        wf,
        dynamic_keys=(
            NeXusData[NXdetector, SampleRun],
            NeXusData[Incident, SampleRun],
            NeXusData[Transmission, SampleRun],
        ),
        target_keys=(IofQ[SampleRun],),
        accumulators=(ReducedQ[SampleRun, Numerator], ReducedQ[SampleRun, Denominator]),
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
    'monitor1': NeXusData[Incident, SampleRun],
    'monitor2': NeXusData[Transmission, SampleRun],
}
f144_attribute_registry = {}


def _make_loki_detectors() -> StreamLUT:
    """
    Loki detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    return {
        InputStreamKey(
            topic=f'loki_detector_bank{bank}', source_name='caen'
        ): f'loki_detector_{bank}'
        for bank in range(9)
    }


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'loki', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='loki'),
        detectors=_make_loki_detectors(),
    ),
}
