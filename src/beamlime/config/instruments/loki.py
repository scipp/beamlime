# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import ess.loki.live  # noqa: F401
import scipp as sc
from ess import loki
from ess.reduce.nexus.types import NeXusData, NeXusDetectorName, SampleRun
from ess.reduce.streaming import StreamProcessor
from ess.sans import types as params
from ess.sans.types import (
    Filename,
    Incident,
    IofQ,
    Numerator,
    ReducedQ,
    Transmission,
)
from scippnexus import NXdetector

from beamlime.config import Instrument
from beamlime.config.env import StreamingEnv
from beamlime.config.models import Parameter, ParameterType
from beamlime.handlers.detector_data_handler import get_nexus_geometry_filename
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

_source_names = (
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


qbins_param = Parameter(
    name='QBins',
    description='Number of Q bins',
    param_type=ParameterType.INT,
    default=20,
)
wav_min_param = Parameter(
    name='WavelengthMin',
    unit='angstrom',
    description='Minimum wavelength',
    param_type=ParameterType.FLOAT,
    default=1.0,
)
wav_max_param = Parameter(
    name='WavelengthMax',
    unit='angstrom',
    description='Maximum wavelength',
    param_type=ParameterType.FLOAT,
    default=13.0,
)
wav_bins_param = Parameter(
    name='WavelengthBins',
    unit=None,
    description='Number of wavelength bins',
    param_type=ParameterType.INT,
    default=100,
)
use_transmission_run = Parameter(
    name='UseTransmissionRun',
    unit=None,
    description='Use transmission run instead of monitor readings of sample run',
    param_type=ParameterType.BOOL,
    default=False,
)

# Created once outside workflow wrappers since this configures some files from pooch
# where a checksum is needed, which takes significant time.
_base_workflow = loki.live._configured_Larmor_AgBeh_workflow()
_base_workflow[Filename[SampleRun]] = get_nexus_geometry_filename('loki')

instrument = Instrument(
    name='loki',
    source_to_key={
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
    },
)


def _transmission_from_current_run(
    data: params.CleanMonitor[SampleRun, params.MonitorType],
) -> params.CleanMonitor[params.TransmissionRun[SampleRun], params.MonitorType]:
    return data


_dynamic_keys = (
    NeXusData[NXdetector, SampleRun],
    NeXusData[Incident, SampleRun],
    NeXusData[Transmission, SampleRun],
)
_accumulators = (
    ReducedQ[SampleRun, Numerator],
    params.CleanMonitor[SampleRun, Incident],
    params.CleanMonitor[SampleRun, Transmission],
)


@instrument.register_workflow(name='I(Q)', source_names=_source_names)
def _i_of_q(source_name: str) -> StreamProcessor:
    wf = _base_workflow.copy()
    wf[NeXusDetectorName] = source_name
    return StreamProcessor(
        wf,
        dynamic_keys=_dynamic_keys,
        target_keys=(IofQ[SampleRun],),
        accumulators=_accumulators,
    )


# Note: For now we are setting up a manual parameter mapping. In the future we may want
# auto-generate this, e.g., based on the widget-related components in ess.reduce.
# On the other hand, a curated list of parameters may be useful and advantageous for
# a simplified workflow control for live reduction.
@instrument.register_workflow(
    name='I(Q) with params',
    source_names=_source_names,
    parameters=(
        qbins_param,
        wav_min_param,
        wav_max_param,
        wav_bins_param,
        use_transmission_run,
    ),
)
def _i_of_q_with_params(
    source_name: str,
    QBins: int,
    WavelengthMin: float,
    WavelengthMax: float,
    WavelengthBins: int,
    UseTransmissionRun: bool,
) -> StreamProcessor:
    wf = _base_workflow.copy()
    wf[NeXusDetectorName] = source_name

    wf[params.QBins] = sc.linspace(
        dim='Q', start=0.01, stop=0.3, num=QBins + 1, unit='1/angstrom'
    )
    wf[params.WavelengthBins] = sc.linspace(
        dim='wavelength',
        start=WavelengthMin,
        stop=WavelengthMax,
        num=WavelengthBins + 1,
        unit='angstrom',
    )
    if not UseTransmissionRun:
        target_keys = (IofQ[SampleRun], params.TransmissionFraction[SampleRun])
        wf.insert(_transmission_from_current_run)
    else:
        # Transmission fraction is static, do not display
        target_keys = (IofQ[SampleRun],)
    return StreamProcessor(
        wf,
        dynamic_keys=_dynamic_keys,
        target_keys=target_keys,
        accumulators=_accumulators,
    )


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
