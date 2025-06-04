# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from typing import NewType

import ess.powder.types  # noqa: F401
import scipp as sc
from ess import dream, powder
from ess.dream.workflow import DreamPowderWorkflow
from ess.reduce.nexus.types import (
    DetectorData,
    Filename,
    NeXusData,
    NeXusName,
    SampleRun,
)
from ess.reduce.streaming import StreamProcessor
from scippnexus import NXdetector

from beamlime.config import Instrument
from beamlime.config.env import StreamingEnv
from beamlime.config.models import Parameter, ParameterType
from beamlime.handlers.detector_data_handler import get_nexus_geometry_filename
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

instrument = Instrument(
    name='dream',
    source_to_key={
        'mantle_detector': NeXusData[NXdetector, SampleRun],
        'endcap_backward_detector': NeXusData[NXdetector, SampleRun],
        'endcap_forward_detector': NeXusData[NXdetector, SampleRun],
        'high_resolution_detector': NeXusData[NXdetector, SampleRun],
        'monitor1': NeXusData[powder.types.CaveMonitor, SampleRun],
    },
)


def _get_mantle_front_layer(da: sc.DataArray) -> sc.DataArray:
    return (
        da.fold(
            dim=da.dim,
            sizes={'wire': 32, 'module': 5, 'segment': 6, 'strip': 256, 'counter': 2},
        )
        .transpose(('wire', 'module', 'segment', 'counter', 'strip'))['wire', 0]
        .flatten(('module', 'segment', 'counter'), to='mod/seg/cntr')
    )


_res_scale = 8
pixel_noise = sc.scalar(4.0, unit='mm')

# Order in 'resolution' matters so plots have X as horizontal axis and Y as vertical.
detectors_config = {
    'detectors': {
        'endcap_backward': {
            'detector_name': 'endcap_backward_detector',
            'resolution': {'y': 30 * _res_scale, 'x': 20 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': pixel_noise,
        },
        'endcap_forward': {
            'detector_name': 'endcap_forward_detector',
            'resolution': {'y': 20 * _res_scale, 'x': 20 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': pixel_noise,
        },
        'High-Res': {
            'detector_name': 'high_resolution_detector',
            'resolution': {'y': 20 * _res_scale, 'x': 20 * _res_scale},
            'projection': 'xy_plane',
            'pixel_noise': pixel_noise,
        },
        # We use the arc length instead of phi as it makes it easier to get a correct
        # aspect ratio for the plot if both axes have the same unit.
        'mantle_projection': {
            'detector_name': 'mantle_detector',
            'resolution': {'arc_length': 10 * _res_scale, 'z': 40 * _res_scale},
            'projection': 'cylinder_mantle_z',
            'pixel_noise': pixel_noise,
        },
        # Different view of the same detector, showing just the front layer instead of
        # a projection.
        'mantle_front_layer': {
            'detector_name': 'mantle_detector',
            'detector_number': sc.arange('dummy', 229377, 720897, unit=None),
            'projection': _get_mantle_front_layer,
        },
    },
    'fakes': {
        'mantle_detector': (229377, 720896),
        'endcap_backward_detector': (71618, 229376),
        'endcap_forward_detector': (1, 71680),
        'high_resolution_detector': (1122337, 1523680),  # Note: Not consecutive!
        'sans_detector': (0, 0),  # TODO
    },
}


def _make_dream_detectors() -> StreamLUT:
    """
    Dream detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    mapping = {
        'bwec': 'endcap_backward',
        'fwec': 'endcap_forward',
        'hr': 'high_resolution',
        'mantle': 'mantle',
        'sans': 'sans',
    }
    return {
        InputStreamKey(
            topic=f'dream_detector_{key}', source_name='dream'
        ): f'{value}_detector'
        for key, value in mapping.items()
    }


_reduction_workflow = DreamPowderWorkflow(
    run_norm=powder.RunNormalization.monitor_integrated
)

_source_names = [
    'mantle_detector',
    'endcap_backward_detector',
    'endcap_forward_detector',
    'high_resolution_detector',
]

TotalCounts = NewType('TotalCounts', sc.DataArray)


def _total_counts(data: DetectorData[SampleRun]) -> TotalCounts:
    """Dummy provider for some plottable result of total counts."""
    return TotalCounts(
        data.hist(
            event_time_offset=sc.linspace(
                'event_time_offset', 0, 71_000_000, num=1000, unit='ns'
            ),
            dim=data.dims,
        )
    )


_reduction_workflow.insert(_total_counts)
_reduction_workflow[powder.types.DspacingBins] = sc.linspace(
    dim='dspacing',
    start=0.1,
    stop=5.0,
    num=500,
    unit='angstrom',
)
_reduction_workflow[powder.types.TwoThetaBins] = sc.linspace(
    dim="two_theta", unit="rad", start=0.0, stop=3.1416, num=201
)
_reduction_workflow[powder.types.CalibrationData] = None
_reduction_workflow = powder.with_pixel_mask_filenames(_reduction_workflow, [])
_reduction_workflow[dream.InstrumentConfiguration] = (
    dream.InstrumentConfiguration.high_flux
)
_reduction_workflow[powder.types.UncertaintyBroadcastMode] = (
    powder.types.UncertaintyBroadcastMode.drop
)
_reduction_workflow[powder.types.KeepEvents[SampleRun]] = powder.types.KeepEvents[
    SampleRun
](False)

# dream-no-shape is a much smaller file without pixel_shape, which is not needed for
# data reduction.
geometry_file_param = Parameter(
    name='GeometryFile',
    description='NeXus file containing instrument geometry and other static data.',
    param_type=ParameterType.STRING,
    default=str(get_nexus_geometry_filename('dream-no-shape')),
)


@instrument.register_workflow(
    name='Powder reduction',
    source_names=_source_names,
    parameters=[geometry_file_param],
)
def _powder_workflow(source_name: str, GeometryFile: str) -> StreamProcessor:
    wf = _reduction_workflow.copy()
    wf[NeXusName[NXdetector]] = source_name
    wf[Filename[SampleRun]] = GeometryFile
    return StreamProcessor(
        wf,
        dynamic_keys=(
            NeXusData[NXdetector, SampleRun],
            NeXusData[powder.types.CaveMonitor, SampleRun],
        ),
        target_keys=(
            powder.types.FocussedDataDspacing[SampleRun],
            powder.types.FocussedDataDspacingTwoTheta[SampleRun],
        ),
        accumulators=(
            powder.types.ReducedCountsDspacing[SampleRun],
            powder.types.WavelengthMonitor[SampleRun, powder.types.CaveMonitor],
        ),
    )


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'dream', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='dream'),
        detectors=_make_dream_detectors(),
    ),
}
