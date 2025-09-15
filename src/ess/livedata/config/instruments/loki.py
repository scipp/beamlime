# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pydantic
import sciline
import sciline.typing
from scippnexus import NXdetector

import ess.loki.live  # noqa: F401
from ess import loki
from ess.livedata import parameter_models
from ess.livedata.config import Instrument, instrument_registry
from ess.livedata.config.env import StreamingEnv
from ess.livedata.handlers.detector_data_handler import (
    DetectorProjection,
    get_nexus_geometry_filename,
)
from ess.livedata.handlers.monitor_data_handler import register_monitor_workflows
from ess.livedata.handlers.stream_processor_workflow import StreamProcessorWorkflow
from ess.livedata.kafka import InputStreamKey, StreamLUT, StreamMapping
from ess.reduce.nexus.types import NeXusData, NeXusDetectorName, SampleRun
from ess.sans import types as params
from ess.sans.types import (
    Filename,
    Incident,
    IofQ,
    Numerator,
    ReducedQ,
    Transmission,
)

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

detectors_config = {
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


class SansWorkflowParams(pydantic.BaseModel):
    q_edges: parameter_models.QEdges = pydantic.Field(
        title='Q bins',
        description='Define the bin edges for binning in Q.',
        default=parameter_models.QEdges(
            start=0.01,
            stop=0.3,
            num_bins=20,
            unit=parameter_models.QUnit.INVERSE_ANGSTROM,
        ),
    )
    wavelength_edges: parameter_models.WavelengthEdges = pydantic.Field(
        title='Wavelength bins',
        description='Define the bin edges for binning in wavelength.',
        default=parameter_models.WavelengthEdges(
            start=1.0,
            stop=13.0,
            num_bins=100,
            unit=parameter_models.WavelengthUnit.ANGSTROM,
        ),
    )
    use_transmission_run: bool = pydantic.Field(
        title='Use transmission run',
        description='Use transmission run instead of monitor readings of sample run',
        default=False,
    )


# Created once outside workflow wrappers since this configures some files from pooch
# where a checksum is needed, which takes significant time.
_base_workflow = loki.live._configured_Larmor_AgBeh_workflow()
_base_workflow[Filename[SampleRun]] = get_nexus_geometry_filename('loki')

instrument = Instrument(name='loki')
register_monitor_workflows(instrument=instrument, source_names=['monitor1', 'monitor2'])
instrument_registry.register(instrument)
for bank in range(9):
    instrument.add_detector(f'loki_detector_{bank}')
_xy_projection = DetectorProjection(
    instrument=instrument,
    projection='xy_plane',
    pixel_noise='cylindrical',
    resolution={
        'loki_detector_0': {'y': 12, 'x': 12},
        # First window frame
        'loki_detector_1': {'y': 3, 'x': 9},
        'loki_detector_2': {'y': 9, 'x': 3},
        'loki_detector_3': {'y': 3, 'x': 9},
        'loki_detector_4': {'y': 9, 'x': 3},
        # Second window frame
        'loki_detector_5': {'y': 3, 'x': 9},
        'loki_detector_6': {'y': 9, 'x': 3},
        'loki_detector_7': {'y': 3, 'x': 9},
        'loki_detector_8': {'y': 9, 'x': 3},
    },
    resolution_scale=12,
)


def _transmission_from_current_run(
    data: params.CleanMonitor[SampleRun, params.MonitorType],
) -> params.CleanMonitor[params.TransmissionRun[SampleRun], params.MonitorType]:
    return data


def _dynamic_keys(source_name: str) -> dict[str, sciline.typing.Key]:
    return {
        source_name: NeXusData[NXdetector, SampleRun],
        'monitor1': NeXusData[Incident, SampleRun],
        'monitor2': NeXusData[Transmission, SampleRun],
    }


_accumulators = (
    ReducedQ[SampleRun, Numerator],
    params.CleanMonitor[SampleRun, Incident],
    params.CleanMonitor[SampleRun, Transmission],
)


@instrument.register_workflow(
    name='i_of_q',
    version=1,
    title='I(Q)',
    source_names=instrument.detector_names,
    aux_source_names=['monitor1', 'monitor2'],
)
def _i_of_q(source_name: str) -> StreamProcessorWorkflow:
    wf = _base_workflow.copy()
    wf[NeXusDetectorName] = source_name
    return StreamProcessorWorkflow(
        wf,
        dynamic_keys=_dynamic_keys(source_name),
        target_keys=(IofQ[SampleRun],),
        accumulators=_accumulators,
    )


@instrument.register_workflow(
    name='i_of_q_with_params',
    version=1,
    title='I(Q) with params',
    description='I(Q) reduction with configurable parameters.',
    source_names=instrument.detector_names,
    aux_source_names=['monitor1', 'monitor2'],
)
def _i_of_q_with_params(
    source_name: str, params: SansWorkflowParams
) -> StreamProcessorWorkflow:
    wf = _base_workflow.copy()
    wf[NeXusDetectorName] = source_name

    wf[params.QBins] = params.q_edges.get_edges()
    wf[params.WavelengthBins] = params.wavelength_edges.get_edges()

    if not params.use_transmission_run:
        target_keys = (IofQ[SampleRun], params.TransmissionFraction[SampleRun])
        wf.insert(_transmission_from_current_run)
    else:
        # Transmission fraction is static, do not display
        target_keys = (IofQ[SampleRun],)
    return StreamProcessorWorkflow(
        wf,
        dynamic_keys=_dynamic_keys(source_name),
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
