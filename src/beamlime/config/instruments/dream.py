# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from typing import NewType

import ess.powder.types  # noqa: F401
import pydantic
import scipp as sc
from ess import dream, powder
from ess.dream import DreamPowderWorkflow
from ess.reduce.nexus.types import (
    DetectorData,
    Filename,
    NeXusData,
    NeXusName,
    RunType,
    SampleRun,
    VanadiumRun,
)
from ess.reduce.streaming import StreamProcessor
from scippnexus import NXdetector

from beamlime import parameter_models
from beamlime.config import Instrument, instrument_registry
from beamlime.config.env import StreamingEnv
from beamlime.handlers.detector_data_handler import (
    DetectorLogicalView,
    DetectorProjection,
    LogicalViewConfig,
    get_nexus_geometry_filename,
    make_detector_data_instrument,
)
from beamlime.handlers.monitor_data_handler import make_beam_monitor_instrument
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

_monitor_instrument = make_beam_monitor_instrument(
    name='dream', source_names=['monitor1', 'monitor2']
)
_detector_instrument = make_detector_data_instrument(name='dream')
_detector_instrument.add_detector('mantle_detector')
_detector_instrument.add_detector('endcap_backward_detector')
_detector_instrument.add_detector('endcap_forward_detector')
_detector_instrument.add_detector('high_resolution_detector')
_detector_instrument.add_detector('sans_detector')

instrument_registry.register(instrument)
instrument_registry.register(_monitor_instrument)
instrument_registry.register(_detector_instrument)

_xy_projection = DetectorProjection(
    instrument=_detector_instrument,
    projection='xy_plane',
    resolution={
        'endcap_backward_detector': {'y': 30, 'x': 20},
        'endcap_forward_detector': {'y': 20, 'x': 20},
        'high_resolution_detector': {'y': 20, 'x': 20},
    },
)
_cylinder_projection = DetectorProjection(
    instrument=_detector_instrument,
    projection='cylinder_mantle_z',
    resolution={'mantle_detector': {'arc_length': 10, 'z': 40}},
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


_mantle_front_layer_config = LogicalViewConfig(
    name='mantle_front_layer',
    title='Mantle front layer',
    description='All voxels of the front layer of the mantle detector.',
    source_names=['mantle_detector'],
    transform=_get_mantle_front_layer,
)

_logical_view = DetectorLogicalView(
    instrument=_detector_instrument, config=_mantle_front_layer_config
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


# Normalization to monitors is partially broken due to some wavelength-range checking
# in essdiffraction that does not play with TOA-TOF conversion (I think).
_reduction_workflow = DreamPowderWorkflow(
    run_norm=powder.RunNormalization.proton_charge
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
        data.nanhist(
            event_time_offset=sc.linspace(
                'event_time_offset', 0, 71_000_000, num=1000, unit='ns'
            ),
            dim=data.dims,
        )
    )


def _fake_proton_charge(
    data: powder.types.ReducedCountsDspacing[RunType],
) -> powder.types.AccumulatedProtonCharge[RunType]:
    """
    Fake approximate proton charge for consistent normalization during streaming.

    This is not meant for production, but as a workaround until monitor normalization is
    fixed and/or we have setup a proton-charge stream.
    """
    fake_charge = sc.values(data.data).sum()
    fake_charge.unit = 'counts/µAh'
    return powder.types.AccumulatedProtonCharge[RunType](fake_charge)


_reduction_workflow.insert(_total_counts)
_reduction_workflow.insert(_fake_proton_charge)
_reduction_workflow[powder.types.CalibrationData] = None
_reduction_workflow = powder.with_pixel_mask_filenames(_reduction_workflow, [])
_reduction_workflow[powder.types.UncertaintyBroadcastMode] = (
    powder.types.UncertaintyBroadcastMode.drop
)
_reduction_workflow[powder.types.KeepEvents[SampleRun]] = powder.types.KeepEvents[
    SampleRun
](False)

# dream-no-shape is a much smaller file without pixel_shape, which is not needed
# for data reduction.
_reduction_workflow[Filename[SampleRun]] = get_nexus_geometry_filename('dream-no-shape')


class InstrumentConfiguration(pydantic.BaseModel):
    value: dream.InstrumentConfiguration = pydantic.Field(
        default=dream.InstrumentConfiguration.high_flux_BC240,
        description='Chopper settings determining TOA to TOF conversion.',
    )

    @pydantic.model_validator(mode="after")
    def check_high_resolution_not_implemented(self):
        if self.value == dream.InstrumentConfiguration.high_resolution:
            raise pydantic.ValidationError.from_exception_data(
                "ValidationError",
                [
                    {
                        "type": "value_error",
                        "loc": ("value",),
                        "input": self.value,
                        "ctx": {
                            "error": "The 'high_resolution' setting is not available."
                        },
                    }
                ],
            )
        return self


class PowderWorkflowParams(pydantic.BaseModel):
    dspacing_edges: parameter_models.DspacingEdges = pydantic.Field(
        title='d-spacing bins',
        description='Define the bin edges for binning in d-spacing.',
        default=parameter_models.DspacingEdges(
            start=0.4,
            stop=3.5,
            num_bins=500,
            unit=parameter_models.DspacingUnit.ANGSTROM,
        ),
    )
    two_theta_edges: parameter_models.TwoTheta = pydantic.Field(
        title='Two-theta bins',
        description='Define the bin edges for binning in 2-theta.',
        default=parameter_models.TwoTheta(
            start=0.4, stop=3.1415, num_bins=100, unit=parameter_models.AngleUnit.RADIAN
        ),
    )
    wavelength_range: parameter_models.WavelengthRange = pydantic.Field(
        title='Wavelength range',
        description='Range of wavelengths to include in the reduction.',
        default=parameter_models.WavelengthRange(
            start=1.1, stop=4.5, unit=parameter_models.WavelengthUnit.ANGSTROM
        ),
    )
    instrument_configuration: InstrumentConfiguration = pydantic.Field(
        title='Instrument configuration',
        description='Chopper settings determining TOA to TOF conversion.',
        default=InstrumentConfiguration(),
    )


@instrument.register_workflow(
    name='powder_reduction',
    version=1,
    title='Powder reduction',
    description='Powder reduction without vanadium normalization.',
    source_names=_source_names,
)
def _powder_workflow(source_name: str, params: PowderWorkflowParams) -> StreamProcessor:
    wf = _reduction_workflow.copy()
    wf[NeXusName[NXdetector]] = source_name
    wf[dream.InstrumentConfiguration] = params.instrument_configuration.value
    wmin = params.wavelength_range.get_start()
    wmax = params.wavelength_range.get_stop()
    wf[powder.types.WavelengthMask] = lambda w: (w < wmin) | (w > wmax)
    wf[powder.types.TwoThetaBins] = params.two_theta_edges.get_edges()
    wf[powder.types.DspacingBins] = params.dspacing_edges.get_edges()
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


@instrument.register_workflow(
    name='powder_reduction_with_vanadium',
    version=1,
    title='Powder reduction (with vanadium)',
    description='Powder reduction with vanadium normalization.',
    source_names=_source_names,
)
def _powder_workflow_with_vanadium(
    source_name: str, params: PowderWorkflowParams
) -> StreamProcessor:
    wf = _reduction_workflow.copy()
    wf[NeXusName[NXdetector]] = source_name
    wf[Filename[VanadiumRun]] = '268227_00024779_Vana_inc_BC_offset_240_deg_wlgth.hdf'
    wf[dream.InstrumentConfiguration] = params.instrument_configuration.value
    wmin = params.wavelength_range.get_start()
    wmax = params.wavelength_range.get_stop()
    wf[powder.types.WavelengthMask] = lambda w: (w < wmin) | (w > wmax)
    wf[powder.types.TwoThetaBins] = params.two_theta_edges.get_edges()
    wf[powder.types.DspacingBins] = params.dspacing_edges.get_edges()
    return StreamProcessor(
        wf,
        dynamic_keys=(
            NeXusData[NXdetector, SampleRun],
            NeXusData[powder.types.CaveMonitor, SampleRun],
        ),
        target_keys=(
            powder.types.FocussedDataDspacing[SampleRun],
            powder.types.FocussedDataDspacingTwoTheta[SampleRun],
            powder.types.IofDspacing[SampleRun],
            powder.types.IofDspacingTwoTheta[SampleRun],
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
