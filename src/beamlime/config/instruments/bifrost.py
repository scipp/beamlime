"""
Bifrost with all banks merged into a single one.
"""

from collections.abc import Generator
from typing import NewType

import scipp as sc
from ess import bifrost
from ess.reduce.nexus.types import (
    CalibratedBeamline,
    DetectorData,
    Filename,
    NeXusData,
    NeXusName,
    SampleRun,
)
from ess.reduce.streaming import StreamProcessor
from scippnexus import NXdetector

from beamlime.config.env import StreamingEnv
from beamlime.config.models import Parameter, ParameterType
from beamlime.handlers.detector_data_handler import get_nexus_geometry_filename
from beamlime.handlers.stream_processor_factory import StreamProcessorFactory
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping


def _to_flat_detector_view(da: sc.DataArray) -> sc.DataArray:
    return da.flatten(dims=('analyzer', 'tube'), to='analyzer/tube').flatten(
        dims=('sector', 'pixel'), to='sector/pixel'
    )


detector_number = sc.arange('detector_number', 1, 5 * 3 * 9 * 100 + 1, unit=None).fold(
    dim='detector_number', sizes={'analyzer': 5, 'tube': 3, 'sector': 9, 'pixel': 100}
)
detectors_config = {'detectors': {}}
# Each NXdetetor is a He3 tube triplet with shape=(3, 100). Detector numbers in triplet
# are *not* consecutive:
# 1...900 with increasing angle (across all sectors)
# 901 is back to first sector and detector, second tube
detectors_config['detectors']['unified_detector'] = {
    'detector_name': 'unified_detector',
    'projection': _to_flat_detector_view,
    'detector_number': detector_number,
}


def _bifrost_generator() -> Generator[tuple[str, tuple[int, int]]]:
    # BEWARE! There are gaps in the detector_number per bank, which would usually get
    # dropped when mapping to pixels. BUT we merge banks for Bifrost, before mapping to
    # pixels, so the generated fake events in the wrong bank will end up in the right
    # bank. As a consequence we do not lose any fake events, but the travel over Kafka
    # with the wrong source_name.
    start = 123
    ntube = 3
    for sector in range(1, 10):
        for analyzer in range(1, 6):
            # Note: Actual start is at base + 100 * (sector - 1), but we start earlier
            # to get consistent counts across all banks, relating to comment above.
            base = ntube * 900 * (analyzer - 1)
            yield (
                f'{start}_channel_{sector}_{analyzer}_triplet',
                (base + 1, base + 2700),
            )
            start += 4
        start += 1


detectors_config['fakes'] = dict(_bifrost_generator())

# Would like to use a 2-D scipp.Variable, but GenericNeXusWorkflow does not accept
# detector names as scalar variables.
_detector_names = [
    f'{123+4*(analyzer-1)+(5*4+1)*(sector-1)}_channel_{sector}_{analyzer}_triplet'
    for analyzer in range(1, 6)
    for sector in range(1, 10)
]


def _combine_banks(*bank: sc.DataArray) -> sc.DataArray:
    return (
        sc.concat(bank, dim='')
        .fold('', sizes={'analyzer': 5, 'sector': 9})
        .rename_dims(dim_0='tube', dim_1='pixel')
        .transpose(('analyzer', 'tube', 'sector', 'pixel'))
    )


SpectrumView = NewType('SpectrumView', sc.DataArray)
DetectorRotation = NewType('DetectorRotation', sc.DataArray)
CountsPerAngle = NewType('CountsPerAngle', sc.DataArray)

_SpectrumViewTimeBins = NewType('_SpectrumViewTimeBins', int)
_SpectrumViewPixelsPerTube = NewType('_SpectrumViewPixelsPerTube', int)


def _make_spectrum_view(
    data: DetectorData[SampleRun],
    time_bins: _SpectrumViewTimeBins,
    pixels_per_tube: _SpectrumViewPixelsPerTube,
) -> SpectrumView:
    edges = sc.linspace(
        'event_time_offset', 0, 71_000_000, num=time_bins + 1, unit='ns'
    )
    # Combine, e.g., 10 pixels into 1, so we have tubes with 10 pixels each
    return (
        data.fold('pixel', sizes={'pixel': pixels_per_tube, 'subpixel': -1})
        .drop_coords(tuple(data.coords))
        .bins.concat('subpixel')
        .flatten(to='analyzer/tube/sector/pixel')
        .hist(event_time_offset=edges)
        .assign_coords(event_time_offset=edges.to(unit='ms'))
    )


def _make_counts_per_angle(
    data: DetectorData[SampleRun], rotation: DetectorRotation
) -> CountsPerAngle:
    edges = sc.linspace('angle', 0, 91, num=46, unit='deg')
    da = sc.DataArray(
        sc.zeros(dims=['angle'], shape=[45], unit='counts'), coords={'angle': edges}
    )
    counts = sc.values(data.sum().data)
    if rotation is not None:
        da['angle', rotation.data[-1]] += counts
    return da


_reduction_workflow = bifrost.io.nexus.LoadNeXusWorkflow()
_reduction_workflow[Filename[SampleRun]] = get_nexus_geometry_filename('bifrost')
_reduction_workflow[CalibratedBeamline[SampleRun]] = (
    _reduction_workflow[CalibratedBeamline[SampleRun]]
    .map({NeXusName[NXdetector]: _detector_names})
    .reduce(func=_combine_banks)
)

_reduction_workflow[_SpectrumViewTimeBins] = 500
_reduction_workflow[_SpectrumViewPixelsPerTube] = 10
_reduction_workflow.insert(_make_spectrum_view)
_reduction_workflow.insert(_make_counts_per_angle)

_source_names = ('unified_detector',)

spectrum_view_time_bins_param = Parameter(
    name='SpectrumViewTimeBins',
    description='Number of time bins for the spectrum view.',
    param_type=ParameterType.INT,
    default=500,
)

spectrum_view_pixels_per_tube_param = Parameter(
    name='SpectrumViewPixelsPerTube',
    description='Number of pixels per tube for the spectrum view.',
    param_type=ParameterType.OPTIONS,
    default=10,
    options=[1, 2, 5, 10, 20, 50, 100],  # Must be a divisor of 100
)

processor_factory = StreamProcessorFactory()


@processor_factory.register(
    name='spectrum-view',
    source_names=_source_names,
    parameters=(spectrum_view_time_bins_param, spectrum_view_pixels_per_tube_param),
)
def _spectrum_view(
    SpectrumViewTimeBins: int, SpectrumViewPixelsPerTube: int
) -> StreamProcessor:
    wf = _reduction_workflow.copy()
    wf[_SpectrumViewTimeBins] = SpectrumViewTimeBins
    wf[_SpectrumViewPixelsPerTube] = SpectrumViewPixelsPerTube
    return StreamProcessor(
        wf,
        dynamic_keys=(NeXusData[NXdetector, SampleRun],),
        target_keys=(SpectrumView,),
        accumulators=(SpectrumView,),
    )


@processor_factory.register(name='counts-per-angle', source_names=_source_names)
def _counts_per_angle() -> StreamProcessor:
    return StreamProcessor(
        _reduction_workflow.copy(),
        dynamic_keys=(NeXusData[NXdetector, SampleRun],),
        context_keys=(DetectorRotation,),
        target_keys=(CountsPerAngle,),
        accumulators=(CountsPerAngle,),
    )


@processor_factory.register(name='all', source_names=_source_names)
def _all() -> StreamProcessor:
    return StreamProcessor(
        _reduction_workflow.copy(),
        dynamic_keys=(NeXusData[NXdetector, SampleRun],),
        context_keys=(DetectorRotation,),
        target_keys=(CountsPerAngle, SpectrumView),
        accumulators=(CountsPerAngle, SpectrumView),
    )


source_to_key = {
    'unified_detector': NeXusData[NXdetector, SampleRun],
    'detector_rotation': DetectorRotation,
}

f144_attribute_registry = {
    'detector_rotation': {'units': 'deg'},
}


def _make_bifrost_detectors() -> StreamLUT:
    """
    Bifrost detector mapping.

    Input keys based on
    https://confluence.ess.eu/display/ECDC/Kafka+Topics+Overview+for+Instruments
    """
    # Source names have the format `arc=[0-4];triplet=[0-8]`.
    return {
        InputStreamKey(
            topic='bifrost_detector', source_name=f'arc={arc};triplet={triplet}'
        ): f'arc{arc}_triplet{triplet}'
        for arc in range(5)
        for triplet in range(9)
    }


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'bifrost', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='bifrost'),
        detectors=_make_bifrost_detectors(),
    ),
}
