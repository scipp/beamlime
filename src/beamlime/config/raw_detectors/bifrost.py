"""
Bifrost with all banks merged into a single one.
"""

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
from scippnexus import NXdetector

from beamlime.handlers.detector_data_handler import get_nexus_geometry_filename
from beamlime.handlers.workflow_manager import DynamicWorkflow


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


def _make_spectrum_view(data: DetectorData[SampleRun]) -> SpectrumView:
    edges = sc.linspace('event_time_offset', 0, 71_000_000, num=701, unit='ns')
    # Combine 10 pixels into 1, so we have tubes with 10 pixels each
    return (
        data.fold('pixel', sizes={'pixel': 10, 'subpixel': -1})
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

_reduction_workflow.insert(_make_spectrum_view)
_reduction_workflow.insert(_make_counts_per_angle)


def dynamic_workflows() -> dict[str, DynamicWorkflow]:
    return {
        'testing': DynamicWorkflow(
            workflow=_reduction_workflow.copy(),
            dynamic_keys=(NeXusData[NXdetector, SampleRun],),
            context_keys=(DetectorRotation,),
            target_keys=(SpectrumView, CountsPerAngle),
            accumulators=(SpectrumView, CountsPerAngle),
        )
    }


source_names = ('unified_detector',)
source_to_key = {
    'unified_detector': NeXusData[NXdetector, SampleRun],
    'detector_rotation': DetectorRotation,
}

f144_attribute_registry = {
    'detector_rotation': {'units': 'deg'},
}
