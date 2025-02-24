"""
Bifrost with all banks merged into a single one.
"""

from typing import NewType

import scipp as sc
from ess.reduce.live import raw
from ess.reduce.nexus import GenericNeXusWorkflow
from ess.reduce.nexus.types import (
    CalibratedBeamline,
    DetectorData,
    Filename,
    NeXusComponent,
    NeXusData,
    NeXusName,
    SampleRun,
)
from ess.reduce.streaming import StreamProcessor
from scippnexus import NXdetector, NXsource, TransformationChain

detector_number = sc.arange('detector_number', 1, 5 * 3 * 9 * 100 + 1, unit=None).fold(
    dim='detector_number', sizes={'analyzer': 5, 'tube': 3, 'sector': 9, 'pixel': 100}
)
start = 123
detectors_config = {'detectors': {}}
# Each NXdetetor is a He3 tube triplet with shape=(3, 100). Detector numbers in triplet
# are *not* consecutive:
# 1...900 with increasing angle (across all sectors)
# 901 is back to first sector and detector, second tube
detectors_config['detectors']['unified_detector'] = {
    'detector_name': 'unified_detector',
    'projection': raw.LogicalView(),
    'detector_number': detector_number,
}

# Plan (later):
# Make a fake NeXus file with unified detector banks. Merge ev44 messages
# from all banks into a single one.
# But that might not work, due to NeXus limitations?


wf = GenericNeXusWorkflow(run_types=[SampleRun], monitor_types=[])
wf[NeXusComponent[NXsource, SampleRun]] = sc.DataGroup(
    depends_on=TransformationChain(parent='', value='.')
)
detector_names = [
    f'{123+4*(analyzer-1)+5*4*(sector-1)+sector-1}_channel_{sector}_{analyzer}_triplet'
    for analyzer in range(1, 6)
    for sector in range(1, 10)
]


def combine_banks(*bank: sc.DataArray) -> sc.DataArray:
    return (
        sc.concat(bank, dim='')
        .fold('', sizes={'analyzer': 5, 'sector': 9})
        .rename_dims(dim_0='tube', dim_1='pixel')
        .transpose(('analyzer', 'tube', 'sector', 'pixel'))
    )


wf[CalibratedBeamline[SampleRun]] = (
    wf[CalibratedBeamline[SampleRun]]
    .map({NeXusName[NXdetector]: detector_names})
    .reduce(func=combine_banks)
)

SpectrumView = NewType('SpectrumView', sc.DataArray)


def make_spectrum_view(data: DetectorData[SampleRun]) -> SpectrumView:
    # Should be 700, need to increase message size limit.
    edges = sc.linspace('event_time_offset', 0, 71_000_000, num=101, unit='ns')
    return (
        data.bins.concat('pixel')
        .flatten(to='analyzer/tube/sector')
        .hist(event_time_offset=edges)
        .drop_coords(('gravity', 'source_position', 'sample_position'))
        .assign_coords(event_time_offset=edges.to(unit='ms'))
    )


filename = '/home/simon/instruments/bifrost/BIFROST_20240905T122604.h5'
wf[Filename[SampleRun]] = filename

wf.insert(make_spectrum_view)


def _make_processor():
    return StreamProcessor(
        wf,
        dynamic_keys=(NeXusData[NXdetector, SampleRun],),
        accumulators=(SpectrumView,),
        target_keys=(SpectrumView,),
    )


def make_stream_processors():
    return {
        'unified_detector': _make_processor(),
    }


source_to_key = {'unified_detector': NeXusData[NXdetector, SampleRun]}
