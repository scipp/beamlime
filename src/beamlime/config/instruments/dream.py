# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from typing import NewType

import sciline
import scipp as sc
from ess.reduce.streaming import StreamProcessor


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
}


# Below is a dummy workflow for early dev and testing purposes.
# This will be replaced by a real workflow provided by the ess.dream package.
RawDetectorData = NewType('RawDetectorData', sc.DataArray)
DetectorData = NewType('DetectorData', sc.DataArray)
RawMon1 = NewType('RawMon1', sc.DataArray)
RawMon2 = NewType('RawMon2', sc.DataArray)
Mon1 = NewType('Mon1', sc.DataArray)
Mon2 = NewType('Mon2', sc.DataArray)
TransmissionFraction = NewType('TransmissionFraction', sc.DataArray)
IofQ = NewType('IofQ', sc.DataArray)


def process_detector_data(raw_detector_data: RawDetectorData) -> DetectorData:
    return raw_detector_data.bins.concat().hist(
        event_time_offset=sc.linspace(
            'event_time_offset', 0, 71_000_000, num=100, unit='ns'
        )
    )


def process_mon1(raw_mon1: RawMon1) -> Mon1:
    return raw_mon1.sum()


def process_mon2(raw_mon2: RawMon2) -> Mon2:
    return raw_mon2.sum()


def transmission_fraction(mon1: Mon1, mon2: Mon2) -> TransmissionFraction:
    return mon2 / mon1


def iofq(data: DetectorData, transmission_fraction: TransmissionFraction) -> IofQ:
    return data / transmission_fraction


wf = sciline.Pipeline(
    (process_detector_data, process_mon1, process_mon2, transmission_fraction, iofq)
)


def _make_processor():
    return StreamProcessor(
        wf,
        dynamic_keys=(RawMon1, RawMon2, RawDetectorData),
        accumulators=(Mon1, Mon2, DetectorData),
        target_keys=(IofQ,),
    )


def make_stream_processors():
    return {
        'mantle_detector': _make_processor(),
        'endcap_backward_detector': _make_processor(),
        'endcap_forward_detector': _make_processor(),
        'high_resolution_detector': _make_processor(),
    }


source_to_key = {
    'mantle_detector': RawDetectorData,
    'endcap_backward_detector': RawDetectorData,
    'endcap_forward_detector': RawDetectorData,
    'high_resolution_detector': RawDetectorData,
    'monitor1': RawMon1,
    'monitor2': RawMon2,
}
