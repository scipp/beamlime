# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import List, NewType

import scipp as sc

from beamlime.constructors import Factory, ProviderGroup, SingletonProvider

from .parameters import FrameRate, HistogramBinSize, NumPixels

# Coordinates
PixelID = NewType("PixelID", sc.Variable)
EventTimeOffset = NewType("EventTimeOffset", sc.Variable)
Length = NewType("Length", sc.Variable)
WaveLength = NewType("WaveLength", sc.Variable)

# Constants
FirstPulseTime = NewType("FirstPulseTime", sc.Variable)
FrameUnwrappingGraph = NewType("FrameUnwrappingGraph", dict)
CoordTransformGraph = NewType("CoordTransformGraph", dict)

# Generated/Calculated
Events = NewType("Events", List[sc.DataArray])
MergedData = NewType("MergedData", sc.DataArray)
PixelIDEdges = NewType("PixelIDEdges", sc.Variable)
PixelGrouped = NewType("PixelGrouped", sc.DataArray)
ReducedData = NewType("ReducedData", sc.DataArray)
Histogrammed = NewType("Histogrammed", sc.DataArray)


def provide_coord_transform_graph(
    frame_rate: FrameRate, first_pulse_time: FirstPulseTime
) -> CoordTransformGraph:
    from scipp.constants import h, m_n

    lambda_min = sc.scalar(0, unit='angstrom')
    frame_period = sc.scalar(1 / frame_rate, unit='ns')  # No pulse skipping
    scale_factor = (m_n / h).to(
        unit=sc.units.us / sc.units.angstrom**2
    )  # All wavelength is in angstrom unit.
    c_a = sc.scalar(0.00001, unit='m')
    c_b = sc.scalar(0.1, unit='m')
    c_c = sc.scalar(1, unit='1e-3m^2/s')

    def time_offset_pivot(tof_min: sc.Variable, frame_offset: sc.Variable):
        return (frame_offset + tof_min) % frame_period

    def tof_from_time_offset(
        event_time_offset: sc.Variable,
        time_offset_pivot: sc.Variable,
        tof_min: sc.Variable,
    ):
        shift = tof_min - time_offset_pivot
        tof = sc.where(
            event_time_offset >= time_offset_pivot, shift, shift + frame_period
        )
        tof += event_time_offset
        return tof

    def wavelength_from_tof(tof, L):
        return (c_c * tof / L).to(unit='angstrom')

    return CoordTransformGraph(
        {
            'L1': lambda pixel_id: (pixel_id * c_a) + c_b,
            'L2': lambda pixel_id: (pixel_id * c_a) + c_b,
            'L': lambda L1, L2: L1 + L2,
            'tof_min': lambda L: (L * scale_factor * lambda_min).to(unit=sc.units.ns),
            'frame_offset': lambda event_time_zero: event_time_zero - first_pulse_time,
            'time_offset_pivot': time_offset_pivot,
            'tof': tof_from_time_offset,
            'wavelength': wavelength_from_tof,
        }
    )


def merge_data_list(da_list: Events) -> MergedData:
    return MergedData(sc.concat(da_list, dim='event'))


def provide_pixel_id_bin_edges(num_pixels: NumPixels) -> PixelIDEdges:
    return PixelIDEdges(sc.arange(dim='pixel_id', start=0, stop=num_pixels))


def bin_pixel_id(da: MergedData, pixel_bin_coord: PixelIDEdges) -> PixelGrouped:
    return PixelGrouped(da.group(pixel_bin_coord))


def transform_coords(
    binned: PixelGrouped,
    graph: CoordTransformGraph,
) -> ReducedData:
    return ReducedData(binned.transform_coords(['L', 'wavelength'], graph=graph))


def histogram_result(
    bin_size: HistogramBinSize, reduced_data: ReducedData
) -> Histogrammed:
    return reduced_data.hist(wavelength=bin_size).sum('L')


Workflow = NewType("Workflow", Factory)


def provide_workflow(
    num_pixels: NumPixels, histogram_binsize: HistogramBinSize, frame_rate: FrameRate
) -> Workflow:
    providers = ProviderGroup(
        merge_data_list,
        bin_pixel_id,
        provide_coord_transform_graph,
        transform_coords,
        histogram_result,
        provide_pixel_id_bin_edges,
    )

    providers[NumPixels] = lambda: num_pixels
    providers[HistogramBinSize] = lambda: histogram_binsize
    providers[FrameRate] = lambda: frame_rate
    return Workflow(Factory(providers))


workflow_providers = ProviderGroup(SingletonProvider(provide_workflow))
