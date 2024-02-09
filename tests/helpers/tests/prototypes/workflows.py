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
LtotalGraph = NewType("LtotalGraph", dict)
WavelengthGraph = NewType("WavelengthGraph", dict)

# Generated/Calculated
Events = NewType("Events", List[sc.DataArray])
MergedData = NewType("MergedData", sc.DataArray)
PixelIDEdges = NewType("PixelIDEdges", sc.Variable)
PixelGrouped = NewType("PixelGrouped", sc.DataArray)
LTotalCalculated = NewType("Transformed", sc.DataArray)
FrameUnwrapped = NewType("FrameUnwrapped", sc.DataArray)
ReducedData = NewType("ReducedData", sc.DataArray)
Histogrammed = NewType("Histogrammed", sc.DataArray)


def provide_Ltotal_graph() -> LtotalGraph:
    c_a = sc.scalar(0.00001, unit='m')
    c_b = sc.scalar(0.1, unit='m')

    return LtotalGraph(
        {
            'L1': lambda pixel_id: (pixel_id * c_a) + c_b,
            'L2': lambda pixel_id: (pixel_id * c_a) + c_b,
            'Ltotal': lambda L1, L2: L1 + L2,
        }
    )


def provide_wavelength_graph() -> WavelengthGraph:
    c_c = sc.scalar(1, unit='1e-3m^2/s')

    return WavelengthGraph(
        {'wavelength': lambda tof, Ltotal: (c_c * tof / Ltotal).to(unit='angstrom')}
    )


def merge_data_list(da_list: Events) -> MergedData:
    return MergedData(sc.concat(da_list, dim='event'))


def provide_pixel_id_bin_edges(num_pixels: NumPixels) -> PixelIDEdges:
    return PixelIDEdges(sc.arange(dim='pixel_id', start=0, stop=num_pixels))


def bin_pixel_id(da: MergedData, pixel_bin_coord: PixelIDEdges) -> PixelGrouped:
    return PixelGrouped(da.group(pixel_bin_coord))


def calculate_ltotal(
    binned: PixelGrouped,
    graph: LtotalGraph,
) -> LTotalCalculated:
    da = binned.transform_coords(['Ltotal'], graph=graph)
    if not isinstance(da, sc.DataArray):
        raise TypeError

    return LTotalCalculated(da)


def unwrap_frames(
    da: LTotalCalculated, frame_rate: FrameRate, first_pulse_time: FirstPulseTime
) -> FrameUnwrapped:
    from scippneutron.tof import unwrap_frames

    return FrameUnwrapped(
        unwrap_frames(
            da,
            pulse_period=sc.scalar(1 / frame_rate, unit='ns'),  # No pulse skipping
            lambda_min=sc.scalar(5.0, unit='angstrom'),
            frame_offset=first_pulse_time.to(unit='ms'),
            first_pulse_time=first_pulse_time,
        )
    )


def calculate_wavelength(
    unwrapped: FrameUnwrapped, graph: WavelengthGraph
) -> ReducedData:
    da = unwrapped.transform_coords(['wavelength'], graph=graph)
    if not isinstance(da, sc.DataArray):
        raise TypeError

    return ReducedData(da)


def histogram_result(
    bin_size: HistogramBinSize, reduced_data: ReducedData
) -> Histogrammed:
    return reduced_data.hist(wavelength=bin_size)


Workflow = NewType("Workflow", Factory)


def provide_workflow(
    num_pixels: NumPixels, histogram_binsize: HistogramBinSize, frame_rate: FrameRate
) -> Workflow:
    providers = ProviderGroup(
        SingletonProvider(provide_wavelength_graph),
        SingletonProvider(provide_Ltotal_graph),
        SingletonProvider(provide_pixel_id_bin_edges),
        merge_data_list,
        bin_pixel_id,
        calculate_ltotal,
        calculate_wavelength,
        unwrap_frames,
        histogram_result,
    )

    providers[NumPixels] = lambda: num_pixels
    providers[HistogramBinSize] = lambda: histogram_binsize
    providers[FrameRate] = lambda: frame_rate
    return Workflow(Factory(providers))


workflow_providers = ProviderGroup(SingletonProvider(provide_workflow))