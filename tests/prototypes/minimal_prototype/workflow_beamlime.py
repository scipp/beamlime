# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import List, NewType

import scipp as sc

from beamlime.constructors import Factory, ProviderGroup

# User Configured
NumPixels = NewType("NumPixels", int)
HistogramBinSize = NewType("HistogramBinSize", int)
ChunkSize = NewType("ChunkSize", int)

# Coordinates
PixelID = NewType("PixelID", sc.Variable)
EventTimeOffset = NewType("EventTimeOffset", sc.Variable)
Length = NewType("Length", sc.Variable)
WaveLength = NewType("WaveLength", sc.Variable)

# Constants
ConstantA = NewType("ConstantA", sc.Variable)
ConstantB = NewType("ConstantB", sc.Variable)
ConstantC = NewType("ConstantC", sc.Variable)
default_c_a = ConstantA(sc.scalar(0.0001, unit='m'))
default_c_b = ConstantB(sc.scalar(1, unit='m'))
default_c_c = ConstantC(sc.scalar(1, unit='1e-6m^2/s'))

# Generated/Calculated
Event = NewType("Event", sc.DataArray)
Events = NewType("Events", List[Event])
MergedData = NewType("MergedData", sc.DataArray)
PixelIDEdges = NewType("PixelIDEdges", sc.Variable)
BinnedData = NewType("BinnedData", sc.DataArray)
ReducedData = NewType("ReducedData", sc.DataArray)
Histogrammed = NewType("Histogrammed", sc.DataArray)


def merge_data_list(da_list: Events) -> MergedData:
    return MergedData(sc.concat(da_list, dim='event'))


def provide_pixel_id_bin_edges(num_pixels: NumPixels) -> PixelIDEdges:
    return PixelIDEdges(sc.arange(dim='pixel_id', start=0, stop=num_pixels))


def bin_pixel_id(da: MergedData, pixel_bin_coord: PixelIDEdges) -> BinnedData:
    return da.group(pixel_bin_coord)


def retrieve_pixel_id(binned: BinnedData) -> PixelID:
    return PixelID(binned.coords['pixel_id'])


def retrieve_event_time_offset(binned: BinnedData) -> EventTimeOffset:
    return EventTimeOffset(binned.bins.coords['event_time_offset'])


def pixel_id_to_l(
    pixel_id: PixelID, c_a: ConstantA = default_c_a, c_b: ConstantB = default_c_b
) -> Length:
    # Dummy function. Doesn't have any meaning.
    return Length((pixel_id * c_a) + c_b)


def etoff_to_wavelength(
    et_offset: EventTimeOffset, length: Length, c_c: ConstantC = default_c_c
) -> WaveLength:
    # Dummy function. Doesn't have any meaning.
    return WaveLength(c_c * et_offset / length)


def combine_coords(
    binned: BinnedData, length: Length, wavelength: WaveLength
) -> ReducedData:
    # TODO: benchmark sc.transform_coords
    binned.coords['L'] = length
    binned.bins.coords['wavelength'] = wavelength
    return ReducedData(binned)


def histogram_result(
    bin_size: HistogramBinSize, reduced_data: ReducedData
) -> Histogrammed:
    return reduced_data.hist(wavelength=bin_size).sum('pixel_id')


Workflow = NewType("Workflow", Factory)


def provide_workflow(
    num_pixels: NumPixels, histogram_binsize: HistogramBinSize
) -> Workflow:
    providers = ProviderGroup(
        merge_data_list,
        bin_pixel_id,
        retrieve_pixel_id,
        pixel_id_to_l,
        etoff_to_wavelength,
        combine_coords,
        retrieve_event_time_offset,
        histogram_result,
    )
    providers.cached_provider(PixelIDEdges, provide_pixel_id_bin_edges)
    providers[ConstantA] = lambda: sc.scalar(0.0001, unit='m')
    providers[ConstantB] = lambda: sc.scalar(1, unit='m')
    providers[ConstantC] = lambda: sc.scalar(1, unit='1e-6m^2/s')
    providers[NumPixels] = lambda: num_pixels
    providers[HistogramBinSize] = lambda: histogram_binsize
    return Workflow(Factory(providers))


workflow_providers = ProviderGroup(provide_workflow)
