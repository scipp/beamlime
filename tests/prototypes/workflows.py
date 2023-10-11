# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import List, NewType

import scipp as sc

from beamlime.constructors import Factory, ProviderGroup

from .parameters import HistogramBinSize, NumPixels

# Coordinates
PixelID = NewType("PixelID", sc.Variable)
EventTimeOffset = NewType("EventTimeOffset", sc.Variable)
Length = NewType("Length", sc.Variable)
WaveLength = NewType("WaveLength", sc.Variable)

# Constants
CoordTransformGraph = NewType("CoordTransformGraph", dict)

# Generated/Calculated
Events = NewType("Events", List[sc.DataArray])
MergedData = NewType("MergedData", sc.DataArray)
PixelIDEdges = NewType("PixelIDEdges", sc.Variable)
PixelGrouped = NewType("PixelGrouped", sc.DataArray)
ReducedData = NewType("ReducedData", sc.DataArray)
Histogrammed = NewType("Histogrammed", sc.DataArray)


def provide_coord_transform_graph() -> CoordTransformGraph:
    c_a = sc.scalar(0.00001, unit='m')
    c_b = sc.scalar(0.1, unit='m')
    c_c = sc.scalar(1, unit='1e-3m^2/s')
    return CoordTransformGraph(
        {
            'L1': lambda pixel_id: (pixel_id * c_a) + c_b,
            'L2': lambda pixel_id: (pixel_id * c_a) + c_b,
            'L': lambda L1, L2: L1 + L2,
            'wavelength': lambda event_time_offset, L: (c_c * event_time_offset / L).to(
                unit='angstrom'
            ),
        }
    )


def workflow_script(
    da_list: Events, num_pixels: NumPixels, histogram_bin_size: HistogramBinSize
) -> Histogrammed:
    merged = sc.concat(da_list, dim='event')
    pixel_ids = sc.arange(dim='pixel_id', start=0, stop=num_pixels)
    binned = merged.group(pixel_ids)

    graph = provide_coord_transform_graph()

    transformed = binned.transform_coords(['L', 'wavelength'], graph=graph)
    return transformed.hist(wavelength=histogram_bin_size).sum('L')


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
    num_pixels: NumPixels, histogram_binsize: HistogramBinSize
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
    return Workflow(Factory(providers))


workflow_providers = ProviderGroup(provide_workflow)
