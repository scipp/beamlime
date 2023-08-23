# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import List

import scipp as sc


def workflow(
    da_list: List[sc.DataArray], num_pixels: int, histogram_bin_size: int = 50
):
    c_a = sc.scalar(0.0001, unit='m')
    c_b = sc.scalar(1, unit='m')
    c_c = sc.scalar(1, unit='1e-6m^2/s')

    merged = sc.concat(da_list, dim='event')
    pixel_ids = sc.arange(dim='pixel_id', start=0, stop=num_pixels)
    binned = merged.group(pixel_ids)

    binned = binned.transform_coords(
        ['L'],
        graph={
            'L': lambda pixel_id: (pixel_id * c_a) + c_b,
        },
    )
    binned.bins.coords['wavelength'] = (
        c_c * binned.bins.coords['event_time_offset'] / binned.coords['L']
    )
    return binned.hist(wavelength=histogram_bin_size).sum('L')
