# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc
from scipp.testing import assert_identical

from beamlime.handlers.accumulators import Histogrammer


def test_histogrammer_returns_zeros_if_no_chunks_added() -> None:
    histogrammer = Histogrammer(config={'time_of_arrival_bins': 7})
    da = histogrammer.get()
    dim = 'time_of_arrival'
    bins = sc.linspace(dim, 0.0, 1000 / 14, num=7, unit='ms')
    assert_identical(
        da,
        sc.DataArray(
            sc.zeros(dims=[dim], shape=[6], unit='counts', dtype='int64'),
            coords={dim: bins},
        ),
    )
