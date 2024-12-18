# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc
from scipp.testing import assert_identical

from beamlime.handlers.accumulators import Histogrammer, MonitorEvents


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


def test_can_clear_histogrammer() -> None:
    histogrammer = Histogrammer(config={})
    histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ns'))
    before = histogrammer.get()
    assert before.sum().value > 0
    histogrammer.clear()
    after = histogrammer.get()
    assert after.sum().value == 0


def test_histogrammer_accumulates_consecutive_add_calls() -> None:
    histogrammer = Histogrammer(config={'time_of_arrival_bins': 7})
    histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ns'))
    histogrammer.add(1, MonitorEvents(time_of_arrival=[2.0, 20.0], unit='ns'))
    da = histogrammer.get()
    assert sc.identical(da.sum().data, sc.scalar(4, unit='counts'))
