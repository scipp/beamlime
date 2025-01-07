# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc
from scipp.testing import assert_identical
from streaming_data_types import eventdata_ev44

from beamlime.core.handler import Accumulator
from beamlime.handlers.accumulators import (
    Cumulative,
    Histogrammer,
    MonitorEvents,
    SlidingWindow,
)


def test_MonitorEvents_from_ev44() -> None:
    ev44 = eventdata_ev44.EventData(
        source_name='ignored',
        message_id=0,
        reference_time=[],
        reference_time_index=[],
        pixel_id=[1, 1, 1],
        time_of_flight=[1, 2, 3],
    )
    monitor_events = MonitorEvents.from_ev44(ev44)
    assert monitor_events.time_of_arrival == [1, 2, 3]
    assert monitor_events.unit == 'ns'


@pytest.mark.parametrize('accumulator_cls', [Cumulative, SlidingWindow])
def test_accumulator_raises_if_get_before_add(
    accumulator_cls: type[Accumulator],
) -> None:
    accumulator = accumulator_cls(config={})
    with pytest.raises(ValueError, match="No data has been added"):
        accumulator.get()


def test_cumulative_no_clear_on_get() -> None:
    cumulative = Cumulative(config={}, clear_on_get=False)
    da = sc.DataArray(
        sc.array(dims=['x'], values=[1.0], unit='counts'),
        coords={'x': sc.arange('x', 2, unit='s')},
    )
    cumulative.add(0, da)
    assert sc.identical(
        cumulative.get().data, sc.array(dims=['x'], values=[1.0], unit='counts')
    )
    cumulative.add(1, da)
    assert sc.identical(
        cumulative.get().data, sc.array(dims=['x'], values=[2.0], unit='counts')
    )


def test_cumulative_clear_on_get() -> None:
    cumulative = Cumulative(config={}, clear_on_get=True)
    da = sc.DataArray(
        sc.array(dims=['x'], values=[1.0], unit='counts'),
        coords={'x': sc.arange('x', 2, unit='s')},
    )
    cumulative.add(0, da)
    assert sc.identical(
        cumulative.get().data, sc.array(dims=['x'], values=[1.0], unit='counts')
    )
    cumulative.add(1, da)
    assert sc.identical(
        cumulative.get().data, sc.array(dims=['x'], values=[1.0], unit='counts')
    )


@pytest.mark.parametrize('accumulator_cls', [Cumulative, SlidingWindow])
def test_accumulator_clears_when_data_sizes_changes(
    accumulator_cls: type[Accumulator],
) -> None:
    cumulative = accumulator_cls(config={})
    da = sc.DataArray(
        sc.array(dims=['x'], values=[1.0], unit='counts'),
        coords={'x': sc.arange('x', 2, unit='s')},
    )
    cumulative.add(0, da)
    cumulative.add(1, da)
    da2 = sc.DataArray(
        sc.array(dims=['y'], values=[1.0], unit='counts'),
        coords={'y': sc.arange('y', 2, unit='s')},
    )
    cumulative.add(2, da2)
    cumulative.add(3, da2)
    assert sc.identical(
        cumulative.get().data, sc.array(dims=['y'], values=[2.0], unit='counts')
    )


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


def test_histogrammer_raises_if_unit_is_not_ns() -> None:
    histogrammer = Histogrammer(config={'time_of_arrival_bins': 7})
    with pytest.raises(ValueError, match="Expected unit 'ns'"):
        histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ms'))
