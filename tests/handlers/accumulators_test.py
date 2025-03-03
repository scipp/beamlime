# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc
from scipp.testing import assert_identical
from streaming_data_types import eventdata_ev44

from beamlime.core.handler import Accumulator
from beamlime.handlers.accumulators import (
    Cumulative,
    DetectorEvents,
    MonitorEvents,
    TOAHistogrammer,
    ToNXevent_data,
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


@pytest.mark.parametrize('accumulator_cls', [Cumulative, ToNXevent_data])
def test_accumulator_raises_if_get_before_add(
    accumulator_cls: type[Accumulator],
) -> None:
    accumulator = accumulator_cls()
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


@pytest.mark.parametrize('accumulator_cls', [Cumulative])
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
    histogrammer = TOAHistogrammer(config={'time_of_arrival_bins': 7})
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
    histogrammer = TOAHistogrammer(config={})
    histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ns'))
    before = histogrammer.get()
    assert before.sum().value > 0
    histogrammer.clear()
    after = histogrammer.get()
    assert after.sum().value == 0


def test_histogrammer_accumulates_consecutive_add_calls() -> None:
    histogrammer = TOAHistogrammer(config={'time_of_arrival_bins': 7})
    histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ns'))
    histogrammer.add(1, MonitorEvents(time_of_arrival=[2.0, 20.0], unit='ns'))
    da = histogrammer.get()
    assert sc.identical(da.sum().data, sc.scalar(4, unit='counts'))


def test_histogrammer_raises_if_unit_is_not_ns() -> None:
    histogrammer = TOAHistogrammer(config={'time_of_arrival_bins': 7})
    with pytest.raises(ValueError, match="Expected unit 'ns'"):
        histogrammer.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ms'))


def test_MonitorEvents_ToNXevent_data() -> None:
    to_nx = ToNXevent_data()
    to_nx.add(0, MonitorEvents(time_of_arrival=[1.0, 10.0], unit='ns'))
    to_nx.add(1000, MonitorEvents(time_of_arrival=[2.0], unit='ns'))
    events = to_nx.get()
    content = sc.DataArray(
        data=sc.ones(dims=['event'], shape=[3], unit='counts', dtype='float32'),
        coords={
            'event_time_offset': sc.array(
                dims=['event'], values=[1.0, 10.0, 2.0], unit='ns'
            )
        },
    )
    assert_identical(
        events,
        sc.DataArray(
            data=sc.bins(
                begin=sc.array(dims=['event'], values=[0, 2], unit=None),
                dim='event',
                data=content,
            ),
            coords={
                'event_time_zero': sc.epoch(unit='ns')
                + sc.array(dims=['event_time_zero'], values=[0, 1000], unit='ns')
            },
        ),
    )
    empty_events = to_nx.get()
    assert empty_events.sizes == {'event_time_zero': 0}
    assert_identical(empty_events, events[0:0])


def test_DetectorEvents_ToNXevent_data() -> None:
    to_nx = ToNXevent_data()
    to_nx.add(
        0, DetectorEvents(time_of_arrival=[1.0, 10.0], pixel_id=[2, 1], unit='ns')
    )
    to_nx.add(1000, DetectorEvents(time_of_arrival=[2.0], pixel_id=[1], unit='ns'))
    events = to_nx.get()
    content = sc.DataArray(
        data=sc.ones(dims=['event'], shape=[3], unit='counts', dtype='float32'),
        coords={
            'event_time_offset': sc.array(
                dims=['event'], values=[1.0, 10.0, 2.0], unit='ns'
            ),
            'event_id': sc.array(
                dims=['event'], values=[2, 1, 1], unit=None, dtype='int32'
            ),
        },
    )
    assert_identical(
        events,
        sc.DataArray(
            data=sc.bins(
                begin=sc.array(dims=['event'], values=[0, 2], unit=None),
                dim='event',
                data=content,
            ),
            coords={
                'event_time_zero': sc.epoch(unit='ns')
                + sc.array(dims=['event_time_zero'], values=[0, 1000], unit='ns')
            },
        ),
    )
    empty_events = to_nx.get()
    assert empty_events.sizes == {'event_time_zero': 0}
    assert_identical(empty_events, events[0:0])
