# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc
from scipp.testing import assert_identical
from streaming_data_types import eventdata_ev44

from beamlime.handlers.to_nxevent_data import (
    DetectorEvents,
    MonitorEvents,
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
