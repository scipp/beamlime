# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc
from streaming_data_types import logdata_f144

from beamlime.core.handler import Accumulator
from beamlime.handlers.accumulators import (
    Cumulative,
    ForwardingAccumulator,
    GroupIntoPixels,
    LogData,
    NullAccumulator,
)
from beamlime.handlers.to_nxevent_data import DetectorEvents, ToNXevent_data


def test_LogData_from_f144() -> None:
    f144_data = logdata_f144.ExtractedLogData(
        source_name='abc', value=42.0, timestamp_unix_ns=12345
    )

    log_data = LogData.from_f144(f144_data)
    assert log_data.time == 12345
    assert log_data.value == 42.0


@pytest.mark.parametrize('accumulator_cls', [Cumulative, ToNXevent_data])
def test_accumulator_raises_if_get_before_add(
    accumulator_cls: type[Accumulator],
) -> None:
    accumulator = accumulator_cls()
    with pytest.raises(ValueError, match="No data has been added"):
        accumulator.get()


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


class TestNullAccumulator:
    def test_add_does_nothing(self) -> None:
        accumulator = NullAccumulator()
        accumulator.add(0, "some data")
        # Should not raise any exceptions

    def test_get_returns_none(self) -> None:
        accumulator = NullAccumulator()
        assert accumulator.get() is None

    def test_clear_does_nothing(self) -> None:
        accumulator = NullAccumulator()
        accumulator.clear()
        # Should not raise any exceptions


class TestForwardingAccumulator:
    def test_add_stores_data(self) -> None:
        accumulator = ForwardingAccumulator[str]()
        accumulator.add(0, "test data")
        assert accumulator.get() == "test data"

    def test_get_before_add_raises_error(self) -> None:
        accumulator = ForwardingAccumulator[str]()
        with pytest.raises(ValueError, match="No data has been added"):
            accumulator.get()

    def test_add_overwrites_previous_data(self) -> None:
        accumulator = ForwardingAccumulator[str]()
        accumulator.add(0, "first")
        accumulator.add(1, "second")
        assert accumulator.get() == "second"

    def test_clear_does_nothing(self) -> None:
        accumulator = ForwardingAccumulator[str]()
        accumulator.add(0, "test")
        accumulator.clear()
        # Clear doesn't affect ForwardingAccumulator behavior
        assert accumulator.get() == "test"


class TestCumulative:
    def test_get_before_add_raises_error(self) -> None:
        accumulator = Cumulative()
        with pytest.raises(ValueError, match="No data has been added"):
            accumulator.get()

    def test_cumulative_no_clear_on_get(self) -> None:
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

    def test_cumulative_clear_on_get(self) -> None:
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

    def test_clears_when_data_sizes_changes(self) -> None:
        cumulative = Cumulative(config={})
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

    def test_clears_when_coords_change(self) -> None:
        cumulative = Cumulative(config={})
        da1 = sc.DataArray(
            sc.array(dims=['x'], values=[1.0], unit='counts'),
            coords={'x': sc.array(dims=['x'], values=[0.0], unit='s')},
        )
        da2 = sc.DataArray(
            sc.array(dims=['x'], values=[2.0], unit='counts'),
            coords={'x': sc.array(dims=['x'], values=[1.0], unit='s')},
        )
        cumulative.add(0, da1)
        cumulative.add(1, da2)
        # Should have cleared and only contain da2
        assert sc.identical(
            cumulative.get().data, sc.array(dims=['x'], values=[2.0], unit='counts')
        )

    def test_manual_clear(self) -> None:
        cumulative = Cumulative(config={})
        da = sc.DataArray(
            sc.array(dims=['x'], values=[1.0], unit='counts'),
            coords={'x': sc.arange('x', 2, unit='s')},
        )
        cumulative.add(0, da)
        cumulative.clear()
        with pytest.raises(ValueError, match="No data has been added"):
            cumulative.get()


class TestGroupIntoPixels:
    def test_get_before_add_raises_error(self) -> None:
        detector_number = sc.array(dims=['y', 'x'], values=[[0, 1], [2, 3]], unit=None)
        grouper = GroupIntoPixels(config={}, detector_number=detector_number)

        # Should not raise since it returns empty grouped data
        result = grouper.get()
        assert result.sizes == {'y': 2, 'x': 2}

    def test_groups_events_by_pixel_id(self) -> None:
        detector_number = sc.array(dims=['y', 'x'], values=[[0, 1], [2, 3]], unit=None)
        grouper = GroupIntoPixels(config={}, detector_number=detector_number)

        events = DetectorEvents(
            pixel_id=[0, 1, 0, 2],
            time_of_arrival=[100.0, 200.0, 150.0, 300.0],
            unit='ns',
        )

        grouper.add(0, events)
        result = grouper.get()

        assert result.sizes == {'y': 2, 'x': 2}
        # Check that events are grouped correctly
        pixel_0_0 = result['y', 0]['x', 0]  # detector_number=0
        pixel_0_1 = result['y', 0]['x', 1]  # detector_number=1
        pixel_1_0 = result['y', 1]['x', 0]  # detector_number=2
        pixel_1_1 = result['y', 1]['x', 1]  # detector_number=3

        assert len(pixel_0_0.values) == 2  # Two events for pixel 0
        assert len(pixel_0_1.values) == 1  # One event for pixel 1
        assert len(pixel_1_0.values) == 1  # One event for pixel 2
        assert len(pixel_1_1.values) == 0  # No events for pixel 3

    def test_raises_if_unit_is_not_ns(self) -> None:
        detector_number = sc.array(dims=['x'], values=[0, 1])
        grouper = GroupIntoPixels(config={}, detector_number=detector_number)

        events = DetectorEvents(
            pixel_id=[0, 1], time_of_arrival=[100.0, 200.0], unit='ms'
        )

        with pytest.raises(ValueError, match="Expected unit 'ns'"):
            grouper.add(0, events)

    def test_accumulates_multiple_chunks(self) -> None:
        detector_number = sc.array(dims=['x'], values=[0, 1], unit=None)
        grouper = GroupIntoPixels(config={}, detector_number=detector_number)

        events1 = DetectorEvents(
            pixel_id=[0, 1], time_of_arrival=[100.0, 200.0], unit='ns'
        )
        events2 = DetectorEvents(
            pixel_id=[0, 1], time_of_arrival=[150.0, 250.0], unit='ns'
        )

        grouper.add(0, events1)
        grouper.add(1, events2)
        result = grouper.get()

        # Each pixel should have 2 events
        assert len(result['x', 0].values) == 2
        assert len(result['x', 1].values) == 2

    def test_clear(self) -> None:
        detector_number = sc.array(dims=['x'], values=[0, 1], unit=None)
        grouper = GroupIntoPixels(config={}, detector_number=detector_number)

        events = DetectorEvents(
            pixel_id=[0, 1], time_of_arrival=[100.0, 200.0], unit='ns'
        )

        grouper.add(0, events)
        grouper.clear()
        result = grouper.get()

        # Should be empty after clear
        assert len(result['x', 0].values) == 0
        assert len(result['x', 1].values) == 0
