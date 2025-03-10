# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc
from scipp.testing import assert_identical

from beamlime.handlers.accumulators import LogData
from beamlime.handlers.to_nx_log import ToNXlog


def test_to_nxlog_initialization():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)
    assert accumulator is not None


def test_to_nxlog_add_single_value():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    log_data = LogData(time=5000000, value=42.0)
    accumulator.add(timestamp=0, data=log_data)

    # Check the data was added by retrieving it
    result = accumulator.get()
    assert_identical(result.data, sc.array(dims=['time'], values=[42.0], unit='counts'))


def test_to_nxlog_get_single_value():
    start_str = '2023-01-01T00:00:00.000000'
    attrs = {'time': {'start': start_str, 'units': 'ns'}, 'value': {'units': 'counts'}}
    accumulator = ToNXlog(attrs=attrs)

    log_data = LogData(time=1_000_000_000, value=42.0)
    accumulator.add(timestamp=0, data=log_data)

    result = accumulator.get()

    assert_identical(result.data, sc.array(dims=['time'], values=[42.0], unit='counts'))
    assert_identical(
        result.coords['time'][0], sc.datetime('2023-01-01T00:00:01', unit='ns')
    )


def test_to_nxlog_get_multiple_values():
    start_str = '2023-06-15T12:30:45.000000'
    attrs = {
        'time': {'start': start_str, 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    log_data1 = LogData(time=1000000, value=273.15)
    log_data2 = LogData(time=2000000, value=293.15)
    log_data3 = LogData(time=3000000, value=303.15)

    accumulator.add(timestamp=0, data=log_data1)
    accumulator.add(timestamp=0, data=log_data2)
    accumulator.add(timestamp=0, data=log_data3)

    result = accumulator.get()

    assert_identical(
        result.data, sc.array(dims=['time'], values=[273.15, 293.15, 303.15], unit='K')
    )

    # Test timestamps are correct relative to start time
    start_time = sc.datetime(start_str, unit='ns')
    expected_times = [start_time.value + t for t in [1000000, 2000000, 3000000]]
    expected_time_coord = sc.array(dims=['time'], values=expected_times, unit='ns')
    assert_identical(result.coords['time'], expected_time_coord)


def test_to_nxlog_clear():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add data and verify it's there by getting it
    log_data = LogData(time=5000000, value=42.0)
    accumulator.add(timestamp=0, data=log_data)
    result = accumulator.get()
    assert_identical(result.data, sc.array(dims=['time'], values=[42.0], unit='counts'))

    # After get(), accumulator should be empty
    # Add another value to check if it's the only one
    log_data2 = LogData(time=6000000, value=43.0)
    accumulator.add(timestamp=0, data=log_data2)

    # Explicitly clear
    accumulator.clear()

    # Verify it's empty by adding a new value and checking it's the only one
    log_data3 = LogData(time=7000000, value=44.0)
    accumulator.add(timestamp=0, data=log_data3)
    result = accumulator.get()
    assert_identical(result.data, sc.array(dims=['time'], values=[44.0], unit='counts'))


def test_to_nxlog_get_does_not_clear_data():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add data and get it
    log_data = LogData(time=5000000, value=42.0)
    accumulator.add(timestamp=0, data=log_data)
    _ = accumulator.get()

    # After get(), adding new values should keep the previous data
    log_data2 = LogData(time=7000000, value=100.0)
    accumulator.add(timestamp=0, data=log_data2)

    result = accumulator.get()
    assert_identical(
        result.data, sc.array(dims=['time'], values=[42.0, 100.0], unit='counts')
    )


def test_to_nxlog_empty_get():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Getting data from an empty accumulator should return an empty DataArray
    result = accumulator.get()
    assert_identical(result.data, sc.array(dims=['time'], values=[], unit='counts'))


def test_to_nxlog_missing_attributes():
    # Missing start time
    attrs1 = {
        'time': {'units': 'ns'},
        'value': {'units': 'counts'},
    }
    with pytest.raises(KeyError, match="'start'"):
        ToNXlog(attrs=attrs1)

    # Missing units => unit is None
    attrs2 = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {},
    }
    assert ToNXlog(attrs=attrs2).unit is None

    # Missing time dictionary
    attrs3 = {
        'value': {'units': 'counts'},
    }
    with pytest.raises(KeyError, match="'time'"):
        ToNXlog(attrs=attrs3)

    # Missing value dictionary => unit is None
    attrs4 = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
    }
    assert ToNXlog(attrs=attrs4).unit is None


def test_to_nxlog_invalid_time_format():
    # Invalid time string format
    attrs = {
        'time': {'start': 'invalid_time_string', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    with pytest.raises(ValueError, match="Failed to parse start time"):
        ToNXlog(attrs=attrs)


def test_to_nxlog_add_values_with_different_timestamps():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add values with increasing timestamps to verify correct ordering
    log_data1 = LogData(time=3000000, value=30.0)
    log_data2 = LogData(time=1000000, value=10.0)
    log_data3 = LogData(time=2000000, value=20.0)

    accumulator.add(timestamp=100, data=log_data1)
    accumulator.add(timestamp=200, data=log_data2)
    accumulator.add(timestamp=300, data=log_data3)

    result = accumulator.get()

    # Verify the values are ordered by log_data.time, not by timestamp
    assert_identical(
        result.data, sc.array(dims=['time'], values=[10.0, 20.0, 30.0], unit='counts')
    )

    # Verify times are also ordered
    start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='ns')
    expected_times = [start_time.value + t for t in [1000000, 2000000, 3000000]]
    expected_time_coord = sc.array(dims=['time'], values=expected_times, unit='ns')
    assert_identical(result.coords['time'], expected_time_coord)


def test_to_nxlog_with_different_time_units():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'us'},
        'value': {'units': 'counts'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add microsecond time values
    log_data1 = LogData(time=1000, value=10.0)
    log_data2 = LogData(time=2000, value=20.0)

    accumulator.add(timestamp=0, data=log_data1)
    accumulator.add(timestamp=0, data=log_data2)

    result = accumulator.get()

    # Verify values
    assert_identical(
        result.data, sc.array(dims=['time'], values=[10.0, 20.0], unit='counts')
    )

    # Verify times use the correct unit
    start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='us')
    expected_time_coord = start_time + sc.array(
        dims=['time'], values=[1000, 2000], unit='us'
    )
    assert_identical(result.coords['time'], expected_time_coord)


def test_to_nxlog_raises_if_not_time_units():
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000'},  # No units specified
        'value': {'units': 'counts'},
    }
    with pytest.raises(KeyError, match="units"):
        ToNXlog(attrs=attrs)


def test_capacity_expansion_with_many_adds():
    """Test that capacity expands automatically as needed with many additions."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add initial data with 3 items
    accumulator.add(0, LogData(time=10, value=1.0))
    accumulator.add(0, LogData(time=20, value=1.0))
    accumulator.add(0, LogData(time=30, value=1.0))

    result = accumulator.get()
    assert result.sizes["time"] == 3

    # Add more data that would exceed the initial capacity
    accumulator.add(0, LogData(time=40, value=1.0))
    accumulator.add(0, LogData(time=50, value=1.0))
    accumulator.add(0, LogData(time=60, value=1.0))
    accumulator.add(0, LogData(time=70, value=1.0))

    # Check all data is preserved
    result = accumulator.get()
    assert result.sizes["time"] == 7

    # Continue adding more data to trigger multiple capacity expansions
    for i in range(8, 20):
        accumulator.add(0, LogData(time=i * 10, value=1.0))

    # Verify all data is still correct
    result = accumulator.get()
    assert result.sizes["time"] == 19
    expected_times = [10, 20, 30, 40, 50, 60, 70] + [i * 10 for i in range(8, 20)]
    expected_time_values = [
        sc.datetime('2023-01-01T00:00:00.000000', unit='ns').value + t
        for t in expected_times
    ]
    assert_identical(
        result.coords['time'],
        sc.array(dims=['time'], values=expected_time_values, unit='ns'),
    )


def test_large_capacity_jumps():
    """Test adding data that forces large capacity increases."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # First add a small item to initialize
    accumulator.add(0, LogData(time=10, value=1.0))

    # Add many items that would greatly exceed any reasonable initial capacity
    many_items = list(range(100, 600))
    for t in many_items:
        accumulator.add(0, LogData(time=t, value=1.0))

    # Check that all items were added correctly
    result = accumulator.get()
    assert result.sizes["time"] == len(many_items) + 1

    # Check first and last values for boundary cases
    start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='ns')
    assert_identical(result.coords['time'][0], start_time + sc.scalar(10, unit='ns'))
    assert_identical(result.coords['time'][-1], start_time + sc.scalar(599, unit='ns'))


def test_capacity_against_small_additions():
    """Test capacity behavior with many small additions."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add many items one by one, which should trigger capacity expansion multiple times
    for i in range(50):  # Using a smaller number (50) for test runtime
        value = i * 10
        accumulator.add(0, LogData(time=value, value=1.0))

        # Check that data added so far is preserved correctly
        result = accumulator.get()
        assert result.sizes["time"] == i + 1

    # Final verification
    result = accumulator.get()
    assert result.sizes["time"] == 50
    # Check first and last values
    start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='ns')
    assert_identical(result.coords['time'][0], start_time + sc.scalar(0, unit='ns'))
    assert_identical(result.coords['time'][-1], start_time + sc.scalar(490, unit='ns'))


def test_repeated_expand_and_clear_cycles():
    """Test repeated cycles of adding many items and clearing."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    for cycle in range(3):
        # Add enough items to trigger capacity expansion
        times = [(cycle * 1000) + (i * 10) for i in range(20)]
        for t in times:
            accumulator.add(0, LogData(time=t, value=1.0))

        # Verify all items are present
        result = accumulator.get()
        assert result.sizes["time"] == len(times)

        # Sample check for cycle 0, 1, 2
        start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='ns')
        expected_first_time = start_time + sc.scalar(cycle * 1000, unit='ns')
        expected_last_time = start_time + sc.scalar((cycle * 1000) + 190, unit='ns')
        assert_identical(result.coords['time'][0], expected_first_time)
        assert_identical(result.coords['time'][-1], expected_last_time)

        # Clear and verify empty
        accumulator.clear()
        empty_result = accumulator.get()
        assert empty_result.sizes["time"] == 0


def test_preservation_of_addition_order():
    """Test that sorting occurs when getting data with multiple adds out of order."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    # Add data with time 30
    accumulator.add(0, LogData(time=30, value=3.0))

    # Add data with earlier time 10
    accumulator.add(0, LogData(time=10, value=1.0))

    # Add data with middle time 20
    accumulator.add(0, LogData(time=20, value=2.0))

    # The data should be sorted by time, not in the order it was added
    result = accumulator.get()
    start_time = sc.datetime('2023-01-01T00:00:00.000000', unit='ns')

    # Check values are sorted by time
    assert_identical(
        result.data, sc.array(dims=['time'], values=[1.0, 2.0, 3.0], unit='K')
    )

    # Check times are sorted
    expected_times = [start_time.value + t for t in [10, 20, 30]]
    expected_time_coord = sc.array(dims=['time'], values=expected_times, unit='ns')
    assert_identical(result.coords['time'], expected_time_coord)


def test_empty_initialization_get():
    """Test that a newly created ToNXlog returns an empty DataArray."""
    attrs = {
        'time': {'start': '2023-01-01T00:00:00.000000', 'units': 'ns'},
        'value': {'units': 'K'},
    }
    accumulator = ToNXlog(attrs=attrs)

    result = accumulator.get()
    assert result.sizes["time"] == 0
    assert result.dims == ('time',)
    assert result.unit == 'K'
    assert 'time' in result.coords
