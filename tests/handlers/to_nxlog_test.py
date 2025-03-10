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
