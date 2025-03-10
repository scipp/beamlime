# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import pytest
import scipp as sc
from scipp.testing import assert_identical

from beamlime.handlers.timeseries_handler import Timeseries


@pytest.fixture
def simple_dataarray() -> sc.DataArray:
    """Create a simple DataArray with a time dimension."""
    return sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0], unit="K"),
        coords={"time": sc.array(dims=["time"], values=[10], unit="ns")},
    )


@pytest.fixture
def multi_item_dataarray() -> sc.DataArray:
    """Create a DataArray with multiple items in the time dimension."""
    return sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0, 2.0, 3.0], unit="K"),
        coords={"time": sc.array(dims=["time"], values=[10, 20, 30], unit="ns")},
    )


@pytest.fixture
def multi_dim_dataarray() -> sc.DataArray:
    """Create a DataArray with multiple dimensions including time."""
    return sc.DataArray(
        data=sc.array(
            dims=["time", "detector"], values=[[1.0, 2.0], [3.0, 4.0]], unit="K"
        ),
        coords={
            "time": sc.array(dims=["time"], values=[10, 20], unit="ns"),
            "detector": sc.array(dims=["detector"], values=[0, 1]),
        },
    )


@pytest.fixture
def make_dataarray() -> callable:
    """Create a factory function for making DataArrays with specified time values."""

    def _make_dataarray(time_values: list[int]) -> sc.DataArray:
        return sc.DataArray(
            data=sc.array(dims=["time"], values=[1.0] * len(time_values), unit="K"),
            coords={"time": sc.array(dims=["time"], values=time_values, unit="ns")},
        )

    return _make_dataarray


def test_raises_when_getting_from_empty_initial_state():
    """Test that a newly created Timeseries has no data."""
    ts = Timeseries()
    with pytest.raises(ValueError, match="No data has been added"):
        ts.get()


def test_add_first_data(simple_dataarray: sc.DataArray):
    """Test adding initial data to a timeseries."""
    ts = Timeseries()
    ts.add(0, simple_dataarray)

    result = ts.get()
    assert_identical(result, simple_dataarray)


def test_add_multiple_items(simple_dataarray: sc.DataArray, make_dataarray: callable):
    """Test adding multiple items to a timeseries."""
    ts = Timeseries()
    ts.add(0, simple_dataarray)  # Add first item with time=10

    # Add second item with time=20
    second_data = make_dataarray([20])
    ts.add(0, second_data)

    # Check returned data combines both additions correctly
    expected = sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0, 1.0], unit="K"),
        coords={"time": sc.array(dims=["time"], values=[10, 20], unit="ns")},
    )
    assert_identical(ts.get(), expected)


def test_capacity_expansion_with_many_adds(make_dataarray: callable):
    """Test that capacity expands automatically as needed with many additions."""
    ts = Timeseries()

    # Add initial data with 3 items
    ts.add(0, make_dataarray([10, 20, 30]))
    assert_identical(ts.get(), make_dataarray([10, 20, 30]))

    # Add more data that would exceed the initial capacity
    ts.add(0, make_dataarray([40, 50, 60, 70]))

    # Check all data is preserved
    expected = sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0] * 7, unit="K"),
        coords={
            "time": sc.array(
                dims=["time"], values=[10, 20, 30, 40, 50, 60, 70], unit="ns"
            )
        },
    )
    assert_identical(ts.get(), expected)

    # Continue adding more data to trigger multiple capacity expansions
    for i in range(8, 20):
        ts.add(0, make_dataarray([i * 10]))

    # Verify all data is still correct
    expected_times = [10, 20, 30, 40, 50, 60, 70] + [i * 10 for i in range(8, 20)]
    expected = sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0] * len(expected_times), unit="K"),
        coords={"time": sc.array(dims=["time"], values=expected_times, unit="ns")},
    )
    assert_identical(ts.get(), expected)


def test_clear_timeseries(simple_dataarray: sc.DataArray):
    """Test that clear removes all data."""
    ts = Timeseries()
    ts.add(0, simple_dataarray)

    # Verify data is there
    assert ts.get().sizes["time"] == 1

    # Clear and check that get returns empty DataArray
    ts.clear()
    assert ts.get().sizes["time"] == 0


def test_add_after_clear(simple_dataarray: sc.DataArray, make_dataarray: callable):
    """Test adding data after clearing."""
    ts = Timeseries()
    ts.add(0, simple_dataarray)
    ts.clear()

    # Add new data
    new_data = make_dataarray([50])
    ts.add(0, new_data)

    # Check the new data is correctly stored
    assert_identical(ts.get(), new_data)


def test_add_multidimensional_data(multi_dim_dataarray: sc.DataArray):
    """Test adding data with multiple dimensions."""
    ts = Timeseries()
    ts.add(0, multi_dim_dataarray)

    # Check returned data
    result = ts.get()
    assert_identical(result, multi_dim_dataarray)
    assert result.dims == ("time", "detector")


def test_add_empty_dataarray(make_dataarray: callable):
    """Test adding an empty DataArray."""
    empty_data = make_dataarray([])

    ts = Timeseries()
    ts.add(0, empty_data)

    # Should return an empty DataArray
    assert ts.get().sizes["time"] == 0


def test_preservation_of_addition_order(make_dataarray: callable):
    """Test that multiple adds preserve the order in which data was added."""
    ts = Timeseries()

    # Add data with time 30
    ts.add(0, make_dataarray([30]))

    # Add data with earlier time 10
    ts.add(0, make_dataarray([10]))

    # Add data with middle time 20
    ts.add(0, make_dataarray([20]))

    # The data should be appended in the order it was added, not sorted by time
    expected = sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0, 1.0, 1.0], unit="K"),
        coords={"time": sc.array(dims=["time"], values=[30, 10, 20], unit="ns")},
    )
    assert_identical(ts.get(), expected)


def test_large_capacity_jumps(make_dataarray: callable):
    """Test adding data that forces large capacity increases."""
    ts = Timeseries()

    # First add a small item to initialize
    ts.add(0, make_dataarray([10]))

    # Add much larger array that would greatly exceed any reasonable initial capacity
    many_items = list(range(100, 600))
    large_data = make_dataarray(many_items)
    ts.add(0, large_data)

    # Check that all items were added correctly
    all_times = [10, *many_items]
    expected = sc.DataArray(
        data=sc.array(dims=["time"], values=[1.0] * len(all_times), unit="K"),
        coords={"time": sc.array(dims=["time"], values=all_times, unit="ns")},
    )
    assert_identical(ts.get(), expected)


def test_capacity_against_small_additions(make_dataarray: callable):
    """Test capacity behavior with many small additions."""
    ts = Timeseries()

    # Add many items one by one, which should trigger capacity expansion multiple times
    all_times = []
    for i in range(100):
        value = i * 10
        ts.add(0, make_dataarray([value]))
        all_times.append(value)

        # Check that all data added so far is preserved correctly
        expected = sc.DataArray(
            data=sc.array(dims=["time"], values=[1.0] * len(all_times), unit="K"),
            coords={"time": sc.array(dims=["time"], values=all_times, unit="ns")},
        )
        assert_identical(ts.get(), expected)


def test_repeated_expand_and_clear_cycles(make_dataarray: callable):
    """Test repeated cycles of adding many items and clearing."""
    ts = Timeseries()

    for cycle in range(3):
        # Add enough items to trigger capacity expansion
        times = [(cycle * 1000) + (i * 10) for i in range(50)]
        for t in times:
            ts.add(0, make_dataarray([t]))

        # Verify all items are present
        expected = sc.DataArray(
            data=sc.array(dims=["time"], values=[1.0] * len(times), unit="K"),
            coords={"time": sc.array(dims=["time"], values=times, unit="ns")},
        )
        assert_identical(ts.get(), expected)

        # Clear and verify empty
        ts.clear()
        assert ts.get().sizes["time"] == 0
