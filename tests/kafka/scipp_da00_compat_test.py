# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import numpy as np
import pytest
import scipp as sc
from streaming_data_types import dataarray_da00

from ess.livedata.kafka.scipp_da00_compat import da00_to_scipp, scipp_to_da00


@pytest.mark.parametrize("unit", [None, 'm', 's', 'counts'])
def test_scipp_to_da00_basic(unit: str | None):
    # Create simple DataArray
    da = sc.DataArray(
        data=sc.array(dims=['x'], values=[1, 2, 3], unit=unit),
        coords={'x': sc.array(dims=['x'], values=[10, 20, 30], unit='m')},
    )

    # Convert to da00
    variables = scipp_to_da00(da)

    # Check results
    assert len(variables) == 2  # data and x coord
    data_var = next(var for var in variables if var.name == 'signal')
    x_var = next(var for var in variables if var.name == 'x')

    assert data_var.unit == unit
    assert np.array_equal(data_var.data, [1, 2, 3])
    assert data_var.axes == ['x']

    assert x_var.unit == 'm'
    assert np.array_equal(x_var.data, [10, 20, 30])
    assert x_var.axes == ['x']


def test_scipp_to_da00_with_variances():
    # Create DataArray with variances
    da = sc.DataArray(
        data=sc.array(
            dims=['x'], values=[1.0, 2.0, 3.0], variances=[0.1, 0.2, 0.3], unit='counts'
        )
    )

    variables = scipp_to_da00(da)

    assert len(variables) == 2  # data and errors
    errors_var = next(var for var in variables if var.name == 'errors')

    # Check that errors are standard deviations (sqrt of variances)
    expected_errors = np.sqrt([0.1, 0.2, 0.3])
    assert np.allclose(errors_var.data, expected_errors)


def test_da00_to_scipp():
    # Create da00 variables
    variables = [
        dataarray_da00.Variable(
            name='signal', data=[1, 2, 3], axes=['x'], shape=(3,), unit='counts'
        ),
        dataarray_da00.Variable(
            name='x', data=[10, 20, 30], axes=['x'], shape=(3,), unit='m'
        ),
    ]

    da = da00_to_scipp(variables)

    assert sc.identical(
        da,
        sc.DataArray(
            sc.array(dims=['x'], values=[1, 2, 3], unit='counts'),
            coords={'x': sc.array(dims=['x'], values=[10, 20, 30], unit='m')},
        ),
    )


def test_da00_to_scipp_with_none_unit():
    """Test that variables with unit=None are handled correctly."""
    variables = [
        dataarray_da00.Variable(
            name='signal', data=[1, 2, 3], axes=['x'], shape=(3,), unit=None
        ),
        dataarray_da00.Variable(
            name='x', data=[10, 20, 30], axes=['x'], shape=(3,), unit=None
        ),
    ]

    da = da00_to_scipp(variables)

    assert sc.identical(
        da,
        sc.DataArray(
            sc.array(dims=['x'], values=[1, 2, 3], unit=None),
            coords={'x': sc.array(dims=['x'], values=[10, 20, 30], unit=None)},
        ),
    )


def test_da00_to_scipp_with_errors_and_none_unit():
    """Test conversion with errors when unit is None."""
    variables = [
        dataarray_da00.Variable(
            name='signal', data=[1.0, 2.0, 3.0], axes=['x'], shape=(3,), unit=None
        ),
        dataarray_da00.Variable(
            name='errors', data=[0.1, 0.2, 0.3], axes=['x'], shape=(3,), unit=None
        ),
    ]

    da = da00_to_scipp(variables)

    expected_variances = np.array([0.1, 0.2, 0.3]) ** 2
    assert sc.identical(
        da,
        sc.DataArray(
            sc.array(
                dims=['x'],
                values=[1.0, 2.0, 3.0],
                variances=expected_variances,
                unit=None,
            )
        ),
    )


def test_scipp_to_da00_datetime64():
    """Test conversion of datetime64 variables."""
    times = sc.array(
        dims=['time'],
        values=np.array(['2024-01-01', '2024-01-02'], dtype='datetime64[D]'),
    )
    da = sc.DataArray(
        data=sc.array(dims=['time'], values=[1, 2], unit='counts'),
        coords={'time': times},
    )

    variables = scipp_to_da00(da)

    time_var = next(var for var in variables if var.name == 'time')
    assert time_var.unit == 'datetime64[D]'
    assert time_var.axes == ['time']


def test_da00_to_scipp_datetime64():
    """Test conversion from da00 datetime64 variables."""
    # Days since epoch for 2024-01-01 and 2024-01-02
    days_since_epoch = [19723, 19724]

    variables = [
        dataarray_da00.Variable(
            name='signal', data=[1, 2], axes=['time'], shape=(2,), unit='counts'
        ),
        dataarray_da00.Variable(
            name='time',
            data=days_since_epoch,
            axes=['time'],
            shape=(2,),
            unit='datetime64[D]',
        ),
    ]

    da = da00_to_scipp(variables)

    expected_times = sc.epoch(unit='D') + sc.array(
        dims=['time'], values=days_since_epoch, unit='D'
    )
    assert sc.identical(da.coords['time'], expected_times)


def test_roundtrip_datetime64():
    """Test roundtrip conversion with datetime64 coordinates."""
    times = sc.array(
        dims=['time'],
        values=np.array(['2024-01-01', '2024-01-02'], dtype='datetime64[D]'),
    )
    original = sc.DataArray(
        data=sc.array(dims=['time'], values=[1, 2], unit='counts'),
        coords={'time': times},
    )

    da00 = scipp_to_da00(original)
    converted = da00_to_scipp(da00)

    assert sc.identical(original, converted)


def test_scipp_to_da00_empty_data():
    """Test conversion with empty arrays."""
    da = sc.DataArray(
        data=sc.array(dims=['x'], values=[], unit='counts'),
        coords={'x': sc.array(dims=['x'], values=[], unit='m')},
    )

    variables = scipp_to_da00(da)

    assert len(variables) == 2
    data_var = next(var for var in variables if var.name == 'signal')
    assert len(data_var.data) == 0
    assert data_var.shape == (0,)


def test_da00_to_scipp_empty_data():
    """Test conversion from da00 with empty arrays."""
    variables = [
        dataarray_da00.Variable(
            name='signal', data=[], axes=['x'], shape=(0,), unit='counts'
        ),
    ]

    da = da00_to_scipp(variables)

    assert da.data.shape == (0,)
    assert da.data.unit == 'counts'


def test_roundtrip_conversion():
    original = sc.DataArray(
        data=sc.array(
            dims=['x'], values=[1.0, 2.0, 3.0], variances=[1.0, 4.0, 9.0], unit='counts'
        ),
        coords={'x': sc.array(dims=['x'], values=[10, 20, 30], unit='m')},
    )

    da00 = scipp_to_da00(original)
    converted = da00_to_scipp(da00)

    assert sc.identical(original, converted)
