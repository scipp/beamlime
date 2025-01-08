# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import numpy as np
import scipp as sc
from streaming_data_types import dataarray_da00

from beamlime.kafka.scipp_da00_compat import da00_to_scipp, scipp_to_da00


def test_scipp_to_da00_basic():
    # Create simple DataArray
    da = sc.DataArray(
        data=sc.array(dims=['x'], values=[1, 2, 3], unit='counts'),
        coords={'x': sc.array(dims=['x'], values=[10, 20, 30], unit='m')},
    )

    # Convert to da00
    variables = scipp_to_da00(da)

    # Check results
    assert len(variables) == 2  # data and x coord
    data_var = next(var for var in variables if var.name == 'signal')
    x_var = next(var for var in variables if var.name == 'x')

    assert data_var.unit == 'counts'
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
            name='signal', data=[1, 2, 3], axes=['x'], shape=[3], unit='counts'
        ),
        dataarray_da00.Variable(
            name='x', data=[10, 20, 30], axes=['x'], shape=[3], unit='m'
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
