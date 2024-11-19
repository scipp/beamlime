# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc

from beamlime.core.serialization import (
    deserialize_data_array,
    deserialize_variable,
    serialize_data_array,
    serialize_variable,
)


def test_variable_roundtrip() -> None:
    var = sc.arange('x', 10, unit='m').fold('x', sizes={'y': 2, 'z': 5})
    serialized = serialize_variable(var)
    result = deserialize_variable(serialized)
    assert sc.identical(var, result)


def test_data_array_roundtrip() -> None:
    da = sc.DataArray(
        data=sc.arange('xy', 10, unit='m').fold('xy', sizes={'x': 2, 'y': 5}),
        coords={'x': sc.array(dims=['x'], values=[1.0, 2.0], unit='s')},
        masks={'mask': sc.array(dims=['y'], values=[False, True, False, True, False])},
    )
    serialized = serialize_data_array(da)
    result = deserialize_data_array(serialized)
    assert sc.identical(da, result)
