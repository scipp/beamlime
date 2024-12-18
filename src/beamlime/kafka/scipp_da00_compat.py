# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc
from streaming_data_types import dataarray_da00


def scipp_to_da00(
    da: sc.DataArray, *, signal_name: str = 'signal'
) -> list[dataarray_da00.Variable]:
    if da.variances is None:
        variables = [_to_da00_variable(signal_name, da.data)]
    else:
        variables = [
            _to_da00_variable(signal_name, sc.values(da.data)),
            _to_da00_variable('errors', sc.stddevs(da.data)),
        ]
    variables.extend([_to_da00_variable(name, var) for name, var in da.coords.items()])
    return variables


def da00_to_scipp(
    variables: list[dataarray_da00.Variable], *, signal_name: str = 'signal'
) -> sc.DataArray:
    variables = {var.name: _to_scipp_variable(var) for var in variables}
    data = variables.pop(signal_name)
    if (errors := variables.pop('errors', None)) is not None:
        data.variances = (errors**2).values
    return sc.DataArray(data, coords=variables)


def _to_da00_variable(name: str, var: sc.Variable) -> dataarray_da00.Variable:
    return dataarray_da00.Variable(
        name=name,
        data=var.values,
        axes=list(var.dims),
        shape=var.shape,
        unit=str(var.unit),
    )


def _to_scipp_variable(var: dataarray_da00.Variable) -> sc.Variable:
    return sc.array(dims=var.axes, values=var.data, unit=var.unit)
