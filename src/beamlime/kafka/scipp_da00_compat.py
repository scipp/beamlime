# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc
from streaming_data_types import dataarray_da00


def scipp_to_da00(da: sc.DataArray) -> list[dataarray_da00.Variable]:
    if da.variances is None:
        variables = [_to_da00_variable('data', da.data)]
    else:
        variables = [
            _to_da00_variable('data', sc.values(da.data)),
            _to_da00_variable('errors', sc.stddevs(da.data)),
        ]
    variables.extend([_to_da00_variable(name, var) for name, var in da.coords.items()])
    return variables


def da00_to_scipp(vars: list[dataarray_da00.Variable]) -> sc.DataArray:
    vars = {var.name: _to_scipp_variable(var) for var in vars}
    data = vars.pop('data')
    if (errors := vars.pop('errors', None)) is not None:
        data.variances = (errors**2).values
    return sc.DataArray(data, coords=vars)


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
