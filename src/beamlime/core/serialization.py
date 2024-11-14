# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Any

import numpy as np
import scipp as sc


def serialize_variable(var: sc.Variable) -> dict[str, Any]:
    content = {
        'dims': var.dims,
        'shape': var.shape,
        'data': var.values.tobytes(),
        'dtype': var.values.dtype.str,
    }
    if var.unit is not None:
        content['unit'] = str(var.unit)
    return content


def deserialize_variable(data: dict[str, Any]) -> sc.Variable:
    values = np.frombuffer(data['data'], dtype=np.dtype(data['dtype'])).reshape(
        data['shape']
    )
    return sc.array(dims=data['dims'], values=values, unit=data.get('unit'))


def serialize_data_array(da: sc.DataArray) -> dict[str, Any]:
    return {
        'data': serialize_variable(da.data),
        'coords': {k: serialize_variable(v) for k, v in da.coords.items()},
        'masks': {k: serialize_variable(v) for k, v in da.masks.items()},
    }


def deserialize_data_array(data: bytes) -> sc.DataArray:
    return sc.DataArray(
        data=deserialize_variable(data['data']),
        coords={k: deserialize_variable(v) for k, v in data['coords'].items()},
        masks={k: deserialize_variable(v) for k, v in data['masks'].items()},
    )
