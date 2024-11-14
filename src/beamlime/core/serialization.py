# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import msgpack
import numpy as np
import scipp as sc


def serialize_variable(var: sc.Variable) -> bytes:
    content = {
        'dims': var.dims,
        'shape': var.shape,
        'data': var.values.tobytes(),
        'dtype': var.values.dtype.str,
    }
    if var.unit is not None:
        content['unit'] = str(var.unit)
    return msgpack.packb(content)


def deserialize_variable(data: bytes) -> sc.Variable:
    unpacked = msgpack.unpackb(data)
    values = np.frombuffer(unpacked['data'], dtype=np.dtype(unpacked['dtype'])).reshape(
        unpacked['shape']
    )
    return sc.array(dims=unpacked['dims'], values=values, unit=unpacked.get('unit'))


def serialize_data_array(da: sc.DataArray) -> bytes:
    return msgpack.packb(
        {
            'data': serialize_variable(da.data),
            'coords': {k: serialize_variable(v) for k, v in da.coords.items()},
            'masks': {k: serialize_variable(v) for k, v in da.masks.items()},
        }
    )


def deserialize_data_array(data: bytes) -> sc.DataArray:
    unpacked = msgpack.unpackb(data)
    return sc.DataArray(
        data=deserialize_variable(unpacked['data']),
        coords={k: deserialize_variable(v) for k, v in unpacked['coords'].items()},
        masks={k: deserialize_variable(v) for k, v in unpacked['masks'].items()},
    )
