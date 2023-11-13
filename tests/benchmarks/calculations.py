# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from collections import namedtuple
from typing import Any, Callable, Generator, Optional

import scipp as sc


def _value_or_nan(value: Any) -> Any:
    """Returns ``value`` if it is not ``None`` otherwise ``numpy.NaN``."""
    import numpy as np

    return value if value is not None else np.NaN


def list_to_scipp_scalar_column(values: list[Any]) -> list[sc.Variable]:
    """Return a list of ``scipp.Variable`` from a list of numbers or string.

    If ``value`` is ``None``, it will be replaced with ``numpy.NaN``.
    """
    return [sc.scalar(_value_or_nan(value), unit=None) for value in values]


def dict_to_scipp_scalar_column(value_unit: dict[str, list]) -> list[sc.Variable]:
    """Return a list of ``scipp.Variable`` from lists of ``value`` and ``unit``.

    If ``value`` is ``None``, it will be replaced with ``numpy.NaN``.
    """
    return [
        sc.scalar(_value_or_nan(value), unit=unit)
        for value, unit in zip(value_unit['value'], value_unit["unit"])
    ]


def sample_variance(da: sc.DataArray) -> float:
    import numpy as np

    if len(da) <= 1:
        return np.nan
    return (sc.sum((da - da.mean()) ** 2) / (len(da) - 1)).value


def _get_bin(binned: sc.DataArray, *dim_idx: tuple[str, int]) -> sc.DataArray:
    if not dim_idx:
        return binned
    return _get_bin(binned[dim_idx[0][0], dim_idx[0][1]], *dim_idx[1:])


def _set_value(da: sc.DataArray, value: Any, *dim_idx: tuple[str, int]) -> None:
    if not dim_idx:
        raise ValueError("No dimension and index pair was given.")
    elif len(dim_idx) == 1:
        da[dim_idx[0][0], dim_idx[0][1]] = value
    else:
        child = da[dim_idx[0][0], dim_idx[0][1]]
        return _set_value(child, value, *dim_idx[1:])


def grid_like(binned: sc.DataArray, unit: Optional[str] = None) -> sc.DataArray:
    """Create a data array with ``binned`` coordinates and data with size of bins.

    The data of the returned array is filled with ``NaN``.

    Raises
    ------
    ValueError
        If ``binned.bins`` is ``None``.

    """
    import numpy as np

    if binned.bins is None:
        raise ValueError("Bins not found.")

    return sc.DataArray(
        data=sc.full(
            value=np.nan, sizes=(bin_size := binned.bins.size()).sizes, unit=unit
        ),
        coords=bin_size.coords,
    )


DimensionIndex = namedtuple("DimensionIndex", [('dimension', str), ('index', int)])


def collect_dimension_indexes(
    multi_dim_da: sc.DataArray,
) -> Generator[list[DimensionIndex], None, None]:
    return (
        [DimensionIndex(dim, i_size) for i_size in range(size)]
        for dim, size in multi_dim_da.sizes.items()
    )


def cast_operation_per_bin(
    binned: sc.DataArray,
    operation: Callable[[sc.DataArray], Any],
    unit_func: Optional[Callable[[sc.DataArray], Any]] = None,
) -> sc.DataArray:
    """Cast ``operation`` per bin, (data array view) and returns the result.

    ``unit_func`` retrieves the unit of output from the unit of a single bin.
    If ``unit_func`` is not given, ``operation`` will be used.

    Example
    -------
    >>> binned = sc.data.binned_xy(nevent=100, nx=10, ny=10)
    >>> casted = cast_operation_per_bin(binned, operation=sc.sum, unit_func=lambda _: _)
    >>> casted.sizes
    {'x': 10, 'y': 10}
    >>> casted.unit
    Unit(K)
    >>> sc.identical(binned.bins.sum(), casted)
    True

    """
    from itertools import product

    unit_operation = unit_func or operation
    container = grid_like(binned, unit=unit_operation(binned.values[0].unit))

    for dim_indices in product(*collect_dimension_indexes(container)):
        cur_bin = _get_bin(binned, *dim_indices).values  # A single DataArrayView
        cur_value = operation(cur_bin)

        _set_value(container, cur_value, *dim_indices)

    return container


def sample_variance_per_bin(binned: sc.DataArray) -> sc.DataArray:
    return cast_operation_per_bin(binned, sample_variance, unit_func=lambda _: None)


def sample_mean_per_bin(binned: sc.DataArray) -> sc.DataArray:
    return cast_operation_per_bin(binned, sc.mean, unit_func=lambda x: x)
