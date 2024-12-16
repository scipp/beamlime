# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Generator
from typing import Any, NamedTuple, TypeVar

import scipp as sc

T = TypeVar("T")


def sample_variance(da: sc.DataArray) -> sc.Variable:
    import numpy as np

    if len(da) <= 1:
        return sc.scalar(np.nan, unit=da.unit)

    dof = sc.scalar(len(da) - 1, unit=da.unit)
    return sc.sum((da - da.mean()) ** 2) / dof


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


def grid_like(binned: sc.DataArray, unit: str | None = None) -> sc.DataArray:
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


class DimensionIndex(NamedTuple):
    dimension: str
    index: int


def collect_dimension_indexes(
    multi_dim_da: sc.DataArray,
) -> Generator[list[DimensionIndex], None, None]:
    return (
        [DimensionIndex(dim, i_size) for i_size in range(size)]
        for dim, size in multi_dim_da.sizes.items()
    )


def cast_operation_per_bin(
    binned: sc.DataArray,
    operation: Callable[[sc.DataArray], sc.Variable | sc.DataArray],
) -> sc.DataArray:
    """Cast ``operation`` per bin, (data array view) and returns the result.

    Example
    -------
    >>> binned = sc.data.binned_xy(nevent=100, nx=10, ny=10)
    >>> casted = cast_operation_per_bin(binned, operation=sc.sum)
    >>> casted.sizes
    {'x': 10, 'y': 10}
    >>> casted.unit
    Unit(K)
    >>> sc.identical(binned.bins.sum(), casted)
    True
    """
    from itertools import product

    container = grid_like(binned)
    values = {
        tuple(dim_indices): operation(_get_bin(binned, *dim_indices).values)
        for dim_indices in product(*collect_dimension_indexes(container))
    }
    container.unit = next(value.unit for value in values.values())

    for dim_indices, value in values.items():
        _set_value(container, value, *dim_indices)

    return container


def sample_variance_per_bin(binned: sc.DataArray) -> sc.DataArray:
    return cast_operation_per_bin(binned, sample_variance)


def sample_mean_per_bin(binned: sc.DataArray) -> sc.DataArray:
    return cast_operation_per_bin(binned, sc.mean)
