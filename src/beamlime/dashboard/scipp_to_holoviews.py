# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import scipp as sc


def _create_coord_dimension(data: sc.DataArray) -> hv.Dimension:
    """Create a Holoviews Dimension for the coordinate."""
    dim = data.dim
    coord = data.coords[dim]
    label = f"{dim} [{coord.unit}]" if coord.unit is not None else dim
    return hv.Dimension(dim, label=label)


def _create_value_dimension(data: sc.DataArray) -> hv.Dimension:
    """Create a Holoviews Dimension for the values."""
    parts = []
    if data.name:
        parts.append(data.name)
    if data.unit is not None:
        parts.append(f"[{data.unit}]")
    label = ' '.join(parts) if parts else 'values'
    return hv.Dimension('values', label=label)


def _create_coord_dimensions_2d(data: sc.DataArray) -> list[hv.Dimension]:
    """Create Holoviews Dimensions for 2D coordinates."""
    dims = []
    for dim in reversed(data.dims):
        coord = data.coords[dim]
        label = f"{dim} [{coord.unit}]" if coord.unit is not None else dim
        dims.append(hv.Dimension(dim, label=label))
    return dims


def convert_histogram_1d(data: sc.DataArray) -> hv.Histogram:
    """
    Convert a 1D scipp DataArray to a Holoviews Histogram.

    Returns
    -------
    hv.Histogram
        A Holoviews Histogram object.
    """
    dim = data.dim
    coord = data.coords[dim]
    kdims = [_create_coord_dimension(data)]
    vdims = [_create_value_dimension(data)]

    return hv.Histogram(data=(coord.values, data.values), kdims=kdims, vdims=vdims)


def convert_curve_1d(data: sc.DataArray) -> hv.Curve:
    """
    Convert a 1D scipp DataArray to a Holoviews Curve.

    Returns
    -------
    hv.Curve
        A Holoviews Curve object.
    """
    dim = data.dim
    coord = data.coords[dim]
    kdims = [_create_coord_dimension(data)]
    vdims = [_create_value_dimension(data)]

    return hv.Curve(data=(coord.values, data.values), kdims=kdims, vdims=vdims)


def convert_quadmesh_2d(data: sc.DataArray) -> hv.QuadMesh:
    """
    Convert a 2D scipp DataArray to a Holoviews QuadMesh.

    This supports non-evenly spaced coordinates.

    Returns
    -------
    hv.QuadMesh
        A Holoviews QuadMesh object.
    """
    kdims = _create_coord_dimensions_2d(data)
    vdims = [_create_value_dimension(data)]
    coords = [data.coords[dim].values for dim in reversed(data.dims)]

    # QuadMesh expects (x, y, values) format
    return hv.QuadMesh(data=(*coords, data.values), kdims=kdims, vdims=vdims)


def to_holoviews(data: sc.DataArray) -> hv.Histogram | hv.Curve | hv.QuadMesh:
    """
    Convert a scipp DataArray to a Holoviews object.

    Parameters
    ----------
    data : sc.DataArray
        The input scipp DataArray to convert.

    Returns
    -------
    hv.Histogram | hv.Curve | hv.QuadMesh
        A Holoviews Histogram, Curve, or QuadMesh object.
    """
    if data.dims == ():
        raise ValueError("Input DataArray must have at least one dimension.")

    if len(data.dims) == 1:
        if data.coords.is_edges(data.dim):
            return convert_histogram_1d(data)
        else:
            return convert_curve_1d(data)
    elif len(data.dims) == 2:
        return convert_quadmesh_2d(data)
    else:
        raise ValueError("Only 1D and 2D data are supported.")
