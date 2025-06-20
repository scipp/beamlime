# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import scipp as sc


def _coord_to_dimension(var: sc.Variable) -> hv.Dimension:
    """Create a Holoviews Dimension for the coordinate."""
    dim = var.dim
    unit = str(var.unit) if var.unit is not None else None
    return hv.Dimension(dim, label=dim, unit=unit)


def _create_value_dimension(data: sc.DataArray) -> hv.Dimension:
    """Create a Holoviews Dimension for the values."""
    label = data.name if data.name else 'values'
    unit = str(data.unit) if data.unit is not None else None
    return hv.Dimension('values', label=label, unit=unit)


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
    kdims = [_coord_to_dimension(coord)]
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
    kdims = [_coord_to_dimension(coord)]
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
    kdims = [_coord_to_dimension(data.coords[dim]) for dim in reversed(data.dims)]
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
