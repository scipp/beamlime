# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import scipp as sc


def coord_to_dimension(var: sc.Variable) -> hv.Dimension:
    """Create a Holoviews Dimension for the coordinate."""
    dim = var.dim
    unit = str(var.unit) if var.unit is not None else None
    return hv.Dimension(dim, label=dim, unit=unit)


def create_value_dimension(data: sc.DataArray) -> hv.Dimension:
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
    kdims = [coord_to_dimension(coord)]
    vdims = [create_value_dimension(data)]

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
    kdims = [coord_to_dimension(coord)]
    vdims = [create_value_dimension(data)]

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
    kdims = [coord_to_dimension(data.coords[dim]) for dim in reversed(data.dims)]
    vdims = [create_value_dimension(data)]
    coords = [data.coords[dim].values for dim in reversed(data.dims)]

    # QuadMesh expects (x, y, values) format
    return hv.QuadMesh(data=(*coords, data.values), kdims=kdims, vdims=vdims)


def _get_midpoints(data: sc.DataArray, dim: str) -> sc.Variable:
    coord = data.coords[dim]
    if data.coords.is_edges(dim):
        # See https://github.com/scipp/scipp/issues/3765 for why we convert to float64
        return sc.midpoints(coord.to(dtype='float64', copy=False), dim)
    return coord


def convert_image_2d(data: sc.DataArray) -> hv.Image:
    """
    Convert a 2D scipp DataArray to a Holoviews Image.

    This is used when all coordinates are evenly spaced.

    Returns
    -------
    hv.Image
        A Holoviews Image object.
    """
    kdims = [coord_to_dimension(data.coords[dim]) for dim in reversed(data.dims)]
    vdims = [create_value_dimension(data)]
    return hv.Image(
        data=(
            _get_midpoints(data, data.dims[1]).values,
            _get_midpoints(data, data.dims[0]).values,
            data.values,
        ),
        kdims=kdims,
        vdims=vdims,
    )


def _all_coords_evenly_spaced(data: sc.DataArray) -> bool:
    """Check if all coordinates in the DataArray are evenly spaced."""
    for dim in data.dims:
        coord = data.coords[dim]
        if not sc.islinspace(coord):
            return False
    return True


def to_holoviews(
    data: sc.DataArray,
    preserve_edges: bool = False,
) -> hv.Histogram | hv.Curve | hv.QuadMesh | hv.Image:
    """
    Convert a scipp DataArray to a Holoviews object.

    Parameters
    ----------
    data:
        The input scipp DataArray to convert.
    preserve_edges:
        If True, use QuadMesh for 2D data with bin edges instead of Image.
        Default is False, which favors Image for better plotting performance.
        Edges are always preserved for 1D histogram data.

    Returns
    -------
    hv.Histogram | hv.Curve | hv.QuadMesh | hv.Image
        A Holoviews Histogram, Curve, QuadMesh, or Image object.
    """
    if data.dims == ():
        raise ValueError("Input DataArray must have at least one dimension.")

    if len(data.dims) == 1:
        if data.coords.is_edges(data.dim):
            return convert_histogram_1d(data)
        else:
            return convert_curve_1d(data)
    elif len(data.dims) == 2:
        # Check if we have bin edges and user favors QuadMesh
        has_bin_edges = any(data.coords.is_edges(dim) for dim in data.dims)
        if preserve_edges and has_bin_edges:
            return convert_quadmesh_2d(data)
        elif _all_coords_evenly_spaced(data):
            return convert_image_2d(data)
        else:
            return convert_quadmesh_2d(data)
    else:
        raise ValueError("Only 1D and 2D data are supported.")
