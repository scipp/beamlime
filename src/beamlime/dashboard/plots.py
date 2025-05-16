# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import plotly.graph_objects as go
import scipp as sc


def create_monitor_plot(key: str, data: sc.DataArray) -> go.Figure:
    """
    Create a line plot suitable for monitor data.

    Parameters
    ----------
    key
        Plot title and identifier
    data
        1D DataArray to plot
    """
    fig = go.Figure()
    fig.add_scatter(x=[], y=[], mode='lines', line_width=2)
    dim = data.dim
    fig.update_layout(
        title=key,
        width=500,
        height=400,
        xaxis_title=f'{dim} [{data.coords[dim].unit}]',
        yaxis_title=f'[{data.unit}]',
        uirevision=key,
    )
    return fig


def create_detector_plot(key: str, data: sc.DataArray) -> go.Figure:
    """
    Create an appropriate plot for detector data.

    For 1D data, creates a line plot.
    For 2D data, creates a heatmap with appropriate sizing.

    Parameters
    ----------
    key
        Plot title and identifier
    data
        DataArray to plot (1D or 2D)
    """
    if len(data.dims) == 1:
        return create_monitor_plot(key, data)

    fig = go.Figure()
    y_dim, x_dim = data.dims
    fig.add_heatmap(
        z=[[]],
        x=[],  # Will be filled with coordinate values
        y=[],  # Will be filled with coordinate values
        colorscale='Viridis',
    )
    # Add ROI rectangle (initially hidden)
    if not key.startswith('reduced'):  # ROI selection only for raw detector plots
        fig.add_shape(
            type="rect",
            x0=0,
            y0=0,
            x1=1,
            y1=1,
            line={'color': 'red', 'width': 2},
            fillcolor="red",
            opacity=0.2,
            visible=False,
            name="ROI",
        )

    def maybe_unit(dim: str) -> str:
        unit = data.coords[dim].unit
        return f' [{unit}]' if unit is not None else ''

    size = 800
    opts = {
        'title': key,
        'xaxis_title': f'{x_dim}{maybe_unit(x_dim)}',
        'yaxis_title': f'{y_dim}{maybe_unit(y_dim)}',
        'uirevision': key,
        'showlegend': False,
    }
    y_size, x_size = data.shape
    if data.coords[x_dim].unit is not None and (maybe_unit(y_dim) == maybe_unit(x_dim)):
        if y_size < x_size:
            fig.update_layout(width=size, **opts)
            fig.update_yaxes(scaleanchor="x", scaleratio=1, constrain="domain")
            fig.update_xaxes(constrain="domain")
        else:
            fig.update_layout(height=size, **opts)
            fig.update_xaxes(scaleanchor="y", scaleratio=1, constrain="domain")
            fig.update_yaxes(constrain="domain")
    else:
        # Set size based on pixel count
        long = max(y_size, x_size)
        short = min(y_size, x_size)
        ratio = long / short
        max_size = 900
        if ratio > 3:
            if y_size < x_size:
                fig.update_layout(width=max_size, height=max_size // 3, **opts)
            else:
                fig.update_layout(width=max_size // 3, height=max_size, **opts)
        else:
            scale = max_size / long
            fig.update_layout(width=x_size * scale, height=y_size * scale, **opts)
    return fig
