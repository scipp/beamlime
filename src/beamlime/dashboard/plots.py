# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""This file contains utilities for creating plots in the dashboard."""

import holoviews as hv
import numpy as np
from holoviews import opts

from .subscribers import RawData


def monitor_total_counts_bar_chart(**monitors: RawData | None) -> hv.Bars:
    """Create bar chart showing total counts from all monitors."""
    totals = [
        (name, np.nan if monitor is None else np.sum(monitor.current.values))
        for name, monitor in monitors.items()
    ]
    bars = hv.Bars(reversed(totals), kdims='Monitor', vdims='Total Counts')

    return bars.opts(  # pyright: ignore[reportReturnType]
        opts.Bars(
            title="",
            height=50 + 30 * len(totals),
            color='lightblue',
            ylabel="Total Counts",
            xlabel="",
            invert_axes=True,
            show_legend=False,
            toolbar=None,
            responsive=True,
            xformatter='%.1e',
            xrotation=25,
        )
    )
