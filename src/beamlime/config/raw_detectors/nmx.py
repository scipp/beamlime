# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc
from ess.reduce.live import raw

# TODO Unclear if this is transposed or not. Wait for updated files.
dim = 'detector_number'
sizes = {'x': 1280, 'y': 1280}
nmx_detectors_config = {
    'dashboard': {'nrow': 1, 'ncol': 3},
    'detectors': {
        'Panel 0': {
            'detector_name': 'detector_panel_0',
            'detector_number': sc.arange(
                dim, 0 * 1280**2 + 1, 1 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
            'gridspec': (0, 0),
            'projection': raw.LogicalView(),
        },
        'Panel 1': {
            'detector_name': 'detector_panel_1',
            'detector_number': sc.arange(
                dim, 1 * 1280**2 + 1, 2 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
            'gridspec': (0, 1),
            'projection': raw.LogicalView(),
        },
        'Panel 2': {
            'detector_name': 'detector_panel_2',
            'detector_number': sc.arange(
                dim, 2 * 1280**2 + 1, 3 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
            'gridspec': (0, 2),
            'projection': raw.LogicalView(),
        },
    },
}
