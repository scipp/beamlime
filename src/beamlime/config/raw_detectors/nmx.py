# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

from ess.reduce.live import raw

nmx_detectors_config = {
    'dashboard': {'nrow': 1, 'ncol': 3},
    'detectors': {
        'Panel 0': {
            'detector_name': 'detector_panel_0',
            'gridspec': (0, 0),
            'projection': raw.LogicalView(),
        },
        'Panel 1': {
            'detector_name': 'detector_panel_1',
            'gridspec': (0, 1),
            'projection': raw.LogicalView(),
        },
        'Panel 2': {
            'detector_name': 'detector_panel_2',
            'gridspec': (0, 2),
            'projection': raw.LogicalView(),
        },
    },
}
