# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc

# TODO Unclear if this is transposed or not. Wait for updated files.
dim = 'detector_number'
sizes = {'x': 1280, 'y': 1280}
detectors_config = {
    'detectors': {
        'Panel 0': {
            'detector_name': 'detector_panel_0',
            'detector_number': sc.arange(
                dim, 0 * 1280**2 + 1, 1 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
        },
        'Panel 1': {
            'detector_name': 'detector_panel_1',
            'detector_number': sc.arange(
                dim, 1 * 1280**2 + 1, 2 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
        },
        'Panel 2': {
            'detector_name': 'detector_panel_2',
            'detector_number': sc.arange(
                dim, 2 * 1280**2 + 1, 3 * 1280**2 + 1, unit=None
            ).fold(dim=dim, sizes=sizes),
        },
    },
}
