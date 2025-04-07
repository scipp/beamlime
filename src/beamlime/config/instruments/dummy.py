# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Detector configuration for a dummy instrument used for development and testing.
"""

import scipp as sc

detectors_config = {
    'detectors': {
        'Panel 0': {
            'detector_name': 'panel_0',
            'detector_number': sc.arange('yx', 0, 128**2, unit=None).fold(
                dim='yx', sizes={'y': -1, 'x': 128}
            ),
        },
    },
}
