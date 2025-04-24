# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import scipp as sc

# TODO Unclear if this is transposed or not. Wait for updated files.
dim = 'detector_number'
sizes = {'y': 1280, 'x': 1280}

detectors_config = {
    'detectors': {
        'Panel 0': {
            'detector_name': 'nmx',
            'detector_number': sc.arange(
                dim, 0 * 1280**2 + 1, 1 * 1280**2 + 1,  unit=None
            ).fold(dim=dim, sizes=sizes),
        },
    },
}

