# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

"""
This config is used to setup live raw detector views.
Currently the instrument specific config is stored in python files, but
they can be moved to a separate file format in the future.
"""

from .dummy import dummy_detectors_config
from .dream import dream_detectors_config
from .loki import loki_detectors_config
from .nmx import nmx_detectors_config

__all__ = [
    'dummy_detectors_config',
    'dream_detectors_config',
    'loki_detectors_config',
    'nmx_detectors_config',
]
