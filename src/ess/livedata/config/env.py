# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Environment configuration for streaming.
"""

from enum import Enum, auto


class StreamingEnv(Enum):
    """Environment for streaming data."""

    DEV = auto()
    PROD = auto()
