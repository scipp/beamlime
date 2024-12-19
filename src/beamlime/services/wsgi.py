# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for gunicorn.

Usage:

    BEAMLIME_INSTRUMENT=dream gunicorn beamlime.services.wsgi:application
"""

import os

from beamlime.services.dashboard_plotly import create_app

# Get config from environment
instrument = os.getenv('BEAMLIME_INSTRUMENT', 'dummy')
log_level = int(os.getenv('BEAMLIME_LOG_LEVEL', '20'))  # 20 is INFO

application = create_app(instrument=instrument, debug=False, log_level=log_level)
