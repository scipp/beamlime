# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for detectors dashboard with gunicorn.

Usage:

    BEAMLIME_INSTRUMENT=dummy gunicorn beamlime.dashboard.detectors_wsgi:application
"""

from beamlime.core.service import get_env_defaults
from beamlime.dashboard.detectors import DashboardApp, get_arg_parser

_args = get_env_defaults(parser=get_arg_parser(), prefix='BEAMLIME')
_app = DashboardApp(**_args)
_app.start(blocking=False)
application = _app.server
