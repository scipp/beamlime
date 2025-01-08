# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for gunicorn.

Usage:

    BEAMLIME_INSTRUMENT=dream gunicorn beamlime.services.wsgi:application
"""

from beamlime.core.service import get_env_defaults
from beamlime.services.dashboard import DashboardApp, setup_arg_parser

_args = get_env_defaults(parser=setup_arg_parser(), prefix='BEAMLIME')
_app = DashboardApp(**_args)
_app.start(blocking=False)
application = _app.server
