# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for reduction dashboard with gunicorn.

Usage:

    BEAMLIME_INSTRUMENT=dummy gunicorn beamlime.dashboard.reduction_wsgi:application
"""

from beamlime import Service
from beamlime.core.service import get_env_defaults
from beamlime.dashboard.reduction import ReductionApp

_args = get_env_defaults(parser=Service.setup_arg_parser(), prefix='BEAMLIME')
_app = ReductionApp(**_args)
_app.start(blocking=False)
application = _app.server
