# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for reduction dashboard with gunicorn.

Usage:

    LIVEDATA_INSTRUMENT=dummy gunicorn ess.livedata.dashboard.reduction_wsgi:application
"""

from ess.livedata.core.service import get_env_defaults
from ess.livedata.dashboard.reduction import ReductionApp, get_arg_parser

_args = get_env_defaults(parser=get_arg_parser(), prefix='LIVEDATA')
_app = ReductionApp(**_args)
_app.start(blocking=False)
application = _app.server
