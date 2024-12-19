# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
"""
WSGI entry point for gunicorn.

Usage:

    BEAMLIME_INSTRUMENT=dream gunicorn beamlime.services.wsgi:application
"""

from beamlime.core.service import get_env_defaults
from beamlime.services.dashboard_plotly import create_app, setup_arg_parser

parser = setup_arg_parser()
args = get_env_defaults(parser)
application = create_app(**args)
