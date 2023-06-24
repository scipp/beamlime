# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Accessible binders with all necessary providers filled in.
# flake8: noqa F401

from .applications.providers import app_factory
from .logging.providers import empty_log_factory as log_factory
