# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# flake8: noqa F401

import logging

from .logging import get_logger, initialize_file_handler
from .records import BeamlimeColorLogRecord

logging.setLogRecordFactory(BeamlimeColorLogRecord)
