# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from beamlime.complete_binders import LoggingBinder
from beamlime.constructors import Binder, context_binder


@contextmanager
def local_logger_binder() -> Iterator[Binder]:
    """
    Keep a copy of logger names in logging.Logger.manager.loggerDict
    and remove newly added loggers at the end of the context
    within the sub context using ``LoggingBinder``.

    It will help a test not to interfere other tests.
    """

    original_logger_names = list(logging.Logger.manager.loggerDict.keys())
    try:
        with context_binder(LoggingBinder) as binder:
            yield binder
    finally:
        extra_logger_names = [
            logger_name
            for logger_name in logging.Logger.manager.loggerDict.keys()
            if logger_name not in original_logger_names
        ]
        for extra_name in extra_logger_names:
            del logging.Logger.manager.loggerDict[extra_name]
