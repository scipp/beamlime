# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from beamlime.constructors import Factory
from beamlime.ready_factory import log_factory


@contextmanager
def local_logger_factory() -> Iterator[Factory]:
    """
    Keep a copy of logger names in logging.Logger.manager.loggerDict
    and remove newly added loggers at the end of the context
    within the sub context using ``LoggingBinder``.

    It will help a test not to interfere other tests.
    """

    original_logger_names = list(logging.Logger.manager.loggerDict.keys())
    try:
        with log_factory.local_factory() as local_log_factory:
            yield local_log_factory
    finally:
        extra_logger_names = [
            logger_name
            for logger_name in logging.Logger.manager.loggerDict.keys()
            if logger_name not in original_logger_names
        ]
        for extra_name in extra_logger_names:
            del logging.Logger.manager.loggerDict[extra_name]
