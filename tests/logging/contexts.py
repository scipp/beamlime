# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def local_loggers() -> Iterator[None]:
    """
    Keep a copy of logger names in logging.Logger.manager.loggerDict
    and remove newly added loggers at the end of the context
    with a sub context beamlime.constructors.local_providers.

    It will help a test not to interfere other tests.
    """
    from beamlime.constructors import local_providers

    original_logger_names = list(logging.Logger.manager.loggerDict.keys())
    try:
        with local_providers():
            yield None
    finally:
        extra_logger_names = [
            logger_name
            for logger_name in logging.Logger.manager.loggerDict.keys()
            if logger_name not in original_logger_names
        ]
        for extra_name in extra_logger_names:
            del logging.Logger.manager.loggerDict[extra_name]
