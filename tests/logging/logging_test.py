# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from logging import Logger


def test_get_logger_default():
    from .contexts import local_logger_factory

    with local_logger_factory():
        from beamlime.logging import get_logger

        default_logger: Logger = get_logger()
        assert default_logger is get_logger()
        assert default_logger.name == "beamlime"


def test_logger_provider():
    from .contexts import local_logger_factory

    with local_logger_factory() as factory:
        from beamlime.logging import BeamlimeLogger, get_logger

        assert factory[BeamlimeLogger] is get_logger()
        assert factory[BeamlimeLogger].name == "beamlime"


def test_get_scipp_logger():
    from .contexts import local_logger_factory

    with local_logger_factory():
        from beamlime.logging import get_scipp_logger

        scipp_logger: Logger = get_scipp_logger(widget=False)
        assert scipp_logger.name == "scipp"


def test_get_scipp_logger_set_level():
    from .contexts import local_logger_factory

    with local_logger_factory():
        from logging import DEBUG

        from beamlime.logging import get_scipp_logger

        scipp_logger: Logger = get_scipp_logger(log_level=DEBUG, widget=False)
        assert scipp_logger.name == "scipp"
        assert scipp_logger.level == DEBUG
