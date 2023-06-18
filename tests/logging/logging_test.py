# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import scipp as _  # noqa F401

# TODO: Update local_provider context


def test_get_logger_default():
    from .contexts import local_loggers

    with local_loggers():
        from beamlime.logging import get_logger

        default_logger = get_logger()
        assert default_logger is get_logger()
        assert default_logger.name == "beamlime"


def test_logger_provider():
    from beamlime.constructors import Container

    from .contexts import local_loggers

    with local_loggers():
        from beamlime.logging import BeamlimeLogger, get_logger

        assert Container[BeamlimeLogger] is get_logger()
        assert Container[BeamlimeLogger].name == "beamlime"


def test_get_scipp_logger():
    from .contexts import local_loggers

    with local_loggers():
        from beamlime.logging import get_scipp_logger

        scipp_logger = get_scipp_logger(widget=False)
        assert scipp_logger.name == "scipp"


def test_get_scipp_logger_set_level():
    from .contexts import local_loggers

    with local_loggers():
        from logging import DEBUG

        from beamlime.logging import get_scipp_logger

        scipp_logger = get_scipp_logger(log_level=DEBUG, widget=False)
        assert scipp_logger.name == "scipp"
        assert scipp_logger.level == DEBUG
