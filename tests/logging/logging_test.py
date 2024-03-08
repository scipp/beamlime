# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from logging import Logger


def test_get_logger_default(local_logger: bool):
    from beamlime.logging import get_logger

    assert local_logger

    default_logger: Logger = get_logger()
    assert default_logger is get_logger()
    assert default_logger.name == "beamlime"


def test_logger_provider(local_logger: bool):
    from beamlime import Factory
    from beamlime.logging import BeamlimeLogger, get_logger
    from beamlime.logging.providers import log_providers

    log_factory = Factory(log_providers)

    assert local_logger
    with log_factory.local_factory() as factory:
        assert factory[BeamlimeLogger] is get_logger()
        assert factory[BeamlimeLogger].name == "beamlime"


def test_get_scipp_logger(local_logger: bool):
    from beamlime.logging import get_scipp_logger

    assert local_logger
    scipp_logger: Logger = get_scipp_logger(widget=False)
    assert scipp_logger.name == "scipp"


def test_get_scipp_logger_set_level(local_logger: bool):
    from logging import DEBUG

    from beamlime.logging import get_scipp_logger

    assert local_logger
    scipp_logger: Logger = get_scipp_logger(log_level=DEBUG, widget=False)
    assert scipp_logger.name == "scipp"
    assert scipp_logger.level == DEBUG
