# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo


def test_get_logger_default():
    from beamlime.logging import get_logger
    from beamlime.logging.loggers import BeamlimeLogger

    default_logger = get_logger()
    assert default_logger is get_logger()
    assert isinstance(default_logger, BeamlimeLogger)
    assert default_logger.name == "beamlime"


def test_get_logger_general():
    from logging import Logger

    from beamlime.logging import get_logger

    general_logger = get_logger("anonymous", Logger)
    assert isinstance(general_logger, Logger)
    assert general_logger.name == "anonymous"
    assert general_logger is get_logger("anonymous", Logger)
