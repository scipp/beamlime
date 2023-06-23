# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import Logger

from beamlime.applications.controller import ControlInterface


class LocalController(ControlInterface):
    """Non-singleton controller for testing."""

    def __new__(cls):
        # del CommandInterface._instances
        return object.__new__(LocalController)

    def __init__(self) -> None:
        self.logger = Logger("controller")


def test_start():
    controller = LocalController()
    assert not controller.started
    controller.start()
    assert controller.started


def test_start_and_stop():
    controller = LocalController()
    controller.start()
    assert controller.started
    controller.stop()
    assert not controller.started


def test_pause():
    controller = LocalController()
    controller.start()
    assert not controller.paused
    controller.pause()
    assert controller.paused


def test_pause_and_resume():
    controller = LocalController()
    controller.pause()
    assert controller.paused
    controller.resume()
    assert not controller.paused


def test_command_provider():
    from beamlime.applications.controller import empty_app_factory
    from beamlime.constructors import Factory

    from ..logging.contexts import local_logger_factory

    with local_logger_factory() as log_factory:
        factory = Factory(log_factory, empty_app_factory)
        controller = factory[ControlInterface]
        assert isinstance(controller, ControlInterface)
        assert factory[ControlInterface] is factory[ControlInterface]
