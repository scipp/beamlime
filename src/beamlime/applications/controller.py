# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from typing import NewType

from ..empty_factory import empty_app_factory
from ..logging import BeamlimeLogger
from ..logging.mixins import LogMixin

StartedFlag = NewType("StartedFlag", bool)
default_started = StartedFlag(False)

PausedFlag = NewType("PausedFlag", bool)
default_paused = PausedFlag(False)


class ApplicationNotStartedError(Exception):
    ...


class ApplicationNotResumedError(Exception):
    ...


class ControlInterface(LogMixin):
    """Command Board singleon that all applications will share."""

    logger: BeamlimeLogger
    started: StartedFlag = default_started
    paused: PausedFlag = default_paused

    def start(self):
        self.started = StartedFlag(True)
        self.debug("Start command received.")

    def stop(self):
        self.started = StartedFlag(False)
        self.debug("Stop command received.")

    def pause(self):
        self.paused = StartedFlag(True)
        self.debug("Paused command received.")

    def resume(self):
        self.paused = StartedFlag(False)
        self.debug("Resume command received.")


empty_app_factory.cache_product(ControlInterface, ControlInterface)
