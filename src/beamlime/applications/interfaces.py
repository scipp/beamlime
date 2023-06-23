# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Callable, NewType

from ..core.schedulers import async_retry
from ..logging.mixins import LogMixin
from .controller import (
    ApplicationNotResumedError,
    ApplicationNotStartedError,
    ControlInterface,
)

Timeout = NewType("Timeout", float)
WaitInterval = NewType("WaitInterval", float)


class CoroutineInterface(ABC):
    """
    Application coroutine interfaces.

    Protocol
    --------
    BeamlimeCoroutineProtocol
    """

    _command: ControlInterface
    timeout: Timeout = 10
    wait_interval: WaitInterval = 1

    async def _check_raise(
        self,
        status_indicator: Callable,
        *exceptions,
        wait_on_success: bool = False,
    ) -> bool:
        """
        Check if ``status_indicator`` raises errors.
        When an expected error occurs, it will wait for the ``wait_interval``
        and try the check again until it reaches maximum trials.
        """
        # TODO: Handle maximum of max_trials and minimum of wait_interval.
        max_trials = int(self.timeout / self.wait_interval) + 1
        _check = async_retry(
            *exceptions, max_trials=max_trials, interval=self.wait_interval
        )(status_indicator)

        await _check()
        if wait_on_success:
            await asyncio.sleep(self.wait_interval)

    async def _raise_if_stopped(self):
        if not self._command.started:
            raise ApplicationNotStartedError

    async def can_start(self, wait_on_true: bool = False) -> bool:
        """Check if ``started``."""
        try:
            await self._check_raise(
                self._raise_if_stopped,
                ApplicationNotStartedError,
                wait_on_success=wait_on_true,
            )
        except ApplicationNotStartedError:
            return False
        else:
            return True

    async def _raise_if_paused(self):
        if not self._command.started:
            raise ApplicationNotStartedError
        elif self._command.paused:
            raise ApplicationNotResumedError

    async def running(self, wait_on_true: bool = False) -> bool:
        """Check if ``started`` and not ``paused``."""
        try:
            await self._check_raise(
                self._raise_if_paused,
                ApplicationNotResumedError,
                wait_on_success=wait_on_true,
            )
        except (ApplicationNotStartedError, ApplicationNotResumedError):
            return False
        else:
            return True

    @abstractmethod
    async def run(self) -> None:
        """
        Application coroutine.
        """
        ...  # pragma: no cover


class BeamlimeApplicationInterface(
    LogMixin,
    CoroutineInterface,
):
    """
    Beamlime Application Interface

    Protocol
    --------
    BeamlimeApplicationProtocol
    """
