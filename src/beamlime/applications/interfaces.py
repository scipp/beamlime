# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from logging import Logger

from ..communication.broker import CommunicationBroker
from ..config.preset_options import Timeout, WaitInterval
from .mixins import (
    BrokerBasedCommunicationMixin,
    CoroutineMixin,
    FlagControlMixin,
    LogMixin,
)


class BeamlimeApplicationInterface(
    LogMixin, FlagControlMixin, CoroutineMixin, BrokerBasedCommunicationMixin, ABC
):
    """
    Beamlime Application Interface

    Protocol
    --------
    BeamlimeApplicationProtocol
    """

    def __init__(
        self,
        /,
        name: str,
        broker: CommunicationBroker = None,
        logger: Logger = None,
        timeout: float = Timeout.default,
        wait_interval: float = WaitInterval.default,
        **kwargs,
    ) -> None:
        self.app_name = name
        self.broker = broker
        self.set_logger(logger)
        self.timeout = timeout
        self.wait_interval = wait_interval

    @property
    def app_name(self) -> str:
        return self._app_name

    @app_name.setter
    def app_name(self, name: str) -> None:
        from ..config.preset_options import RESERVED_APP_NAME

        if name == RESERVED_APP_NAME:
            raise ValueError(
                f"{name} is a reserved name. "
                "Please use another name for the application."
            )
        self._app_name = name

    @abstractmethod
    async def _run(self) -> None:
        """
        Application coroutine.
        """
        ...

    @abstractmethod
    def __del__(self):
        ...


class BeamlimeDataReductionInterface(BeamlimeApplicationInterface, ABC):
    @abstractmethod
    def process(self):
        pass
