# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from logging import Logger
from queue import Empty
from typing import Any

from ..core.schedulers import async_timeout
from .mixins import CoroutineMixin, FlagControlMixin, LogMixin


class BeamlimeApplicationInterface(LogMixin, FlagControlMixin, CoroutineMixin, ABC):
    """
    Beamlime Application Interface

    Protocol
    --------
    BeamlimeApplicationProtocol
    """

    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        self._pause_interval = 0.1
        self._init_logger(logger=logger)
        self.app_name = kwargs.get("name", "")
        self._input_ch = None
        self._output_ch = None
        self._wait_int = kwargs.get("update_rate", 1)
        self._timeout = kwargs.get(
            "timeout", min(self._wait_int * 10, kwargs.get("max-paused", 1))
        )
        from ..config.preset_options import RESERVED_APP_NAME

        if self.app_name == RESERVED_APP_NAME:
            # TODO: Move this exception raises to earlier point.
            raise ValueError(
                f"{self.app_name} is a reserved name. "
                "Please use another name for the application."
            )
        self.parse_config(config)

    def _init_logger(self, logger=None):
        if isinstance(logger, Logger):
            self.logger = logger
        else:
            from ..logging import get_logger

            self.logger = get_logger()

    @abstractmethod
    def parse_config(self, config: dict) -> None:
        ...

    @abstractmethod
    async def _run(self) -> None:
        """
        Application coroutine.
        """
        ...

    def __del__(self):
        self.stop()
        if hasattr(super(), "__del__"):
            super().__del__()

    @property
    def input_channel(self):
        return self._input_ch

    @input_channel.setter
    def input_channel(self, input_channel):
        self._input_ch = input_channel

    @property
    def output_channel(self):
        return self._output_ch

    @output_channel.setter
    def output_channel(self, output_channel):
        self._output_ch = output_channel

    async def receive_data(self, *args, **kwargs) -> Any:
        @async_timeout(Empty)
        async def _receive_data(timeout: int, wait_interval: int, *args, **kwargs):
            # TODO: Move async_timeout(exception=Empty) to communication handler
            # and remove the decorator or use @async_timeout(exception=TimeoutError).
            return self.input_channel.get(*args, timeout=wait_interval, **kwargs)

        try:
            # TODO: Replace the below line to self.broker.receive_data(...)
            return await _receive_data(
                *args, timeout=self._timeout, wait_interval=self._wait_int, **kwargs
            )
        except TimeoutError:
            return None

    async def send_data(self, data, *args, **kwargs) -> None:
        @async_timeout(Empty)
        async def _send_data(timeout: int, wait_interval: int, *args, **kwargs):
            # TODO: Move async_timeout(exception=Empty) to communication handler
            # and remove the decorator or use @async_timeout(exception=TimeoutError).
            self.output_channel.put(data, *args, timeout=wait_interval, **kwargs)
            return True

        try:
            # TODO: Replace the below line to self.broker.send_data(...)
            return await _send_data(
                *args, timeout=self._timeout, wait_interval=self._wait_int, **kwargs
            )
        except TimeoutError:
            return False


class BeamlimeDataReductionInterface(BeamlimeApplicationInterface, ABC):
    @abstractmethod
    def process(self):
        pass
