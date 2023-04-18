# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from abc import ABC, abstractmethod, abstractstaticmethod
from logging import DEBUG, ERROR, INFO, WARN
from queue import Empty
from typing import Protocol

from ..logging.loggers import BeamlimeLogger


class BeamLimeApplicationProtocol(Protocol):
    @property
    def input_channel(self) -> object:
        ...

    @property
    def output_channel(self) -> object:
        ...

    @input_channel.setter
    def input_channel(self, channel) -> None:
        ...

    @output_channel.setter
    def output_channel(self, channel) -> None:
        ...

    def start(self) -> None:
        ...

    def pause(self) -> None:
        ...

    def resume(self) -> None:
        ...

    def __del__(self) -> None:
        ...


class _LogMixin:
    """Logging interfaces for Beamlime Applications"""

    def _log(self, level: int, msg: str, args: tuple):
        if isinstance(self.logger, BeamlimeLogger):
            self.logger._log(level=level, msg=msg, args=args, app_name=self.app_name)
        else:
            if not self.logger.isEnabledFor(level):
                return
            from ..logging.formatters import EXTERNAL_MESSAGE_HEADERS

            self.logger._log(
                level=level,
                msg=EXTERNAL_MESSAGE_HEADERS.fmt % (self.app_name, msg),
                args=args,
                extra={"app_name": self.app_name},
            )

    def debug(self, msg: str, *args) -> None:
        self._log(level=DEBUG, msg=msg, args=args)

    def info(self, msg: str, *args) -> None:
        self._log(level=INFO, msg=msg, args=args)

    def warn(self, msg: str, *args) -> None:
        self._log(level=WARN, msg=msg, args=args)

    def exception(self, msg: str, *args) -> None:
        self._log(level=ERROR, msg=msg, args=args)

    def error(self, msg: str, *args) -> None:
        self._log(level=ERROR, msg=msg, args=args)


class BeamlimeApplicationInterface(_LogMixin, ABC):
    _input_ch = None
    _output_ch = None

    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        self._init_logger(logger=logger)
        self.app_name = kwargs.get("name", "")
        from ..config.preset_options import RESERVED_APP_NAME

        if self.app_name == RESERVED_APP_NAME:
            # TODO: Move this exception raises to earlier point.
            raise ValueError(
                f"{self.app_name} is a reserved name. "
                "Please use another name for the application."
            )
        self.parse_config(config)

    def _init_logger(self, logger=None):
        if logger is None:
            from ..logging import get_logger

            self.logger = get_logger()
        else:
            self.logger = logger

    @abstractmethod
    def parse_config(self, config: dict) -> None:
        ...

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

    async def receive_data(self, *args, **kwargs):
        try:
            return self.input_channel.get(*args, **kwargs)
        except Empty:
            # TODO: Update I/O interface to have common exception.
            self.info("Data not received from the input channel.")
            return None

    async def send_data(self, data, *args, **kwargs) -> None:
        try:
            self.output_channel.put(data, *args, **kwargs)
            return True
        except:  # noqa: E722,B001
            # TODO: Update I/O interface to have common exception.
            self.info("Data not sent to the output channel.")
            self.debug("Failed data: %s", str(data))
            return False

    @abstractstaticmethod
    async def _run(self) -> None:
        """
        Application coroutine generator.
        ``self`` is passed as an argument since the coroutine
        doesn't have the access to the ``cls`` or ``self`` in a normal way.
        as a ``classmethod`` or a member funcition.
        Here is the example below.

        received_data = await self.receive_data()
        result = ... process data ...
        await self.send_data(result)
        if self.verbose:
            print(f"{self.verbose_option}"
                   "...sth to report..."
                  f"{Style.RESET_ALL}")
        """
        pass

    def create_task(self):
        return asyncio.create_task(self._run(self))

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def pause(self) -> None:
        pass

    @abstractmethod
    def resume(self) -> None:
        pass

    @abstractmethod
    def __del__(self) -> None:
        pass


class BeamLimeDataReductionInterface(BeamlimeApplicationInterface, ABC):
    @abstractmethod
    def process(self):
        pass
