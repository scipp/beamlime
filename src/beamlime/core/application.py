# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from abc import ABC, abstractmethod, abstractstaticmethod
from queue import Empty
from typing import Union

from colorama import Style
from colorama.ansi import AnsiBack, AnsiFore, AnsiStyle


class BeamlimeApplicationInterface(ABC):
    _input_ch = None
    _output_ch = None

    def __init__(
        self,
        config: dict,
        verbose: bool = False,
        verbose_option: Union[AnsiFore, AnsiStyle, AnsiBack, str] = Style.RESET_ALL,
    ) -> None:
        self.parse_config(config)
        self.verbose = verbose
        self.verbose_option = verbose_option

    @abstractmethod
    def parse_config(self, config: dict) -> None:
        pass

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
            return None

    async def send_data(self, data, *args, **kwargs) -> None:
        try:
            self.output_channel.put(data, *args, **kwargs)
            return True
        except:  # noqa: E722,B001
            # TODO: Update I/O interface to have common exception.
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
