# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from abc import ABC, abstractmethod, abstractstaticmethod
from logging import DEBUG, ERROR, INFO, WARN, Logger
from queue import Empty
from typing import Callable, Optional, TypeVar

from ..logging.loggers import BeamlimeLogger
from .protocols import (
    BeamLimeApplicationProtocol,
    BeamlimeDownstreamProtocol,
    BeamlimeTwoWayProtocol,
    BeamlimeUpstreamProtocol,
)

_CommunicationChannel = TypeVar(
    "_CommunicationChannel",
    BeamlimeDownstreamProtocol,
    BeamlimeUpstreamProtocol,
    BeamlimeTwoWayProtocol,
)

MAX_INSTANCE_NUMBER = 2
# TODO: Implement multi-process/multi-machine instance group interface.


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

    def warning(self, msg: str, *args) -> None:
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

    async def receive_data(self, *args, timeout=10, waited=0):
        if waited >= timeout:
            # TODO: Update I/O interface to have common exception.
            self.info("Data not received from the input channel.")
            return None
        interval = getattr(self, "_pause_interval", 1)
        # TODO: _pause_interval will be included in the interface later.
        try:
            return self.input_channel.get(*args, timeout=interval)
        except Empty:
            await asyncio.sleep(interval)
            return await self.receive_data(
                self, *args, timeout=timeout, waited=waited + interval
            )

    async def send_data(self, data, *args, **kwargs) -> None:
        try:
            self.output_channel.put(data, *args, **kwargs)
            return True
        except:  # noqa: E722,B001
            # TODO: Update I/O interface to have common exception.
            self.info("Data not sent to the output channel.")
            self.debug("Failed data: %s", str(data))
            return False

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


class AsyncApplicationInterce(BeamlimeApplicationInterface, ABC):
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


class BeamLimeDataReductionInterface(AsyncApplicationInterce, ABC):
    @abstractmethod
    def process(self):
        pass


class ApplicationInstanceGroupInterface(ABC, _LogMixin):
    """
    Multiple instances of a single type of constructor.

    Parameters
    ----------
    constructor: Callable
        ``constructor`` should be able to construct an instance
        by calling it without any arguments.
        It can be made by ``functools.partial``.
        See the example usage in the ``BeamlimeSystem.build_instances``.
    instance_num: int
        Number of instance the ``ApplicationInstanceGroup`` will populate and carry.
        It initially populates instances of ``instance_num``.

    """

    def __init__(
        self, constructor: Callable, instance_num: int = 2, logger: Logger = None
    ) -> None:
        # TODO: Add method to check ``_instances`` if all instances are alive.
        self._init_logger(logger=logger)
        self._init_instances(constructor, instance_num)

    def _init_logger(self, logger: Logger) -> None:
        if logger is None:
            from ..logging import get_logger

            self.logger = get_logger()
        else:
            self.logger = logger

    def _init_instances(self, constructor: Callable, instance_num: int) -> None:
        self.constructor = constructor
        self._instances = []
        for _ in range(instance_num):
            self.populate()

    @property
    def instances(self) -> list[object]:
        """All instance objects in the group."""
        return self._instances

    def __iter__(self) -> object:
        for instance in self._instances:
            yield instance

    def __del__(self) -> None:
        while self._instances:
            self._instances.pop().__del__()

    @abstractmethod
    def populate(self) -> Optional[BeamLimeApplicationProtocol]:
        ...

    @abstractmethod
    def kill(self, num_instances: int = 1) -> None:
        ...

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def pause(self) -> None:
        ...

    @abstractmethod
    def resume(self) -> None:
        ...


class AsyncApplicationInstanceGroup(ApplicationInstanceGroupInterface):
    """
    Multiple instances of a single type of constructor communicates via Async.

    """

    def __init__(
        self, constructor: Callable, instance_num: int = 2, logger: Logger = None
    ) -> None:
        super().__init__(
            constructor=constructor, instance_num=instance_num, logger=logger
        )
        self._input_ch = None
        self._output_ch = None

    @property
    def input_channel(self) -> object:
        # TODO: Update this after implementing Queue Broker.
        return self._instances[0].input_channel

    @property
    def output_channel(self) -> object:
        # TODO: Update this after implementing Queue Broker.
        return self._instances[0].output_channel

    @input_channel.setter
    def input_channel(self, channel: _CommunicationChannel) -> None:
        self._input_ch = channel
        for inst in self._instances:
            inst.input_channel = channel

    @output_channel.setter
    def output_channel(self, channel: _CommunicationChannel) -> None:
        self._output_ch = channel
        for inst in self._instances:
            inst.output_channel = channel

    def populate(self) -> Optional[BeamLimeApplicationProtocol]:
        """Populate one more instance in the group."""
        if len(self._instances) < MAX_INSTANCE_NUMBER:
            self._instances.append(self.constructor())
            if hasattr(self, "_output_ch"):
                self._instances[-1].output_channel = self._output_ch
            if hasattr(self, "_input_ch"):
                self._instances[-1].input_channel = self._input_ch
            return self._instances[-1]
        else:
            self.warning(
                f"There are already {len(self._instances)} "
                "instances in this group. Cannot construct more. "
                f"Maximum number of instances is {MAX_INSTANCE_NUMBER}."
            )
            return None

    def kill(self, num_instances: int = 1) -> None:
        """Kill the last instance in the group."""
        num_target = min(num_instances, len(self._instances))
        for _ in range(num_target):
            self._instances.pop().__del__()

    def start(self) -> None:
        (inst.start() for inst in self._instances)

    def pause(self) -> None:
        (inst.pause() for inst in self._instances)

    def resume(self) -> None:
        (inst.resume() for inst in self._instances)

    @property
    def coroutines(self):
        return [inst._run(inst) for inst in self._instances]

    @property
    def tasks(self):
        return [inst.create_task for inst in self._instances]
