# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from abc import ABC, abstractmethod
from logging import DEBUG, ERROR, INFO, WARN, Logger
from queue import Empty
from typing import Callable, Optional, TypeVar

from ..core.protocols import (
    BeamlimeApplicationProtocol,
    BeamlimeDownstreamProtocol,
    BeamlimeTwoWayProtocol,
    BeamlimeUpstreamProtocol,
)
from ..core.schedulers import async_timeout
from ..logging.loggers import BeamlimeLogger

_CommunicationChannel = TypeVar(
    "_CommunicationChannel",
    BeamlimeDownstreamProtocol,
    BeamlimeUpstreamProtocol,
    BeamlimeTwoWayProtocol,
)

MAX_INSTANCE_NUMBER = 2
# TODO: Implement multi-process/multi-machine instance group interface.
MAX_INSTANCE_PAUSED = 60


class _LogMixin:
    """Logging interfaces"""

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

    def error(self, msg: str, *args) -> None:
        self._log(level=ERROR, msg=msg, args=args)


class _FlagControlMixin:
    """Process control interfaces"""

    _started = False
    _paused = True
    _stopped = False

    def start(self) -> None:
        self.info("Control command 'start' received.")
        if not self._started:
            self._started = True
            self._paused = False
            self.debug(
                "Flags updated, started flag: %s, paused flag: %s",
                self._started,
                self._paused,
            )

    def pause(self) -> None:
        self.info("Control command 'pause' received.")
        if not self._paused:
            self._paused = True
            self.debug("Flag updated, paused flag: %s", self._paused)

    def resume(self) -> None:
        self.info("Control command 'resume' received.")
        if not self._started:
            self.info("Application not started, trying control command 'start' ...")
            self.start()
        elif self._paused:
            self._paused = False
            self.debug("Flag updated, paused flag: %s", self._paused)

    def stop(self) -> None:
        self.info("Control command 'stop' received.")
        if not self._paused:
            self.info(
                "Application not paused, trying control command 'pause' first ..."
            )
            self.pause()
        if not self._stopped:
            self._stopped = True
            self.debug("Flag updated, stopped flag: %s", self._paused)


class ApplicationPausedException(Exception):
    ...


class ApplicationStoppedException(Exception):
    ...


class _DaemonInterface(ABC):
    """
    Daemon Application Interface

    """

    def __del__(self):
        self.stop()
        if hasattr(super(), "__del__"):
            super().__del__()

    async def stopped(
        self,
        timeout: int = MAX_INSTANCE_PAUSED,
        check_pause_interval: int = MAX_INSTANCE_PAUSED,
    ) -> bool:
        @async_timeout(ApplicationPausedException)
        async def wait_resumed(timeout: int, wait_interval: int):
            if self._stopped:
                self.info("Application stopped.")
                return False
            elif not self._paused:
                return True
            elif self._paused:
                self.info(
                    "Application process paused. Waiting for ``resume`` command..."
                )
                raise ApplicationPausedException

        try:
            resumed = await wait_resumed(timeout, check_pause_interval)
            return not resumed
        except TimeoutError:
            self.info("Application paused limit reached. Stopping the application...")
            self.stop()
            return True

    @abstractmethod
    async def _run(self) -> None:
        """
        Application coroutine generator.
        """
        ...

    def run(self) -> None:
        """
        Run the application in a dependent event loop.

        """
        # TODO: ``run`` should also check if the communication-interface
        # is multi-process-friendly before running.
        # For example, if one of input_channel or output_channel is ``Queue``,
        # the application daemon should start by ``create_task`` not, ``run``.

        return asyncio.run(self._run())

    def create_task(self, /, name=None, context=None) -> asyncio.Task:
        """
        Start the application task in the currently running event loop.
        """
        if not hasattr(self, "_task") or self._task.done():
            # TODO: Remove try-except after updating the minimum python version to 3.11
            try:
                self._task = asyncio.create_task(
                    self._run(), name=name, context=context
                )  # py311
            except TypeError:
                self._task = asyncio.create_task(self._run(), name=name)  # py39, py310
        return self._task


class BeamlimeApplicationInterface(_LogMixin, _FlagControlMixin, _DaemonInterface, ABC):
    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        self._pause_interval = 0.1
        self._init_logger(logger=logger)
        self.app_name = kwargs.get("name", "")
        self._input_ch = None
        self._output_ch = None
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

    @async_timeout(Empty)
    async def receive_data(self, *args, timeout=10, wait_interval=0.2, **kwargs):
        # TODO: Move async_timeout(exception=Empty) to communication handler interface
        # and remove the decorator or use @async_timeout(exception=TimeoutError).
        return self.input_channel.get(*args, timeout=wait_interval, **kwargs)

    @async_timeout(Empty)
    async def send_data(
        self, data, *args, timeout=1, wait_interval=1, **kwargs
    ) -> None:
        # TODO: Move async_timeout(exception=Empty) to communication handler interface
        # and remove the decorator or use @async_timeout(exception=TimeoutError).
        self.output_channel.put(data, *args, timeout=wait_interval, **kwargs)
        return True


class BeamLimeDataReductionInterface(BeamlimeApplicationInterface, ABC):
    @abstractmethod
    def process(self):
        pass


class ApplicationInstanceGroupInterface(_LogMixin, ABC):
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

    def parse_config(self, config: dict) -> None:
        # TODO: Move BeamlimeSystem configuration handling part to here.
        ...

    @property
    def instances(self) -> list[BeamlimeApplicationInterface]:
        """All instance objects in the group."""
        return self._instances

    def __iter__(self) -> object:
        for instance in self._instances:
            yield instance

    def __del__(self) -> None:
        while self.instances:
            self.instances.pop()._task.cancel()
        if hasattr(super(), "__del__"):
            super().__del__()

    @abstractmethod
    def populate(self) -> Optional[BeamlimeApplicationProtocol]:
        ...

    @abstractmethod
    def kill(self, num_instances: int = 1) -> None:
        ...


class DaemonApplicationInstanceGroup(ApplicationInstanceGroupInterface):
    """
    Multiple instances of a single type of constructor communicates via Async.

    """

    def __init__(
        self,
        constructor: Callable,
        instance_num: int = 2,
        logger: Logger = None,
        app_name: str = None,
    ) -> None:
        from uuid import uuid4

        self.app_name = (app_name or "") + f".instance_group#{uuid4()}"
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

    def populate(self) -> Optional[BeamlimeApplicationProtocol]:
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
            killed_app = self._instances.pop()
            killed_app.stop()
            if hasattr(killed_app, "task"):
                killed_app.task.cancel()
            del killed_app

    def start(self) -> None:
        for inst in self._instances:
            inst.start()

    def stop(self) -> None:
        for inst in self._instances:
            inst.stop()

    def pause(self) -> None:
        for inst in self._instances:
            inst.pause()

    def resume(self) -> None:
        for inst in self._instances:
            inst.resume()

    def create_task(self) -> list[asyncio.Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        return [inst.create_task() for inst in self._instances]
