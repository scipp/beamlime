# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from logging import Logger
from typing import Callable, Optional, TypeVar

from .. import protocols
from ..core.schedulers import async_timeout
from .interfaces import BeamlimeApplicationInterface
from .mixins import ApplicationNotStartedException, LogMixin

_CommunicationChannel = TypeVar(
    "_CommunicationChannel",
    protocols.BeamlimeDownstreamProtocol,
    protocols.BeamlimeUpstreamProtocol,
    protocols.BeamlimeTwoWayProtocol,
)

MAX_INSTANCE_NUMBER = 2


# TODO: Implement multi-process/multi-machine instance group interface.
class BeamlimeApplicationInstanceGroup(LogMixin):
    """
    Multiple instances of a single type of application.
    It overwrites all interfaces of ``BeamlimeApplicationInterface``,
    in a way that it has the exact same interfaces
    just like a single application but controls multiple instances internally.
    Therefore it does not inherit ``BeamlimeApplicationInterface``,
    but it should pass the protocol test.
    See ``protocol_test.test_instace_group_protocol`` of the unit test package.

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
    app_name: str
        Name of the application.

    Protocol
    --------
    BeamlimeApplicationProtocol

    """

    def __init__(
        self,
        constructor: Callable,
        instance_num: int = 1,
        logger: Logger = None,
        app_name: str = None,
        timeout: float = 10,
        wait_int: float = 1,
    ) -> None:
        self.app_name = app_name or ""
        # TODO: Add method to check ``_instances`` if all instances are alive.
        self._init_logger(logger=logger)
        self._constructor = constructor
        self._instance_num = instance_num
        self._input_channel = None
        self._output_channel = None
        self._tasks = []  # TODO: make ``_tasks`` dict.
        self._instances = dict()
        self._timeout = timeout
        self._wait_int = wait_int
        self._started = False
        self._paused = True

    def _init_logger(self, logger: Logger) -> None:
        if logger is None:
            from ..logging import get_logger

            self._logger = get_logger()
        else:
            self._logger = logger

    def init_instances(self) -> None:
        self._instances.clear()
        for _ in range(self._instance_num):
            self.populate()

    def parse_config(self, config: dict) -> None:
        # TODO: Move BeamlimeSystem configuration handling part to here.
        ...

    @property
    def input_channel(self) -> object:
        # TODO: Update this after implementing communication broker.
        if self._instances:
            for inst in self._instances.values():
                if hasattr(inst, "input_channel"):
                    return inst.input_channel
        return None

    @property
    def output_channel(self) -> object:
        # TODO: Update this after implementing communication broker.
        if self._instances:
            for inst in self._instances.values():
                if hasattr(inst, "input_channel"):
                    return inst.output_channel
        return None

    @input_channel.setter
    def input_channel(self, channel: _CommunicationChannel) -> None:
        self._input_channel = channel
        for inst in self._instances.values():
            inst.input_channel = channel

    @output_channel.setter
    def output_channel(self, channel: _CommunicationChannel) -> None:
        self._output_channel = channel
        for inst in self._instances.values():
            inst.output_channel = channel

    def start(self) -> None:
        for inst in self._instances.values():
            inst.start()

    def stop(self) -> None:
        for inst in self._instances.values():
            inst.stop()

    def pause(self) -> None:
        for inst in self._instances.values():
            inst.pause()

    def resume(self) -> None:
        for inst in self._instances.values():
            inst.resume()

    @property
    def instances(self) -> list[BeamlimeApplicationInterface]:
        """Dictionary of instance objects in the group."""
        return self._instances

    def __iter__(self) -> object:
        for instance in self._instances.values():
            yield instance

    def __del__(self) -> None:
        while self.instances:
            _, killed_inst = self._instances.popitem()
            if hasattr(killed_inst, "_task"):
                killed_inst._task.cancel()
                del killed_inst
        if hasattr(super(), "__del__"):
            super().__del__()

    async def should_proceed(self):
        @async_timeout(ApplicationNotStartedException)
        async def wait_start(timeout: int, wait_interval: int) -> None:
            if not any([inst._started for inst in self._instances.values()]):
                self.debug("Application not started. Waiting for ``start`` command...")
                raise ApplicationNotStartedException
            return

        try:
            await asyncio.sleep(self._wait_int)
            await wait_start(timeout=self._timeout, wait_interval=self._wait_int)
            return True
        except TimeoutError:
            self.debug("Applications not started. Killing instances ...")
            self.kill()
            return False

    async def _run(self) -> None:
        if await self.should_proceed():
            for inst in self._instances.values():
                await inst._run()

    def run(self) -> None:
        asyncio.run(self._run())

    async def _create_task(self) -> None:
        # ApplicationInstanceGroup can also create task by
        # ``asyncio.create_task(self._run())``,
        # but it will be easier to control each tasks
        # if we have handles of each instances like below.
        if await self.should_proceed():
            self._tasks = [
                inst.create_task(name=inst.app_name + "-" + str(inst_num))
                for inst_num, inst in enumerate(self.instances)
            ]
            return self._tasks

    def create_task(self) -> list[asyncio.Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        _create_task_task = asyncio.create_task(self._create_task())
        return asyncio.wait_for(_create_task_task, timeout=self._timeout)

    def populate(self) -> Optional[protocols.BeamlimeApplicationProtocol]:
        """Populate one more instance in the group."""
        from uuid import uuid4

        if len(self._instances) < MAX_INSTANCE_NUMBER:
            inst_name = self.app_name + uuid4().hex
            self._instances[inst_name] = self._constructor()
            self._instances[inst_name].output_channel = self._output_channel
            self._instances[inst_name].input_channel = self._input_channel
            return self._instances[inst_name]
        else:
            self.warning(
                f"There are already {len(self._instances)} "
                "instances in this group. Cannot construct more. "
                f"Maximum number of instances is {MAX_INSTANCE_NUMBER}."
            )
            return None

    def kill(self) -> None:
        """Kill all instances in the group."""
        while self._instances:
            _, killed_app = self._instances.popitem()
            killed_app.stop()
            del killed_app
        while self._tasks:
            self._tasks.pop().cancel()
        self._tasks.clear()
