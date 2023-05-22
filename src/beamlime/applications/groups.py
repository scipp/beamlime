# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from logging import Logger
from typing import Optional

from .. import protocols
from ..communication.broker import CommunicationBroker
from .interfaces import BeamlimeApplicationInterface

MAX_INSTANCE_NUMBER = 2


# TODO: Implement multi-process/multi-machine instance group interface.
class BeamlimeApplicationInstanceGroup(BeamlimeApplicationInterface):
    """
    Controls instance(s) of an application.
    It overwrites all interfaces of ``BeamlimeApplicationInterface``.
    It has the exact same interfaces as a single application instance,
    but controls multiple instances internally.
    Therefore it does not inherit ``BeamlimeApplicationInterface``,
    but it should pass the protocol test.
    See ``protocol_test.test_instace_group_protocol`` of the unit test package.

    Parameters
    ----------
    name: str
        Name of the application.

    constructor: str
        Name of the application constructor.

    specs: dict
        Keyword arguments for the application constructor.

    logger: ``logging.Logger``
        Logger object.

    broker: ``beamlime.communication.broker.CommunicationBroker``
        Communication broker between applications in beamlime system.

    instance_num: int
        Number of instances to be hold.

    Protocol
    --------
    BeamlimeApplicationProtocol

    """

    # TODO: Update docstring.
    # TODO: Add method to check ``_instances`` if all instances are alive.

    def __init__(
        self,
        name: str,
        constructor: str,
        specs: dict = None,
        logger: Logger = None,
        broker: CommunicationBroker = None,
        instance_num: int = 1,
    ) -> None:
        from functools import partial

        from ..config.tools import import_object

        super().__init__(name=name, broker=broker, logger=logger)
        self._instance_num = instance_num
        self._constructor = partial(
            import_object(constructor),
            name=self.app_name,
            broker=self.broker,
            logger=self.logger,
            **(specs or {}),
        )
        self._tasks = dict()
        self._instances = dict()

    def init_instances(self) -> None:
        self._instances.clear()
        for _ in range(self._instance_num):
            self.populate()

    def start(self) -> None:
        super().start()
        while len(self.instances) < self._instance_num:
            self.populate()

        for inst in self._instances.values():
            inst.start()

    def stop(self) -> None:
        super().stop()
        for inst in self._instances.values():
            if inst._started:
                inst.stop()

    def pause(self) -> None:
        super().pause()
        for inst in self._instances.values():
            inst.pause()

    def resume(self) -> None:
        super().resume()
        for inst in self._instances.values():
            inst.resume()

    @property
    def instances(self) -> dict:
        """Dictionary of instance objects in the group."""
        return self._instances

    @instances.setter
    def instances(self, _) -> None:
        raise RuntimeError(
            "Instances should be populated " "by ``populate`` or ``init_instances``."
        )

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

    async def _should_create_tasks(self) -> bool:
        if await self.should_start() and await self.should_proceed():
            return True
        else:
            self.error("Application is not started or resumed.")
            return False

    async def _run(self) -> None:
        if await self._should_create_tasks():
            await asyncio.wait(tuple(inst._run() for inst in self.instances.values()))

    def run(self) -> None:
        asyncio.run(self._run())

    async def _create_task(self) -> None:
        # ApplicationInstanceGroup can also create task by
        # ``asyncio.create_task(self._run())``,
        # but it will be easier to control each tasks
        # if we have handles of each instances like below.
        if await self._should_create_tasks():
            self._tasks = {
                inst_name: inst.create_task(name=inst_name)
                for inst_name, inst in self.instances.items()
            }
            return self._tasks

    def create_task(self) -> list[asyncio.Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        return asyncio.wait_for(self._create_task(), timeout=self.timeout)

    def populate(self) -> Optional[protocols.BeamlimeApplicationProtocol]:
        """Populate one more instance in the group."""
        from uuid import uuid4

        if len(self._instances) < MAX_INSTANCE_NUMBER:
            inst_name = self.app_name + uuid4().hex
            self._instances[inst_name] = self._constructor()
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
            if killed_app._started:
                killed_app.stop()
            del killed_app
        while self._tasks:
            self._tasks.pop().cancel()
        self._tasks.clear()
