# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from logging import Logger
from typing import Optional

from .. import protocols
from ..communication.broker import CommunicationBroker
from ..config.preset_options import DEFAULT_TIMEOUT, DEFAULT_WAIT_INTERVAL
from ..core.schedulers import async_timeout
from .interfaces import BeamlimeApplicationInterface
from .mixins import ApplicationNotStartedException

MAX_INSTANCE_NUMBER = 2


# TODO: Implement multi-process/multi-machine instance group interface.
class BeamlimeApplicationInstanceGroup(BeamlimeApplicationInterface):
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
    app_name: str
        Name of the application.

    Protocol
    --------
    BeamlimeApplicationProtocol

    """

    # TODO: Update docstring.
    # TODO: Add method to check ``_instances`` if all instances are alive.

    def __init__(
        self,
        app_config: dict,
        app_spec: dict = None,
        logger: Logger = None,
        broker: CommunicationBroker = None,
    ) -> None:
        from functools import partial

        from ..config.tools import import_object

        super().__init__(
            app_name=app_config["name"],
            broker=broker,
            logger=logger,
            timeout=app_config.get("timeout") or DEFAULT_TIMEOUT,
            wait_interval=app_config.get("wait-interval") or DEFAULT_WAIT_INTERVAL,
        )
        self._instance_num = app_config.get("instance-number") or 1
        handler = import_object(app_config["data-handler"])
        extra_kwargs = {
            app_spec_key.replace("-", "_"): spec
            for app_spec_key, spec in app_spec.items()
        }
        self._constructor = partial(
            handler,
            app_name=self.app_name,
            timeout=self.timeout,
            wait_interval=self.wait_interval,
            logger=self.logger,
            broker=self.broker,
            **extra_kwargs,
        )
        self._tasks = dict()
        self._instances = dict()

    def init_instances(self) -> None:
        self._instances.clear()
        for _ in range(self._instance_num):
            self.populate()

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

    async def should_proceed(self):
        @async_timeout(ApplicationNotStartedException)
        async def wait_start(timeout: int, wait_interval: int) -> None:
            if not any([inst._started for inst in self._instances.values()]):
                self.debug("Application not started. Waiting for ``start`` command...")
                raise ApplicationNotStartedException
            return

        try:
            await asyncio.sleep(self.wait_interval)
            await wait_start(timeout=self.timeout, wait_interval=self.wait_interval)
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
            killed_app.stop()
            del killed_app
        while self._tasks:
            self._tasks.pop().cancel()
        self._tasks.clear()
