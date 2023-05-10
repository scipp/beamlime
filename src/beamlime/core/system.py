# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from asyncio import Task
from logging import Logger
from typing import Union

from ..applications import (
    BeamlimeApplicationInstanceGroup,
    BeamlimeApplicationInterface,
)
from ..communication.broker import CommunicationBroker


class BeamlimeSystem(BeamlimeApplicationInterface):
    """Manage various types of applications"""

    def __init__(
        self,
        config: Union[dict, str, None] = None,
        logger: Logger = None,
        save_log: bool = False,
    ) -> None:
        from copy import copy

        from ..config.loader import safe_load_config
        from ..config.tools import list_to_dict

        self.user_config = safe_load_config(config)
        self.config = copy(self.user_config)
        self.system_config = self.config["general"]
        self.app_configs = list_to_dict(self.config["data-stream"]["applications"])

        super().__init__(
            app_name=self.system_config["system-name"],
            broker=self.broker,
            logger=logger,
            timeout=self.system_config.get("timeout", 600),
            wait_interval=self.system_config.get("wait-interval", 1),
        )
        if save_log:
            from ..logging import initialize_file_handler

            initialize_file_handler(self.user_config, self.logger)

        self._tasks = dict()
        self.broker = self._build_broker()
        self.applications = self._build_applications()

    def _build_broker(self) -> CommunicationBroker:
        """
        Connect instances if they are communicating with each other.
        """
        channel_list = self.config["data-stream"]["communication-channels"]
        subscription_list = self.config["data-stream"]["application-subscriptions"]
        return CommunicationBroker(
            channel_list=channel_list, subscription_list=subscription_list
        )

    def _build_applications(self) -> dict:
        app_specs = self.config["data-stream"]["application-specs"]
        return {
            app_name: BeamlimeApplicationInstanceGroup(
                app_config=app_config,
                app_spec=app_specs.get(app_name) or {},
                logger=self.logger,
                broker=self.broker,
            )
            for app_name, app_config in self.app_configs.items()
        }

    async def _run(self):
        for app_inst_gr in self.applications.values():
            await app_inst_gr._run()

    def create_task(self) -> list[Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        import asyncio

        # TODO: Handle the case when tasks are still running.
        self._tasks.clear()
        self.start()
        self._tasks = {
            app_name: asyncio.create_task(app._run())
            for app_name, app in self.applications.items()
        }
        return self._tasks

    def kill(self) -> None:
        """Kill all instance groups and coroutine tasks"""
        for app_inst_gr in self.applications.values():
            app_inst_gr.kill()
        while self._tasks:
            self._tasks.pop().cancel()
            self._tasks.clear()

    def __del__(self) -> None:
        while self.applications:
            app_name, app_inst_gr = self.applications.popitem()
            app_inst_gr.kill()
            self.info("Deleting %s instance ... ", app_name)
            del app_inst_gr

    def start(self) -> None:
        # TODO: Update populate logic.
        for app_name, app_inst_gr in self.applications.items():
            self.info("Starting %s instance ... ", app_name)
            if not app_inst_gr.instances:
                app_inst_gr.populate()
            app_inst_gr.start()

    def pause(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Pausing %s instance ... ", app_name)
            app_inst_gr.pause()

    def resume(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Resuming %s instance ... ", app_name)
            app_inst_gr.resume()
