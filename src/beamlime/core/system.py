# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from asyncio import Task
from enum import Enum
from logging import Logger
from typing import Dict, Iterable, Optional, Union

from ..applications import (
    BeamlimeApplicationInstanceGroup,
    BeamlimeApplicationInterface,
)
from ..communication.broker import CommunicationBroker
from ..config.preset_options import Timeout, WaitInterval
from ..config.tools import dict_to_yaml
from ..logging import LOG_LEVELS


class ExitCode(Enum):
    SUCCESS = 0
    INTERRUPTED = 1
    FAIL = 2


def instantiate_system(
    *,
    name: str,
    constructor: str,
    specs: Optional[dict],
    logger: Logger,
    **unused_specs,
) -> BeamlimeSystem:
    from ..config.tools import import_object

    constructor_obj = import_object(constructor)
    logger.info(
        "Unused information in the configuration: %s", dict_to_yaml(unused_specs)
    )
    return constructor_obj(name=name, logger=logger, **specs)


def start_system(
    config: Union[dict, str, None],
    logger: Logger = None,
    log_level: LOG_LEVELS = None,
    verbose: bool = True,
) -> BeamlimeSystem:
    from ..config.loader import safe_load_config
    from ..logging import safe_get_logger

    _runtime_config = safe_load_config(config)  # TODO: configuration validation.
    _logger = safe_get_logger(logger, verbose)
    if log_level:
        _logger.setLevel(log_level)

    system_instance = instantiate_system(logger=_logger, **_runtime_config)

    _logger.info(
        "System instance created with this configuration: \n%s",
        dict_to_yaml(_runtime_config),
    )
    return system_instance


class BeamlimeSystem(BeamlimeApplicationInterface):
    """Manage various types of applications"""

    def __init__(
        self,
        name: str = "Beamlime Dashboard",
        logger: Logger = None,
        save_log: bool = False,
        log_dir: str = None,
        timeout: float = Timeout.maximum,
        wait_interval: float = WaitInterval.minimum,
        applications: Iterable[Dict] = None,
        communications: Iterable[Dict] = None,
        subscriptions: Iterable[Dict] = None,
        **unused_specs,
    ) -> None:
        super().__init__(
            name=name,
            logger=logger,
            timeout=timeout,
            wait_interval=wait_interval,
        )

        self._init_logger(save_log, log_dir)
        self.broker = self._build_broker(communications, subscriptions)
        self.applications = self._build_application(applications)
        self._handle_unused_specs(**unused_specs)

        self._tasks = dict()
        self.info("System instance up and running ...")

    def _init_logger(self, save_log: bool = False, log_dir: str = None) -> None:
        if save_log:
            from ..logging import initialize_file_handler

            initialize_file_handler(log_dir, self.logger)

    def _build_broker(
        self, channel_list: list, subscription_list: list
    ) -> CommunicationBroker:
        _broker = CommunicationBroker(
            channel_list=channel_list, subscription_list=subscription_list
        )
        self.debug("Communication broker built: \n%s", _broker)
        return _broker

    def _build_application(
        self, application_list: list
    ) -> Dict[str, BeamlimeApplicationInstanceGroup]:
        from ..config.tools import list_to_dict

        _applications = {
            app_name: BeamlimeApplicationInstanceGroup(
                logger=self.logger, broker=self.broker, **app_config
            )
            for app_name, app_config in list_to_dict(application_list).items()
        }
        self.debug("Application instances built: \n%s", str(_applications))
        return _applications

    def _handle_unused_specs(self, **unused_specs) -> None:
        if unused_specs:
            self.warning(
                "Unused specs in the configuration : ", dict_to_yaml(unused_specs)
            )

    async def _run(self):
        import asyncio

        await asyncio.wait(
            tuple(app_inst_gr._run() for app_inst_gr in self.applications.values())
        )

    def create_task(self) -> list[Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        import asyncio

        # TODO: Handle the case when tasks are still running.
        self._tasks.clear()
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
        del self.applications

    def start(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Starting %s instance ... ", app_name)
            app_inst_gr.start()

    def pause(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Pausing %s instance ... ", app_name)
            app_inst_gr.pause()

    def resume(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Resuming %s instance ... ", app_name)
            app_inst_gr.resume()

    def stop(self) -> None:
        for app_name, app_inst_gr in self.applications.items():
            self.info("Stopping %s instance ... ", app_name)
            if app_inst_gr._started:
                app_inst_gr.stop()
