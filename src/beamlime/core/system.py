# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from asyncio import Task
from typing import Union, overload

from ..applications import (
    MAX_INSTANCE_NUMBER,
    BeamlimeApplicationInstanceGroup,
    BeamlimeApplicationInterface,
)


def glue_place_holder(_, __):
    ...


class BeamlimeSystem(BeamlimeApplicationInterface):
    def __init__(
        self, config: dict = None, logger: object = None, save_log: bool = False
    ) -> None:
        super().__init__(config=config, logger=logger, name="Manager")
        if save_log:
            from ..logging import initialize_file_handler

            initialize_file_handler(self.config, self.logger)
        self.build_instances()
        self._tasks = []

    @overload
    def parse_config(self, config_path: str) -> None:
        ...

    @overload
    def parse_config(self, config: dict) -> None:
        ...

    def parse_config(self, config: Union[str, dict]) -> None:
        if config is None:
            from beamlime.resources.generated import load_static_default_config

            self.config = load_static_default_config()
            self.info(
                "Configuration not given. "
                "Application will use the default configuration in the package."
            )
        elif isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            from ..config.inspector import validate_config_path

            try:
                validate_config_path(config_path=config)
            except FileNotFoundError as err:
                self.error("Configuration yaml file was not found in %s.", config)
                raise err
            else:
                import yaml

                with open(config) as file:
                    self.config = yaml.safe_load(file)

        self._build_runtime_config()

    def _build_runtime_config(self) -> None:
        from copy import copy

        self.runtime_config = copy(self.config)
        app_configs = self.runtime_config["data-stream"]["applications"]

        instance_numbers = [app.get("instance-number", 1) or 1 for app in app_configs]
        max_requested_num = max(instance_numbers)
        if max_requested_num > MAX_INSTANCE_NUMBER:
            self.warning(
                f"Requested number of instances, {max_requested_num} "
                f"is bigger than the limit, {MAX_INSTANCE_NUMBER}. "
                f"Update runtime-configuration accordingly."
            )
        for app in app_configs:
            app["instance-number"] = min(
                app.get("instance-number", 1) or 1, MAX_INSTANCE_NUMBER
            )

    def build_instances(self):
        from functools import partial

        from ..applications.interfaces import BeamlimeDataReductionInterface
        from ..config.tools import import_object, list_to_dict

        app_configs = list_to_dict(self.runtime_config["data-stream"]["applications"])
        app_specs = self.runtime_config["data-stream"]["application-specs"]

        self.app_instances = dict()
        for app_name, app in app_configs.items():
            handler = import_object(app["data-handler"])
            instance_num = app.get("instance-number", 1) or 1
            if issubclass(handler, BeamlimeDataReductionInterface):
                # Special type of application - Data Reduction
                app_config = self.runtime_config["data-reduction"]
            else:
                app_config = app_specs.get(app_name, None)

            handler_constructor = partial(
                handler,
                config=app_config,
                name=app_name,
                logger=self.logger,
            )

            self.app_instances[app_name] = BeamlimeApplicationInstanceGroup(
                constructor=handler_constructor,
                instance_num=instance_num,
                logger=self.logger,
                app_name=app_name,
            )
        for app_inst_gr in self.app_instances.values():
            app_inst_gr.init_instances()
        self._connect_instances()

    async def _run(self):
        for app_inst_gr in self.app_instances.values():
            await app_inst_gr._run()

    def _connect_instances(self):
        """
        Connect instances if they are communicating with each other.
        """
        from ..config.inspector import validate_application_mapping
        from ..config.tools import list_to_dict, wrap_item

        validate_application_mapping(
            data_stream_config=self.runtime_config["data-stream"]
        )

        app_map_list = self.runtime_config["data-stream"]["applications-mapping"]
        app_mapping = list_to_dict(app_map_list, key_field="from", value_field="to")
        app_configs = list_to_dict(self.runtime_config["data-stream"]["applications"])

        for sender_name, receiver_names in app_mapping.items():
            sender = self.app_instances[sender_name]
            receivers = [
                self.app_instances[receiver_name]
                for receiver_name in wrap_item(receiver_names, list)
            ]

            channel_type = app_configs[sender_name]["output-channel"]

            if channel_type == "QUEUE":
                # TODO: Replace queue to communication handler.
                from ..communication.queue_handlers import glue

                glue_method = glue
            else:  # TODO: Implement other communication options
                glue_method = glue_place_holder

            glue_method(sender, receivers)

    def create_task(self) -> list[Task]:
        # TODO: return TaskGroup instead of list of Task.
        # Will be available from py311
        import asyncio

        # TODO: Handle the case when tasks are still running.
        self._tasks.clear()
        self.start()
        self._tasks = [
            asyncio.create_task(inst_gr._run())
            for inst_gr in self.app_instances.values()
        ]
        return self._tasks

    def kill(self) -> None:
        """Kill all instance groups and coroutine tasks"""
        for app_inst_gr in self.app_instances.values():
            app_inst_gr.kill()
        while self._tasks:
            self._tasks.pop().cancel()
            self._tasks.clear()

    def __del__(self) -> None:
        while self.app_instances:
            app_name, app_inst_gr = self.app_instances.popitem()
            app_inst_gr.kill()
            self.info("Deleting %s instance ... ", app_name)
            del app_inst_gr

    def start(self) -> None:
        # TODO: Update populate logic.
        for app_name, app_inst_gr in self.app_instances.items():
            self.info("Starting %s instance ... ", app_name)
            if not app_inst_gr.instances:
                app_inst_gr.populate()
            app_inst_gr.start()

    def pause(self) -> None:
        for app_name, app_inst_gr in self.app_instances.items():
            self.info("Pausing %s instance ... ", app_name)
            app_inst_gr.pause()

    def resume(self) -> None:
        for app_name, app_inst_gr in self.app_instances.items():
            self.info("Resuming %s instance ... ", app_name)
            app_inst_gr.resume()
