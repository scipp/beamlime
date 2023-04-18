# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from abc import ABC, abstractmethod
from logging import Logger
from queue import Queue
from typing import Callable, Optional, TypeVar, Union, overload

from ..communication.channel import (
    BeamlimeDownstreamProtocol,
    BeamlimeTwoWayProtocol,
    BeamlimeUpstreamProtocol,
)
from .application import (
    BeamlimeApplicationInterface,
    BeamLimeApplicationProtocol,
    _LogMixin,
)

_CommunicationChannel = TypeVar(
    "_CommunicationChannel",
    BeamlimeDownstreamProtocol,
    BeamlimeUpstreamProtocol,
    BeamlimeTwoWayProtocol,
)


MAX_INSTANCE_NUMBER = 2
# TODO: Implement multi-process/multi-machine instance group interface.


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
        self._processes = [inst.create_task() for inst in self._instances]

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


class BeamLimeSystem(BeamlimeApplicationInterface):
    def __init__(
        self, config: dict = None, logger: object = None, save_log: bool = False
    ) -> None:
        super().__init__(config=config, logger=logger, name="Manager")
        if save_log:
            from ..logging import initialize_file_handler

            initialize_file_handler(self.config, self.logger)
        self.build_instances()
        self.connect_instances()

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
        elif isinstance(config, str):
            import os.path

            if not os.path.exists(config):
                self.error("Configuration yaml file was not found in %s.", config)
                raise FileNotFoundError(
                    f"Configuration yaml file was not found in {config}."
                )
            else:
                import yaml

                with open(config) as file:
                    self.config = yaml.safe_load(file)
        elif isinstance(config, dict):
            self.config = config

        self.build_runtime_config()

    def build_runtime_config(self) -> None:
        from copy import copy

        self.runtime_config = copy(self.config)
        for app in self.runtime_config["data-stream"]["applications"]:
            if "instance-number" in app:
                config_num = app["instance-number"]
                if config_num and config_num > MAX_INSTANCE_NUMBER:
                    app["instance-number"] = MAX_INSTANCE_NUMBER
                    self.warning(
                        f"Requested number of instances, {config_num} "
                        f"is bigger than the limit, {MAX_INSTANCE_NUMBER}. "
                        f"Update runtime-configuration accordingly."
                    )

    def build_instances(self):
        from functools import partial

        from ..config.tools import import_object, list_to_dict
        from ..core.application import BeamLimeDataReductionInterface

        self.app_configs = list_to_dict(
            self.runtime_config["data-stream"]["applications"]
        )
        self.app_specs = self.runtime_config["data-stream"]["application-specs"]

        self.app_instances = dict()
        for app_name, app in self.app_configs.items():
            handler = import_object(app["data-handler"])
            instance_num = app.get("instance-number", 1) or 1
            if issubclass(handler, BeamLimeDataReductionInterface):
                # Special type of application - Data Reduction
                app_config = self.runtime_config["data-reduction"]
            else:
                app_config = self.app_specs.get(app_name, None)

            handler_constructor = partial(
                handler,
                config=app_config,
                name=app_name,
                logger=self.logger,
            )

            self.app_instances[app_name] = AsyncApplicationInstanceGroup(
                constructor=handler_constructor, instance_num=instance_num
            )

    def connect_instances(self):
        """
        Connect instances if they are communicating with each other.
        """
        from ..config.tools import list_to_dict

        self.app_mapping = list_to_dict(
            self.runtime_config["data-stream"]["applications-mapping"],
            key_field="from",
            value_field="to",
        )

        for sender_name, _receiver_names in self.app_mapping.items():
            if isinstance(_receiver_names, list):
                receiver_names = _receiver_names
            elif isinstance(_receiver_names, str):
                receiver_names = [_receiver_names]

            sender_config = self.app_configs[sender_name]
            receiver_configs = [
                self.app_configs[receiver_n] for receiver_n in receiver_names
            ]
            receiver_i_chn_set = set([cfg["input-channel"] for cfg in receiver_configs])
            if (len(receiver_i_chn_set) != 1) or (
                sender_config["output-channel"] != receiver_i_chn_set.pop()
            ):
                raise ValueError(
                    "`input-channel` of the `from` application"
                    " and the `output-channel` of the `to` application "
                    "should have the same option."
                )

            channel_type = sender_config["output-channel"]

            sender = self.app_instances[sender_name]
            receivers = [
                self.app_instances[receiver_name] for receiver_name in receiver_names
            ]

            if channel_type == "QUEUE":
                if sender.output_channel is None:
                    new_queue = Queue(maxsize=100)
                    sender.output_channel = new_queue
                for receiver in receivers:
                    if receiver.input_channel is None:
                        receiver.input_channel = new_queue
                    if receiver.input_channel != sender.output_channel:
                        raise RuntimeError("There's a problem in the mapping")

    @property
    def coroutines(self) -> list:
        coroutines = []
        for inst in self.app_instances.values():
            coroutines.extend(inst.coroutines)
        return coroutines

    def create_task(self) -> None:
        tasks = []
        for inst in self.app_instances.values():
            tasks.extend(inst.tasks)
        tasks = [task() for task in tasks]
        return tasks

    def __del__(self) -> None:
        pass

    def _run(self) -> None:
        pass

    def start(self) -> None:
        pass

    def pause(self) -> None:
        pass

    def resume(self) -> None:
        pass
