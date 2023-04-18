# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from queue import Queue
from typing import Union, overload

from ..core.application import BeamlimeApplicationInterface, _LogMixin


class ApplicationInstanceGroup(_LogMixin):
    def __init__(self, constructor, max_instance_num=3) -> None:
        self.constructor = constructor
        self.max_instance_num = max_instance_num
        self._instances = []
        for _ in range(self.max_instance_num):
            self._instances.append(self.constructor())
            self._instances[-1].info("Application instance up and running...")
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
    def input_channel(self, channel) -> None:
        self._input_ch = channel
        for inst in self._instances:
            inst.input_channel = channel

    @output_channel.setter
    def output_channel(self, channel) -> None:
        self._output_ch = channel
        for inst in self._instances:
            inst.output_channel = channel

    @property
    def instances(self) -> list[object]:
        """All instance objects in the group."""
        return self._instances

    def populate(self) -> None:
        """Populate one more instance in the group."""
        if len(self._instances) < self.max_instance_num:
            self._instances.append(self.constructor())
        else:
            raise ResourceWarning(
                f"There are already {len(self._instances)}"
                "instances in this group. Cannot construct more."
            )

    def kill(self, num_instances: int = 1) -> object:
        num_target = min(num_instances, len(self._instances))
        for _ in range(num_target):
            self._instances.pop().__del__()

    def __iter__(self) -> object:
        for instance in self._instances:
            yield instance

    def __del__(self) -> None:
        while self._instances:
            self._instances.pop().__del__()

    def start(self, parallelism_method: str = "ASYNC") -> bool:
        if parallelism_method == "PROCESS":
            # TODO: Implement multi-process here.
            # Still didn't decide where to populate and manage `Process`,
            # whether it should be in ...
            # 1. Each application instance
            # 2. Application instance group (here)
            # 3. Application manager
            pass
        elif parallelism_method == "ASYNC":
            self._processes = [inst.create_task() for inst in self._instances]

    @property
    def coroutines(self):
        return [inst._run(inst) for inst in self._instances]

    @property
    def tasks(self):
        return [inst.create_task for inst in self._instances]

    @property
    def processes(self) -> object:
        return self._processes


class BeamLimeApplicationManager(BeamlimeApplicationInterface):
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
    def parse_config(self, config: str) -> None:
        ...

    @overload
    def parse_config(self, config: dict) -> None:
        ...

    def parse_config(self, config: Union[str, dict]) -> None:
        if config is None:
            from beamlime.resources.generated import load_static_default_config

            self.config = load_static_default_config()
            # TODO: log warning here
            # "Application will use the default configuration in the package."
        elif isinstance(config, str):
            import os.path

            if not os.path.exists(config):
                raise FileNotFoundError(
                    f"Configuration yaml file was not found in {config}."
                )
            else:
                import yaml

                with open(config) as file:
                    self.config = yaml.safe_load(file)
        elif isinstance(config, dict):
            self.config = config

    def build_instances(self):
        from functools import partial

        from ..config.tools import import_object, list_to_dict
        from ..core.application import BeamLimeDataReductionInterface

        MAX_INSTANCE_NUMBER = 2  # TODO: Add this in beamlime.config.rule
        self.app_configs = list_to_dict(self.config["data-stream"]["applications"])
        self.app_specs = self.config["data-stream"]["application-specs"]

        self.app_instances = dict()
        for app_name, app in self.app_configs.items():
            handler = import_object(app["data-handler"])
            if "instance-number" in app["data-handler"]:
                instance_num = min(
                    app["data-handler"]["instance-number"], MAX_INSTANCE_NUMBER
                )
                if app["data-handler"]["instance-number"] != instance_num:
                    # TODO: log warning the configuration exceeded maximum limit.
                    pass
            else:
                instance_num = 1

            if issubclass(handler, BeamLimeDataReductionInterface):
                # Special type of application - Data Reduction
                handler_constructor = partial(
                    handler,
                    config=self.config["data-reduction"],
                    name=app_name,
                    logger=self.logger,
                )
            else:
                if app_name in self.app_specs:
                    handler_constructor = partial(
                        handler,
                        config=self.app_specs[app_name],
                        name=app_name,
                        logger=self.logger,
                    )
                else:
                    handler_constructor = partial(
                        handler, name=app_name, logger=self.logger
                    )

            self.app_instances[app_name] = ApplicationInstanceGroup(
                constructor=handler_constructor, max_instance_num=instance_num
            )

    def connect_instances(self):
        """
        Connect instances if they are communicating with each other.
        """
        from ..config.tools import list_to_dict

        self.app_mapping = list_to_dict(
            self.config["data-stream"]["applications-mapping"],
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
