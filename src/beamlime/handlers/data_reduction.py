# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import Logger
from typing import TypeVar

import numpy as np
import scipp as sc

from ..applications.interfaces import BeamlimeDataReductionInterface
from ..communication.broker import CommunicationBroker
from ..config.tools import list_to_dict, nested_data_get

_DataType = TypeVar("_DataType")


def heatmap_2d(data: _DataType, threshold=0.2, binning_size=(64, 64)) -> sc.DataArray:
    heatmap = sc.array(dims=["x", "y"], values=np.array(data))
    da = sc.DataArray(
        data=heatmap,
        coords={
            "x": sc.linspace(
                "x", 0, binning_size[0] + 1, binning_size[0] + 1, unit="mm"
            ),
            "y": sc.linspace(
                "y", 0, binning_size[1] + 1, binning_size[1] + 1, unit="mm"
            ),
        },
    )
    threshold_mask = (da.values > threshold) * np.ones(da.shape)
    da.values = threshold_mask
    return da


def handover(data: _DataType) -> _DataType:
    return data


class BeamLimeDataReductionApplication(BeamlimeDataReductionInterface):
    def __init__(
        self,
        /,
        name: str,
        broker: CommunicationBroker = None,
        config: dict = None,
        logger: Logger = None,
        timeout: float = 1,
        wait_interval: float = 0.1,
        workflows: dict = None,
        targets: dict = None,
    ) -> None:
        super().__init__(
            name=name,
            broker=broker,
            config=config,
            logger=logger,
            timeout=timeout,
            wait_interval=wait_interval,
        )
        self.workflow_map = list_to_dict(workflows or {}, "name")
        self.target_map = list_to_dict(targets or {}, "name")
        self.history = {wf_name: {"data-count": 0} for wf_name in self.workflow_map}

    def apply_policy(
        self, new_data: _DataType, old_data: _DataType, policy: str, data_count: int = 0
    ) -> _DataType:
        if old_data is None:
            return new_data
        elif policy == "STACK":
            return new_data + old_data
        elif policy == "APPEND":
            return np.append(new_data, old_data)
        elif policy == "AVERAGE":
            return new_data * (data_count / (data_count + 1)) + old_data * (
                1 / (data_count + 1)
            )
        # REPLACE or SKIP
        return new_data

    def __del__(self):
        # TODO: Save the current status somewhere or send it to somewhere.
        ...

    async def process(self, new_data):
        self.debug("Processing new data ...")
        result_map = dict()
        for wf_name, wf_config in self.workflow_map.items():
            self.debug("workflow: %s", wf_name)
            targets = wf_config["targets"]
            # SKIP-able check
            output_policy = wf_config["output-policy"]
            if output_policy == "SKIP" and "last-result" in self.history[wf_name]:
                result_map[wf_name] = self.history[wf_name]["last-result"]
                continue

            # Retrieve arguments for the process based on the input policy
            input_policy = wf_config["input-policy"]
            self.debug("Applying input policy: %s", input_policy)

            if input_policy == "SKIP" and "last-input" in self.history[wf_name]:
                process_inputs = self.history[wf_name]["last-input"]
                self.debug("Using last input: %s", str(process_inputs))
            else:
                last_inputs = self.history[wf_name].get(
                    "last-input", [None] * len(targets)
                )
                new_inputs = [
                    nested_data_get(new_data, self.target_map[t]["index"])
                    for t in targets
                ]
                process_inputs = [
                    self.apply_policy(
                        tg,
                        otg,
                        policy=input_policy,
                        data_count=self.history[wf_name]["data-count"],
                    )
                    for tg, otg in zip(new_inputs, last_inputs)
                ]

                process_inputs.extend(wf_config.get("process-args", []))

            if input_policy != "REPLACE":
                self.history[wf_name]["last-input"] = process_inputs

            # Run the process on the retrieved arguments
            from ..config.tools import import_object

            func = import_object(wf_config["process"])

            if "process-kargs" in wf_config:
                process_kwargs = wf_config["process-kargs"]
                process_result = func(*process_inputs, **process_kwargs)
            else:
                process_result = func(*process_inputs)

            # Update process result based on the output(result) policy
            self.debug("Applying result policy: %s", output_policy)
            last_results = self.history[wf_name].get("last-result", None)
            result = self.apply_policy(
                new_data=process_result,
                old_data=last_results,
                policy=output_policy,
                data_count=self.history[wf_name]["data-count"],
            )
            if output_policy != "REPLACE":
                self.history[wf_name]["last-result"] = result
                self.history[wf_name]["data-count"] += 1

            # Update workflow-result map
            result_map[wf_name] = result
        return result_map

    async def _run(self):
        self.info("Starting data reduction ... ")
        new_data = await self.get()
        while new_data and await self.should_proceed():
            self.debug("Processing new data: %s", str(new_data))
            result = await self.process(new_data)
            if not await self.put(data=result):
                break
            new_data = await self.get()
            self.info("Sending %s", str(result))
        self.info("Finishing the task ...")
