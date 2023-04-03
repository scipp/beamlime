# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from typing import TypeVar

import numpy as np
import scipp as sc
from colorama import Style

from ..config.tools import list_to_dict, nested_data_get
from ..core.application import BeamLimeDataReductionInterface

T = TypeVar("T")


def heatmap_2d(data, threshold=0.2, binning_size=(64, 64)):
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


method_map = {"heatmap_2d": heatmap_2d, "handover": lambda x: x}


class BeamLimeDataReductionApplication(BeamLimeDataReductionInterface):
    def __init__(self, config: dict, verbose: bool = False) -> None:
        from colorama import Fore

        self.history = dict()
        super().__init__(config, verbose, verbose_option=Fore.GREEN)

    def pause(self) -> None:
        pass

    def start(self) -> None:
        pass

    def resume(self) -> None:
        pass

    def __del__(self) -> None:
        pass

    def parse_config(self, config: dict):
        self.workflow_map = list_to_dict(config["workflows"], "name")
        self.target_map = list_to_dict(config["targets"], "name")

        self.history = {wf_name: {"data-count": 0} for wf_name in self.workflow_map}

    def apply_policy(
        self, new_data: T, old_data: T, policy: str, data_count: int = 0
    ) -> T:
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

    def process(self, new_data):
        result_map = dict()
        for wf_name, wf_config in self.workflow_map.items():
            targets = wf_config["targets"]
            # SKIP-able check
            output_policy = wf_config["output-policy"]
            if output_policy == "SKIP" and "last-result" in self.history[wf_name]:
                result_map[wf_name] = self.history[wf_name]["last-result"]
                continue

            # Retrieve arguments for the process based on the input policy
            input_policy = wf_config["input-policy"]

            if input_policy == "SKIP" and "last-input" in self.history[wf_name]:
                process_inputs = self.history[wf_name]["last-input"]
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
            process_id = wf_config["process"]
            func = method_map[process_id]

            if "process-kargs" in wf_config:
                process_kwargs = wf_config["process-kargs"]
                process_result = func(*process_inputs, **process_kwargs)
            else:
                process_result = func(*process_inputs)

            # Update process result based on the output(result) policy
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

    @staticmethod
    async def _run(self):
        await asyncio.sleep(1)
        new_data = await self.receive_data()
        while new_data is not None:
            await asyncio.sleep(0.2)
            result = self.process(new_data)
            send_result = await self.send_data(data=result)
            if not send_result:
                break
            new_data = await self.receive_data(timeout=1)
            if self.verbose:
                print(self.verbose_option, f"Sending {result}", Style.RESET_ALL)
