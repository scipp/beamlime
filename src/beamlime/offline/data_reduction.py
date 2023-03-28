import asyncio
from typing import Iterable, List, TypeVar, Union

import numpy as np
import scipp as sc
from colorama import Style

from ..core.application import BeamlimeApplicationInterface

T = TypeVar("T")


def heatmap_2d(data, threshold=0.4, binning_size=(64, 64)):
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
    da.values *= threshold_mask
    return da


method_map = {"heatmap_2d": heatmap_2d, "handover": lambda x: x}


def wrap_target_index(indices: Union[str, int, List]) -> List:
    """
    In the configuration file, the `index` of the target can be
    a string, an integer or a list[str, int].
    If the `index` is a single string or integer,
    it needs to be wrapped to be used in the `nested_data_get`.

    The check could be done in the `nested_data_get`
    but then it will check every items in the list since it is run recursively.
    """
    if isinstance(indices, List):
        for item in indices:
            if not isinstance(item, (str, int)):
                raise TypeError(
                    "Each index in `indices` should be either `str` or `int`."
                )
        return indices
    return [indices]


def nested_data_get(nested_obj: Iterable, *indices):
    """

    >>> nested_obj = {'a': {'b': [1,2,3]}}
    >>> nested_data_get(nested_obj, 'a', 'b', 0)
    1
    """
    idx = indices[0]
    try:
        child = nested_obj[idx]
    except TypeError:
        raise TypeError(
            f"Index {idx} with type {type(idx)}"
            f"doesn't match the key/index type of {type(nested_obj)}"
        )
    except KeyError:
        raise KeyError(f"{nested_obj} doesn't have the key {idx}")
    except IndexError:
        raise IndexError(f"{idx} is out of the range of {len(nested_obj)-1}")
    if len(indices) == 1:
        return child
    else:
        return nested_data_get(child, *indices[1:])


class BeamLimeDataReductionApplication(BeamlimeApplicationInterface):
    history = None

    def __init__(self, config: dict, verbose: bool = False) -> None:
        from colorama import Fore

        self.history = dict()
        super().__init__(config, verbose, verbose_option=Fore.GREEN)

    def parse_config(self, config: dict):
        self.workflow_target_map = dict()
        for mapping in config["workflow-target-mapping"]:
            self.workflow_target_map[mapping["workflow"]] = mapping["targets"]

        self.workflow_map = dict()
        for workflow in config["workflows"]:
            self.workflow_map[workflow["name"]] = workflow
            self.history[workflow["name"]] = dict()

        self.target_map = dict()
        for target in config["targets"]:
            self.target_map[target["name"]] = target

    def apply_policy(self, new_data: T, old_data: T, policy: str) -> T:
        if old_data is None:
            return new_data
        elif policy == "STACK":
            return new_data + old_data
        elif policy == "APPEND":
            return np.append(new_data, old_data)
        # REPLACE or SKIP
        return new_data

    def process(self, new_data):
        result_map = dict()
        for workflow, targets in self.workflow_target_map.items():
            # SKIP-able check
            output_policy = self.workflow_map[workflow]["output-policy"]
            if output_policy == "SKIP" and "last-result" in self.history[workflow]:
                result_map[workflow] = self.history[workflow]["last-result"]
                continue

            # Retrieve arguments for the process based on the input policy
            input_policy = self.workflow_map[workflow]["input-policy"]

            if input_policy == "SKIP" and "last-input" in self.workflow_map[workflow]:
                process_inputs = self.history[workflow]["last-input"]
            else:
                last_inputs = self.history[workflow].get(
                    "last-input", [None] * len(targets)
                )
                tg_indices = [
                    wrap_target_index(self.target_map[t]["index"]) for t in targets
                ]
                new_inputs = [
                    nested_data_get(new_data, *tg_idx) for tg_idx in tg_indices
                ]
                process_inputs = [
                    self.apply_policy(tg, otg, policy=input_policy)
                    for tg, otg in zip(new_inputs, last_inputs)
                ]

                if "process-args" in self.workflow_map[workflow]:
                    process_inputs.extend(self.workflow_map[workflow]["process-args"])

            if input_policy != "REPLACE":
                self.history[workflow]["last-input"] = process_inputs

            # Run the process on the retrieved arguments
            process_id = self.workflow_map[workflow]["process"]
            func = method_map[process_id]

            if "process-kargs" in self.workflow_map[workflow]:
                process_kwargs = self.workflow_map[workflow]["process-kargs"]
                process_result = func(*process_inputs, **process_kwargs)
            else:
                process_result = func(*process_inputs)

            # Update process result based on the output(result) policy
            last_results = self.history[workflow].get("last-result", None)
            result = self.apply_policy(
                new_data=process_result, old_data=last_results, policy=output_policy
            )

            if output_policy != "REPLACE":
                self.history[workflow]["last-result"] = result

            # Update workflow-result map
            result_map[workflow] = result
        return result_map

    @staticmethod
    async def _run(self):
        await asyncio.sleep(1)
        new_data = await self.receive_data()
        while new_data is not None:
            await asyncio.sleep(0.2)
            result = self.process(new_data)
            await self.send_data(data=result)
            new_data = await self.receive_data(timeout=1)
            if self.verbose:
                print(self.verbose_option, f"Sending {result}", Style.RESET_ALL)
